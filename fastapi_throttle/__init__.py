import asyncio
import inspect
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable

from fastapi import Depends, HTTPException, Request
from fastapi.params import (
    Depends as DependsParam,
)  # Alias to avoid conflict with fastapi.Depends


@dataclass(slots=True)
class Bucket:
    """
    Represents a single token bucket for rate limiting.

    Attributes
    ----------
    last_refill_ns : int
        The monotonic timestamp (in nanoseconds) when the bucket was last refilled.
        Used to calculate how many tokens to add since the last check.
    tokens : float
        The current number of tokens available in the bucket.
    """

    last_refill_ns: int
    tokens: float


class TokenBucketLimiter:
    """
    Implements a classic token bucket algorithm for rate limiting.

    This algorithm allows for bursts of requests up to `token_capacity` and then
    smooths out the rate to `rate_of_token` tokens per second.

    Parameters
    ----------
    rate_of_token : float
        The rate at which tokens are added to the bucket (tokens per second).
        This defines the long-term average rate.
    token_capacity : int
        The maximum number of tokens the bucket can hold. This defines the
        maximum burst capacity.

    Examples
    --------
    >>> limiter = TokenBucketLimiter(rate_of_token=10, token_capacity=20)
    >>> limiter.consume("user1") # Initial consume, should be true
    True
    >>> limiter.consume("user1", token=15) # Consume a larger amount
    True
    >>> limiter.consume("user1", token=10) # Likely false if capacity is low
    False
    """

    def __init__(self, rate_of_token: float, token_capacity: int) -> None:
        """
        Initializes the TokenBucketLimiter.

        Parameters
        ----------
        rate_of_token : float
            The rate at which tokens are added to the bucket (tokens per second).
        token_capacity : int
            The maximum number of tokens the bucket can hold (burst capacity).
        """
        self.rate = rate_of_token  # Tokens added per second
        self.capacity = float(token_capacity)  # Maximum tokens the bucket can hold
        # A dictionary to store token buckets for different identifiers (e.g., client IPs)
        self.buckets: defaultdict[str, Bucket] = defaultdict(
            lambda: Bucket(last_refill_ns=0, tokens=self.capacity)
        )

    def consume(self, identifier: str, token: int = 1) -> bool:
        """
        Attempts to consume a specified number of tokens from the bucket for an identifier.

        The bucket is refilled based on the elapsed time since the last refill
        before attempting to consume tokens.

        Parameters
        ----------
        identifier : str
            A unique string identifying the client or resource (e.g., client IP).
        token : int, optional
            The number of tokens to consume. Defaults to 1.

        Returns
        -------
        bool
            True if enough tokens were available and consumed, False otherwise.

        Examples
        --------
        >>> limiter = TokenBucketLimiter(rate_of_token=1, token_capacity=2)
        >>> limiter.consume("user_A")
        True
        >>> limiter.consume("user_A")
        True
        >>> limiter.consume("user_A") # Should be False if no time passed
        False
        """
        now_ns = time.monotonic_ns()  # Get current time in nanoseconds
        bucket = self.buckets[identifier]  # Get or create bucket for the identifier

        # If it's the first time this bucket is accessed, initialize its last_refill_ns
        if bucket.last_refill_ns == 0:
            bucket.last_refill_ns = now_ns
        else:
            # Calculate elapsed time in seconds
            elapsed_seconds = (now_ns - bucket.last_refill_ns) / 1e9
            # Add tokens based on elapsed time and refill rate, but don't exceed capacity
            bucket.tokens = min(
                self.capacity, bucket.tokens + elapsed_seconds * self.rate
            )
            bucket.last_refill_ns = now_ns  # Update last refill time

        # Check if enough tokens are available to consume
        if bucket.tokens >= token:
            bucket.tokens -= token  # Consume tokens
            return True
        return False  # Not enough tokens


@dataclass(slots=True)
class UsageRecord:
    """
    Represents a usage record for a specific identifier within a fixed window.

    Attributes
    ----------
    last_seen_ns : int
        The monotonic timestamp (in nanoseconds) when the identifier was last seen.
        Used to determine if the current request falls within the active window.
    call_count : int
        The number of calls made by this identifier within the current window.
    """

    last_seen_ns: int
    call_count: int


class RateLimiter:
    """
    Combines a fixed-window counter with a token bucket algorithm for robust rate limiting.

    This limiter tracks the number of calls within a specific time interval
    (fixed-window) and also uses a token bucket to smooth out requests, allowing
    for bursts.

    Parameters
    ----------
    max_calls : int
        The maximum number of calls allowed within `interval_seconds`.
    interval_seconds : int
        The time window (in seconds) for the `max_calls` limit.
    request_per_seconds : float, optional
        The token bucket refill rate (tokens per second). Defaults to 1.0.
    burst : int, optional
        The token bucket capacity (maximum burst). Defaults to 10.
    clean_up_interval_seconds : int, optional
        How often (in seconds) to clean up old usage records from memory.
        Defaults to 60.

    Examples
    --------
    >>> limiter = RateLimiter(max_calls=5, interval_seconds=10, request_per_seconds=1, burst=3)
    >>> limiter.is_allowed("client_X")
    True
    >>> # After 5 calls within 10 seconds, this will return False (fixed window limit)
    >>> # If token bucket is exhausted, this will also return False (burst limit)
    """

    def __init__(
        self,
        max_calls: int,
        interval_seconds: int,
        request_per_seconds: float = 1.0,
        burst: int = 10,
        clean_up_interval_seconds: int = 60,
    ):
        """
        Initializes the RateLimiter with both fixed window and token bucket parameters.

        Parameters
        ----------
        max_calls : int
            The maximum number of calls allowed within `interval_seconds`.
        interval_seconds : int
            The time window (in seconds) for the `max_calls` limit.
        request_per_seconds : float, optional
            The token bucket refill rate (tokens per second). Defaults to 1.0.
        burst : int, optional
            The token bucket capacity (maximum burst). Defaults to 10.
        clean_up_interval_seconds : int, optional
            How often (in seconds) to clean up old usage records from memory.
            Defaults to 60.
        """
        self.max_calls = max_calls  # Max calls in the fixed window
        # Convert interval to nanoseconds for high-resolution timing
        self.interval_ns = interval_seconds * 1_000_000_000
        # Stores usage records for each identifier (fixed window tracking)
        self.usage: defaultdict[str, UsageRecord] = defaultdict(
            lambda: UsageRecord(last_seen_ns=0, call_count=0)
        )
        # Instance of the token bucket limiter for burst control
        self.token_bucket_limiter = TokenBucketLimiter(
            rate_of_token=request_per_seconds, token_capacity=burst
        )
        # Timestamp for the last memory cleanup
        self._last_cleanup_ns = time.monotonic_ns()
        # Interval for memory cleanup, converted to nanoseconds
        self._clean_up_interval_ns = clean_up_interval_seconds * 1_000_000_000

    def _cleanup_usage_memory(self) -> None:
        """
        Cleans up old usage records from memory.

        This method is called periodically to remove `UsageRecord` entries
        for identifiers that haven't made requests for longer than the
        `interval_seconds` window. This prevents the `usage` dictionary
        from growing indefinitely.
        """
        now_ns = time.monotonic_ns()
        # Check if it's time for a cleanup
        if now_ns - self._last_cleanup_ns > self._clean_up_interval_ns:
            # Calculate the cutoff timestamp: any record older than this is removed
            cutoff = now_ns - self.interval_ns
            # Iterate over a copy of keys to safely delete items during iteration
            for key in list(self.usage.keys()):
                if self.usage[key].last_seen_ns < cutoff:
                    del self.usage[key]
            self._last_cleanup_ns = now_ns  # Reset last cleanup time

    def is_allowed(self, identifier: str) -> bool:
        """
        Checks if a request from the given identifier is allowed based on both
        fixed-window and token bucket limits.

        This method first applies the fixed-window rate limit, then the
        token bucket limit. A request is allowed only if it passes both checks.

        Parameters
        ----------
        identifier : str
            A unique string identifying the client or resource (e.g., client IP).

        Returns
        -------
        bool
            True if the request is allowed, False otherwise.

        Examples
        --------
        >>> limiter = RateLimiter(max_calls=2, interval_seconds=1)
        >>> limiter.is_allowed("user_Y") # First call
        True
        >>> limiter.is_allowed("user_Y") # Second call
        True
        >>> limiter.is_allowed("user_Y") # Third call, fixed window exceeded
        False
        >>> # After 1 second, it would be True again.
        """
        # Periodically clean up old usage data to manage memory
        self._cleanup_usage_memory()

        now_ns = time.monotonic_ns()
        record = self.usage[
            identifier
        ]  # Get or create the usage record for this identifier

        # Fixed Window Logic:
        # If this is the first call for this identifier, or if the current call
        # is outside the previous interval window, reset the counter.
        if record.last_seen_ns == 0 or now_ns - record.last_seen_ns > self.interval_ns:
            record.last_seen_ns = now_ns
            record.call_count = 1
        else:
            # If within the current window, check if max calls are exceeded
            if record.call_count >= self.max_calls:
                return False  # Fixed window limit exceeded
            record.call_count += 1
            record.last_seen_ns = now_ns  # Update last seen time within the window

        # Token Bucket Logic:
        # After passing the fixed window check, also check the token bucket.
        # This allows for burst control.
        return self.token_bucket_limiter.consume(identifier)


def limit(
    max_calls: int = 10,
    interval_seconds: int = 1,
    request_per_seconds: float = 1.0,
    burst: int = 10,
    clean_up_interval_seconds: int = 60,
) -> Callable[..., Any]:
    """
    A FastAPI dependency decorator for applying rate limiting to API endpoints.

    This decorator creates a `RateLimiter` instance with the specified parameters
    and applies it to the decorated FastAPI path operation function. It combines
    a fixed-window counter with a token bucket algorithm to provide robust
    rate limiting.

    If the rate limit is exceeded, an `HTTPException` with status code 429 (Too Many Requests)
    is raised.

    Parameters
    ----------
    max_calls : int, optional
        The maximum number of calls allowed within `interval_seconds` for a given client.
        Defaults to 10.
    interval_seconds : int, optional
        The time window (in seconds) for the `max_calls` limit. Defaults to 1.
    request_per_seconds : float, optional
        The token bucket refill rate (tokens per second). This defines the
        long-term average rate. Defaults to 1.0.
    burst : int, optional
        The token bucket capacity (maximum number of tokens that can be accumulated).
        This defines the maximum burst capacity. Defaults to 10.
    clean_up_interval_seconds : int, optional
        How often (in seconds) to clean up old usage records from memory.
        Defaults to 60.

    Returns
    -------
    Callable[..., Any]
        A decorator that can be applied to FastAPI path operation functions.

    Raises
    ------
    ValueError
        If any of `burst`, `max_calls`, `clean_up_interval_seconds`, or
        `interval_seconds` is less than or equal to 0.

    Examples
    --------
    Using the decorator in a FastAPI application:

    >>> from fastapi import FastAPI
    >>> app = FastAPI()

    >>> @app.get("/limited")
    >>> @limit(max_calls=5, interval_seconds=60, request_per_seconds=2, burst=5)
    >>> async def get_limited_resource():
    >>>     return {"message": "This resource is rate-limited."}

    >>> @app.post("/another-limited")
    >>> @limit(max_calls=1, interval_seconds=5)
    >>> async def post_another_limited_resource(request: Request):
    >>>     # If 'request' is already an argument in the function signature,
    >>>     # the decorator will use it directly.
    >>>     return {"message": "Another rate-limited resource."}
    """
    # Validate input parameters to ensure they are positive
    for arg_value, arg_name in [
        (burst, "burst"),
        (max_calls, "max_calls"),
        (clean_up_interval_seconds, "clean_up_interval_seconds"),
        (interval_seconds, "interval_seconds"),
    ]:
        if arg_value <= 0:
            raise ValueError(f"Parameter '{arg_name}' must be greater than 0")

    # Initialize the RateLimiter instance with the configured parameters.
    # This instance will be shared across all decorated endpoints that use
    # this specific `limit` decorator instance.
    rate_limiter = RateLimiter(
        max_calls=max_calls,
        interval_seconds=interval_seconds,
        request_per_seconds=request_per_seconds,
        burst=burst,
        clean_up_interval_seconds=clean_up_interval_seconds,
    )

    async def _request_dependency(request: Request) -> Request:
        """
        A simple FastAPI dependency to inject the Request object if it's not
        already present in the decorated function's signature.

        Parameters
        ----------
        request : Request
            The FastAPI Request object.

        Returns
        -------
        Request
            The incoming Request object.
        """
        return request

    def _limit_access(request: Request):
        """
        Performs the actual rate limit check using the initialized RateLimiter.

        This function extracts the client IP from the request and then calls
        `rate_limiter.is_allowed()`. If the request is not allowed, it raises
        an `HTTPException` with a 429 status code.

        Parameters
        ----------
        request : Request
            The FastAPI Request object from which the client IP is extracted.

        Raises
        ------
        HTTPException
            If the client IP cannot be determined (status 400).
        HTTPException
            If the rate limit is exceeded (status 429).
        """
        # Ensure client IP can be determined for rate limiting
        if not request.client or not request.client.host:
            raise HTTPException(status_code=400, detail="Cannot determine client IP")
        client_ip = request.client.host  # Get the client's IP address

        # Check if the request is allowed by the rate limiter
        if not rate_limiter.is_allowed(client_ip):
            # If not allowed, raise an HTTPException
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={
                    "Retry-After": str(1 / request_per_seconds)
                },  # Suggest when to retry
            )

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        The inner decorator function that wraps the FastAPI path operation.

        This function inspects the signature of the decorated function to determine
        if it already accepts a `Request` object. Based on this, it creates
        an appropriate wrapper that calls `_limit_access` before the original function.

        Parameters
        ----------
        func : Callable[..., Any]
            The FastAPI path operation function to be decorated.

        Returns
        -------
        Callable[..., Any]
            A wrapped version of the original function with rate limiting applied.
        """
        # Inspect the decorated function's signature
        sig = inspect.signature(func)
        # Check if the function already declares a 'Request' parameter
        has_request_param = any(
            param.annotation is Request for param in sig.parameters.values()
        )
        # Check if the function is an asynchronous coroutine function
        is_async = asyncio.iscoroutinefunction(func)

        def _get_request_from_args(
            args: tuple[Any, ...], kwargs: dict[str, Any]
        ) -> Request:
            """
            Extracts the Request object from the function arguments.

            This helper function is used when the decorated function explicitly
            accepts a `Request` object as a parameter. It searches for the
            `Request` instance among positional and keyword arguments.

            Parameters
            ----------
            args : tuple[Any, ...]
                Positional arguments passed to the decorated function.
            kwargs : dict[str, Any]
                Keyword arguments passed to the decorated function.

            Returns
            -------
            Request
                The FastAPI Request object.

            Raises
            ------
            HTTPException
                If a Request parameter was expected but not found (status 500).
            """
            for i, (name, param) in enumerate(sig.parameters.items()):
                if param.annotation is Request:
                    if name in kwargs:
                        return kwargs[name]
                    if i < len(args):
                        return args[i]
            # This case should ideally not be reached if has_request_param is True,
            # as FastAPI should inject it. It's a fallback for unexpected scenarios.
            raise HTTPException(status_code=500, detail="Request parameter not found")

        if has_request_param:
            # Case 1: The original function already explicitly takes a `Request` parameter.
            # We don't need to inject it as a dependency.

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                """
                Asynchronous wrapper for functions that already accept a Request.
                """
                request = _get_request_from_args(
                    args, kwargs
                )  # Get Request from existing args
                _limit_access(request)  # Apply rate limit
                return await func(*args, **kwargs)  # Call original async function

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                """
                Synchronous wrapper for functions that already accept a Request.
                """
                request = _get_request_from_args(
                    args, kwargs
                )  # Get Request from existing args
                _limit_access(request)  # Apply rate limit
                return func(*args, **kwargs)  # Call original sync function

            return async_wrapper if is_async else sync_wrapper

        else:
            # Case 2: The original function does NOT explicitly take a `Request` parameter.
            # We need to inject it as a FastAPI dependency.

            @wraps(func)
            async def async_reqest_wrapper(
                request: Request = Depends(_request_dependency),
                *args: Any,
                **kwargs: Any,
            ) -> Any:
                """
                Asynchronous wrapper for functions that need Request injected via Depends.
                """
                _limit_access(request)  # Apply rate limit using the injected request
                return await func(*args, **kwargs)  # Call original async function

            @wraps(func)
            def sync_reqest_wrapper(
                request: Request = Depends(_request_dependency),
                *args: Any,
                **kwargs: Any,
            ) -> Any:
                """
                Synchronous wrapper for functions that need Request injected via Depends.
                """
                _limit_access(request)  # Apply rate limit using the injected request
                return func(*args, **kwargs)  # Call original sync function

            # Modify the wrapper's signature to include the `request: Request = Depends(...)`
            # parameter. This is crucial for FastAPI to correctly identify and inject
            # the dependency.
            old_sig = inspect.signature(func)
            new_params = (
                [
                    inspect.Parameter(
                        "request",  # Name of the injected parameter
                        inspect.Parameter.KEYWORD_ONLY,  # Enforce it as a keyword-only argument
                        default=DependsParam(
                            _request_dependency
                        ),  # The dependency itself
                        annotation=Request,  # Type hint for the injected parameter
                    )
                ]
                + list(old_sig.parameters.values())
            )  # Add all original parameters

            wrapper = async_reqest_wrapper if is_async else sync_reqest_wrapper
            # Set the modified signature on the wrapper function
            setattr(wrapper, "__signature__", old_sig.replace(parameters=new_params))
            return wrapper

    return decorator
