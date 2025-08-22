import asyncio
import inspect
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable

from fastapi import Depends, HTTPException, Request
from fastapi.params import Depends as DependsParam


@dataclass(slots=True)
class Bucket:
    """
    Represents a single token bucket for rate limiting.

    This dataclass holds the state of a token bucket, including when it was
    last refilled and the current number of tokens available.

    Attributes
    ----------
    last_refill_ns : int
        Timestamp (in nanoseconds, from `time.monotonic_ns()`) when the bucket
        was last refilled or accessed.
    tokens : float
        The current number of tokens available in the bucket.
    """

    last_refill_ns: int
    tokens: float


class TokenBucketLimiter:
    """
    Implements a token bucket algorithm for rate limiting.

    This class manages multiple token buckets, one for each unique identifier,
    allowing for burstable rate limiting based on a continuous token refill
    rate and a maximum capacity.

    Parameters
    ----------
    rate_of_token : float
        The rate at which tokens are added to the bucket per second.
        For example, a rate of 1.0 means 1 token per second.
    token_capacity : int
        The maximum number of tokens a bucket can hold (the burst capacity).

    Attributes
    ----------
    rate : float
        The rate at which tokens are added to each bucket per second.
    capacity : float
        The maximum number of tokens a bucket can hold.
    buckets : defaultdict[str, Bucket]
        A dictionary mapping identifiers (e.g., client IP addresses) to their
        respective `Bucket` objects. New buckets are initialized with full capacity.

    Examples
    --------
    >>> limiter = TokenBucketLimiter(rate_of_token=1.0, token_capacity=5)
    >>> limiter.consume("user1")
    True
    >>> limiter.consume("user1", token=5) # Try to consume more than available + burst
    False
    >>> import time
    >>> time.sleep(1) # Wait for tokens to refill
    >>> limiter.consume("user1")
    True
    """

    def __init__(self, rate_of_token: float, token_capacity: int) -> None:
        self.rate = rate_of_token
        self.capacity = float(token_capacity)
        # defaultdict allows new identifiers to automatically get a new bucket initialized to full capacity.
        self.buckets: defaultdict[str, Bucket] = defaultdict(
            lambda: Bucket(last_refill_ns=0, tokens=self.capacity)
        )

    def consume(self, identifier: str, token: int = 1) -> bool:
        """
        Attempts to consume a specified number of tokens for a given identifier.

        This method first refills the bucket based on the elapsed time since
        the last refill, then checks if enough tokens are available to consume.

        Parameters
        ----------
        identifier : str
            A unique string identifying the client or resource (e.g., client IP address).
        token : int, optional
            The number of tokens to consume. Defaults to 1.

        Returns
        -------
        bool
            True if the tokens were successfully consumed (i.e., allowed),
            False otherwise (i.e., rate limit exceeded).
        """
        now_ns = time.monotonic_ns()  # Get current time in nanoseconds for high precision.
        bucket = self.buckets[identifier]  # Retrieve or create bucket for the identifier.

        if bucket.last_refill_ns == 0:
            # First access for this identifier, initialize last_refill_ns.
            bucket.last_refill_ns = now_ns
        else:
            # Calculate elapsed time in seconds since last refill.
            elapsed_seconds = (now_ns - bucket.last_refill_ns) / 1e9
            # Add new tokens based on refill rate and elapsed time, capped by capacity.
            bucket.tokens = min(self.capacity, bucket.tokens + elapsed_seconds * self.rate)
            # Update last refill time.
            bucket.last_refill_ns = now_ns

        # Check if enough tokens are available to consume.
        if bucket.tokens >= token:
            # If so, consume tokens and allow the request.
            bucket.tokens -= token
            return True
        # Not enough tokens, deny the request.
        return False


@dataclass(slots=True)
class UsageRecord:
    """
    Represents a usage record for a specific identifier in a sliding window.

    This dataclass tracks the last seen timestamp and the call count within
    the current window for a given identifier.

    Attributes
    ----------
    last_seen_ns : int
        Timestamp (in nanoseconds, from `time.monotonic_ns()`) when the
        identifier was last seen or a call was recorded.
    call_count : int
        The number of calls made by this identifier within its current
        time window.
    """

    last_seen_ns: int
    call_count: int


class RateLimiter:
    """
    Combines a sliding window counter and a token bucket algorithm for robust rate limiting.

    This class provides a multi-layered rate-limiting approach. It uses a
    sliding window to limit the number of calls within a specific interval
    (e.g., 10 calls per 1 second) and a token bucket for burst control
    (e.g., allowing 5 requests immediately, then 1 per second).
    It also includes a mechanism to periodically clean up old usage records
    to prevent memory growth.

    Parameters
    ----------
    max_calls : int
        The maximum number of calls allowed within the `interval_seconds`.
    interval_seconds : int
        The time window in seconds during which `max_calls` are allowed.
    request_per_seconds : float, optional
        The token refill rate for the underlying `TokenBucketLimiter`.
        Defaults to 1.0 (1 token per second).
    burst : int, optional
        The maximum token capacity (burst capacity) for the
        `TokenBucketLimiter`. Defaults to 10.
    clean_up_interval_seconds : int, optional
        The interval in seconds at which old usage records are cleaned up
        from memory. Defaults to 60 seconds.

    Attributes
    ----------
    max_calls : int
        Maximum calls allowed within the interval.
    interval_ns : int
        Interval duration in nanoseconds.
    usage : defaultdict[str, UsageRecord]
        Dictionary storing `UsageRecord` for each identifier, tracking calls
        within the sliding window.
    token_bucket_limiter : TokenBucketLimiter
        An instance of `TokenBucketLimiter` for burst control.
    _last_cleanup_ns : int
        Timestamp of the last cleanup operation in nanoseconds.
    _clean_up_interval_ns : int
        Interval for cleanup operations in nanoseconds.

    Examples
    --------
    >>> limiter = RateLimiter(max_calls=5, interval_seconds=1, request_per_seconds=1.0, burst=2)
    >>> limiter.is_allowed("user_X") # Allowed (first call)
    True
    >>> limiter.is_allowed("user_X") # Allowed (second call, within burst and max_calls)
    True
    >>> limiter.is_allowed("user_X") # Denied (burst exceeded, even if max_calls not yet)
    False
    >>> import time
    >>> time.sleep(1.1) # Wait for interval to pass and tokens to refill
    >>> limiter.is_allowed("user_X") # Allowed again
    True
    """

    def __init__(
        self,
        max_calls: int,
        interval_seconds: int,
        request_per_seconds: float = 1.0,
        burst: int = 10,
        clean_up_interval_seconds: int = 60,
    ):
        self.max_calls = max_calls
        # Convert interval to nanoseconds for high-resolution timing.
        self.interval_ns = interval_seconds * 1_000_000_000
        # defaultdict to store usage records for each identifier.
        self.usage: defaultdict[str, UsageRecord] = defaultdict(
            lambda: UsageRecord(last_seen_ns=0, call_count=0)
        )
        # Initialize the token bucket limiter for burst control.
        self.token_bucket_limiter = TokenBucketLimiter(
            rate_of_token=request_per_seconds, token_capacity=burst
        )
        # Initialize cleanup tracking.
        self._last_cleanup_ns = time.monotonic_ns()
        self._clean_up_interval_ns = clean_up_interval_seconds * 1_000_000_000

    def _cleanup_usage_memory(self) -> None:
        """
        Periodically cleans up old usage records from memory.

        Removes identifiers from the `usage` dictionary whose `last_seen_ns`
        timestamp falls outside the current sliding window, preventing the
        dictionary from growing indefinitely. This method is called internally
        by `is_allowed`.
        """
        now_ns = time.monotonic_ns()
        # Check if it's time to perform a cleanup.
        if now_ns - self._last_cleanup_ns > self._clean_up_interval_ns:
            # Calculate the cutoff timestamp for records to be considered old.
            cutoff = now_ns - self.interval_ns
            # Iterate over a copy of keys to safely delete items during iteration.
            for key in list(self.usage.keys()):
                # If a record's last seen time is older than the cutoff, delete it.
                if self.usage[key].last_seen_ns < cutoff:
                    del self.usage[key]
            # Update the timestamp of the last cleanup.
            self._last_cleanup_ns = now_ns

    def is_allowed(self, identifier: str) -> bool:
        """
        Checks if a request from the given identifier is allowed based on
        both sliding window and token bucket limits.

        Parameters
        ----------
        identifier : str
            A unique string identifying the client or resource (e.g., client IP address).

        Returns
        -------
        bool
            True if the request is allowed, False otherwise.
        """
        # First, perform a memory cleanup if due.
        self._cleanup_usage_memory()

        now_ns = time.monotonic_ns()
        record = self.usage[identifier]  # Get or create usage record for identifier.

        # Sliding window logic:
        # If this is the first call for this identifier, or the last call was outside the window.
        if record.last_seen_ns == 0 or now_ns - record.last_seen_ns > self.interval_ns:
            # Reset the window: update last seen time and start call count from 1.
            record.last_seen_ns = now_ns
            record.call_count = 1
        else:
            # If within the window, check if max calls are exceeded.
            if record.call_count >= self.max_calls:
                return False  # Sliding window limit exceeded.
            # Increment call count and update last seen time within the current window.
            record.call_count += 1
            record.last_seen_ns = now_ns

        # Token bucket logic:
        # If the sliding window allows, then check the token bucket for burst control.
        return self.token_bucket_limiter.consume(identifier)


def limit(
    max_calls: int = 10,
    interval_seconds: int = 1,
    request_per_seconds: float = 1.0,
    burst: int = 10,
    clean_up_interval_seconds: int = 60,
) -> Callable[..., Any]:
    """
    A FastAPI decorator factory for applying rate limiting to endpoint functions.

    This decorator factory creates a `RateLimiter` instance with the specified
    parameters and applies it to the decorated FastAPI route. It supports
    both async and sync functions and intelligently injects `Request` if not
    already present in the function signature.

    Parameters
    ----------
    max_calls : int, optional
        The maximum number of calls allowed within the `interval_seconds`.
        Defaults to 10. Must be greater than 0.
    interval_seconds : int, optional
        The time window in seconds during which `max_calls` are allowed.
        Defaults to 1. Must be greater than 0.
    request_per_seconds : float, optional
        The rate at which tokens are refilled per second for burst control.
        Defaults to 1.0.
    burst : int, optional
        The maximum number of tokens allowed in the bucket (burst capacity).
        Defaults to 10. Must be greater than 0.
    clean_up_interval_seconds : int, optional
        The interval in seconds for cleaning up old usage records.
        Defaults to 60. Must be greater than 0.

    Returns
    -------
    Callable[..., Any]
        A decorator that can be applied to FastAPI endpoint functions.

    Raises
    ------
    ValueError
        If any of `burst`, `max_calls`, `clean_up_interval_seconds`,
        or `interval_seconds` are less than or equal to 0.

    Examples
    --------
    Using the decorator on an async FastAPI endpoint:

    >>> from fastapi import FastAPI
    >>> app = FastAPI()
    >>> @app.get("/limited")
    >>> @limit(max_calls=2, interval_seconds=5)
    >>> async def get_limited_data():
    ...     return {"message": "You accessed limited data!"}

    Using the decorator on an endpoint that already defines `Request`:

    >>> from fastapi import FastAPI, Request
    >>> app = FastAPI()
    >>> @app.get("/limited_with_request")
    >>> @limit(max_calls=1, interval_seconds=1)
    >>> async def get_limited_data_with_request(request: Request):
    ...     return {"message": f"Hello from {request.client.host}!"}
    """
    # Validate input parameters to ensure they are positive.
    if burst <= 0:
        raise ValueError("burst must be greater than 0")
    if max_calls <= 0:
        raise ValueError("max_calls must be greater than 0")
    if clean_up_interval_seconds <= 0:
        raise ValueError("clean_up_interval_seconds must be greater than 0")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be greater than 0")

    # Initialize a single RateLimiter instance for all decorated functions using this `limit` call.
    rate_limiter = RateLimiter(
        max_calls=max_calls,
        interval_seconds=interval_seconds,
        request_per_seconds=request_per_seconds,
        burst=burst,
        clean_up_interval_seconds=clean_up_interval_seconds,
    )

    def _limit_access(request: Request):
        """
        Internal function to perform the actual rate limit check using the
        configured `RateLimiter` instance.

        Raises `HTTPException` if the client's IP cannot be determined or
        if the rate limit is exceeded.

        Parameters
        ----------
        request : Request
            The FastAPI request object.

        Raises
        ------
        HTTPException
            - 400 Bad Request if the client's host IP cannot be determined.
            - 429 Too Many Requests if the rate limit is exceeded.
        """
        # Ensure client IP is available to use as an identifier.
        if not request.client or not request.client.host:
            raise HTTPException(status_code=400, detail="Cannot determine client IP")
        client_ip = request.client.host
        # Check if the request is allowed by the rate limiter.
        if not rate_limiter.is_allowed(client_ip):
            # If not allowed, raise an HTTPException with 429 status code.
            # Add a "Retry-After" header suggesting when to retry.
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(1 / request_per_seconds)},
            )

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        The actual decorator that wraps the FastAPI endpoint function.

        This inner function analyzes the decorated function's signature to
        determine if it already accepts a `Request` object. Based on this,
        it either extracts the `Request` from the arguments or injects it
        as a FastAPI dependency. It handles both async and sync functions.

        Parameters
        ----------
        func : Callable[..., Any]
            The FastAPI endpoint function to be decorated.

        Returns
        -------
        Callable[..., Any]
            A wrapped version of the original function with rate limiting applied.
        """
        # Determine if the function is asynchronous.
        is_async = asyncio.iscoroutinefunction(func)
        sig = inspect.signature(func)
        # Check if the function signature already includes a Request parameter.
        has_request_param = any(param.annotation == Request for param in sig.parameters.values())

        if has_request_param:
            # Case 1: The decorated function already defines a 'request: Request' parameter.

            def _get_requests_from_args(args: tuple[Any], kwargs: dict[str, Any]) -> Request:
                """
                Extracts the Request object from the function's arguments or keyword arguments.

                This utility is used when the decorated function explicitly declares
                `Request` as a parameter.

                Parameters
                ----------
                args : tuple[Any]
                    Positional arguments passed to the wrapped function.
                kwargs : dict[str, Any]
                    Keyword arguments passed to the wrapped function.

                Returns
                -------
                Request
                    The FastAPI Request object.

                Raises
                ------
                HTTPException
                    500 Internal Server Error if a Request parameter was declared
                    but could not be found in the actual arguments.
                """
                # Iterate through the original function's parameters to find the Request.
                for index, (name, param) in enumerate(sig.parameters.items()):
                    if param.annotation == Request:
                        if name in kwargs:
                            return kwargs[name]  # Found in keyword arguments.
                        elif index < len(args):
                            return args[index]  # Found in positional arguments.
                # This should ideally not happen if `has_request_param` is True and FastAPI provides it.
                raise HTTPException(status_code=500, detail="Request parameter not found")

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                """
                Async wrapper for functions that already define `Request`.
                Extracts the request, applies rate limiting, then calls the original function.
                """
                request = _get_requests_from_args(args, kwargs)
                _limit_access(request)  # Perform the rate limit check.
                return await func(*args, **kwargs)

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                """
                Sync wrapper for functions that already define `Request`.
                Extracts the request, applies rate limiting, then calls the original function.
                """
                request = _get_requests_from_args(args, kwargs)
                _limit_access(request)  # Perform the rate limit check.
                return func(*args, **kwargs)

            # Return the appropriate wrapper based on the original function's async nature.
            return async_wrapper if is_async else sync_wrapper

        else:
            # Case 2: The decorated function does NOT define a 'request: Request' parameter.
            # We need to inject it using FastAPI's Depends system.

            async def request_dependency(request: Request) -> Request:
                """
                A simple FastAPI dependency that just passes through the Request object.
                This is used to force FastAPI to provide the Request object to our wrapper.
                """
                return request

            @wraps(func)
            async def async_request_wrapper(
                request: Request = Depends(request_dependency), *args: Any, **kwargs: Any
            ) -> Any:
                """
                Async wrapper for functions that *do not* define `Request`.
                FastAPI injects the `Request` via `Depends`, then rate limiting is applied.
                """
                _limit_access(request)  # Perform the rate limit check.
                return await func(*args, **kwargs)

            @wraps(func)
            def sync_request_wrapper(
                request: Request = Depends(request_dependency), *args: Any, **kwargs: Any
            ) -> Any:
                """
                Sync wrapper for functions that *do not* define `Request`.
                FastAPI injects the `Request` via `Depends`, then rate limiting is applied.
                """
                _limit_access(request)  # Perform the rate limit check.
                return func(*args, **kwargs)

            # Modify the signature of the wrapper function so FastAPI properly recognizes
            # the injected 'request: Request = Depends(...)' parameter.
            old_sig = inspect.signature(func)
            # Create a new parameter for the injected 'request'.
            # It's added as KEYWORD_ONLY to avoid clashes with existing positional args
            # and to make it clear it's an injected dependency.
            new_params = [
                inspect.Parameter(
                    "request",
                    inspect.Parameter.KEYWORD_ONLY,
                    default=DependsParam(request_dependency),
                )
            ]
            # Append all original parameters of the decorated function.
            for param in old_sig.parameters.values():
                new_params.append(param)

            # Select the appropriate wrapper (async or sync).
            wrapper = async_request_wrapper if is_async else sync_request_wrapper
            # Set the modified signature on the wrapper. This is crucial for FastAPI's
            # dependency injection system to work correctly for the 'request' parameter.
            setattr(wrapper, "__signature__", old_sig.replace(parameters=new_params))
            return wrapper

    return decorator
