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
    last_refill_ns: int
    tokens: float


class TokenBucketLimiter:
    def __init__(self, rate_of_token: float, token_capacity: int) -> None:
        self.rate = rate_of_token
        self.capacity = float(token_capacity)
        self.buckets: defaultdict[str, Bucket] = defaultdict(
            lambda: Bucket(last_refill_ns=0, tokens=self.capacity)
        )

    def consume(self, identifier: str, token: int = 1) -> bool:
        now_ns = time.monotonic_ns()
        bucket = self.buckets[identifier]

        if bucket.last_refill_ns == 0:
            bucket.last_refill_ns = now_ns
        else:
            elapsed_seconds = (now_ns - bucket.last_refill_ns) / 1e9
            bucket.tokens = min(self.capacity, bucket.tokens + elapsed_seconds * self.rate)
            bucket.last_refill_ns = now_ns

        if bucket.tokens >= token:
            bucket.tokens -= token
            return True
        return False


@dataclass(slots=True)
class UsageRecord:
    last_seen_ns: int
    call_count: int


class RateLimiter:
    def __init__(
        self,
        max_calls: int,
        interval_seconds: int,
        request_per_seconds: float = 1.0,
        burst: int = 10,
        clean_up_interval_seconds: int = 60,
    ):
        self.max_calls = max_calls
        self.interval_ns = interval_seconds * 1_000_000_000
        self.usage: defaultdict[str, UsageRecord] = defaultdict(
            lambda: UsageRecord(last_seen_ns=0, call_count=0)
        )
        self.token_bucket_limiter = TokenBucketLimiter(
            rate_of_token=request_per_seconds, token_capacity=burst
        )
        self._last_cleanup_ns = time.monotonic_ns()
        self._clean_up_interval_ns = clean_up_interval_seconds * 1_000_000_000

    def _cleanup_usage_memory(self) -> None:
        now_ns = time.monotonic_ns()
        if now_ns - self._last_cleanup_ns > self._clean_up_interval_ns:
            cutoff = now_ns - self.interval_ns
            for key in list(self.usage.keys()):
                if self.usage[key].last_seen_ns < cutoff:
                    del self.usage[key]
            self._last_cleanup_ns = now_ns

    def is_allowed(self, identifier: str) -> bool:
        self._cleanup_usage_memory()

        now_ns = time.monotonic_ns()
        record = self.usage[identifier]

        if record.last_seen_ns == 0 or now_ns - record.last_seen_ns > self.interval_ns:
            record.last_seen_ns = now_ns
            record.call_count = 1
        else:
            if record.call_count >= self.max_calls:
                return False
            record.call_count += 1
            record.last_seen_ns = now_ns

        return self.token_bucket_limiter.consume(identifier)


def limit(
    max_calls: int = 10,
    interval_seconds: int = 1,
    request_per_seconds: float = 1.0,
    burst: int = 10,
    clean_up_interval_seconds: int = 60,
) -> Callable[..., Any]:
    if burst <= 0:
        raise ValueError("burst must be greater than 0")
    if max_calls <= 0:
        raise ValueError("max_calls must be greater than 0")
    if clean_up_interval_seconds <= 0:
        raise ValueError("clean_up_interval_seconds must be greater than 0")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be greater than 0")

    rate_limiter = RateLimiter(
        max_calls=max_calls,
        interval_seconds=interval_seconds,
        request_per_seconds=request_per_seconds,
        burst=burst,
        clean_up_interval_seconds=clean_up_interval_seconds,
    )

    def _limit_access(request: Request):
        if not request.client or not request.client.host:
            raise HTTPException(status_code=400, detail="Cannot determine client IP")
        client_ip = request.client.host
        if not rate_limiter.is_allowed(client_ip):
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(1 / request_per_seconds)},
            )

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        is_async = asyncio.iscoroutinefunction(func)
        sig = inspect.signature(func)
        has_request_param = any(param.annotation == Request for param in sig.parameters.values())

        if has_request_param:

            def _get_requests_from_args(args: tuple[Any], kwargs: dict[str, Any]) -> Request:
                request: Request | None = None
                for index, (name, param) in enumerate(sig.parameters.items()):
                    if param.annotation == Request:
                        if name in kwargs:
                            return kwargs[name]
                        else:
                            if index < len(args):
                                return args[index]
                if request is None:
                    raise HTTPException(status_code=500, detail="Request parameter not found")

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                request = _get_requests_from_args(args, kwargs)
                _limit_access(request)
                return await func(*args, **kwargs)

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                request = _get_requests_from_args(args, kwargs)
                _limit_access(request)
                return func(*args, **kwargs)

            return async_wrapper if is_async else sync_wrapper

        else:

            async def request_dependency(request: Request) -> Request:
                return request

            @wraps(func)
            async def async_request_wrapper(
                request: Request = Depends(request_dependency), *args: Any, **kwargs: Any
            ) -> Any:
                _limit_access(request)
                return await func(*args, **kwargs)

            @wraps(func)
            def sync_request_wrapper(
                request: Request = Depends(request_dependency), *args: Any, **kwargs: Any
            ) -> Any:
                _limit_access(request)
                return func(*args, **kwargs)

            old_sig = inspect.signature(func)
            new_params = [
                inspect.Parameter(
                    "request",
                    inspect.Parameter.KEYWORD_ONLY,
                    default=DependsParam(request_dependency),
                )
            ]
            for param in old_sig.parameters.values():
                new_params.append(param)

            wrapper = async_request_wrapper if is_async else sync_request_wrapper
            setattr(wrapper, "__signature__", old_sig.replace(parameters=new_params))
            return wrapper

    return decorator
