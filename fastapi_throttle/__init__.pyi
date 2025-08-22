from typing_extensions import Any, Callable

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
