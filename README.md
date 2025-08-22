# FastAPI Throttle

A simple, standalone rate limiting solution for FastAPI applications.

---

## 🚀 Getting Started

This module is designed for ultimate simplicity. There's no `pip install` required!

1.  **Copy the File:** Get the content of [`fastapi_throttle.py`](./fastapi_throttle.py) and paste it into a new file named `fastapi_throttle.py` (or any name you prefer) directly in your FastAPI project.
2.  **Import and Use:** Import the `limit` decorator from your newly created file and apply it to your FastAPI endpoints.

## ✨ Features

- **Decorator-based Rate Limiting:** Easily apply rate limits using a Python decorator.
- **Flexible Configuration:** Control `max_calls`, `interval_seconds`, `request_per_seconds`, `burst`, and `clean_up_interval_seconds`.
  - `max_calls`: The maximum number of calls allowed within `interval_seconds`.
  - `interval_seconds`: The time window (in seconds) for the `max_calls` limit.
  - `request_per_seconds`: The token bucket refill rate (tokens per second), defining the long-term average rate.
  - `burst`: The token bucket capacity (maximum number of tokens that can be accumulated), defining the maximum burst capacity.
  - `clean_up_interval_seconds`: How often (in seconds) to clean up old usage records from memory.
- **Sync & Async Compatible:** Works seamlessly with both `async def` and `def` FastAPI endpoint functions.
- **Request Object Agnostic:** Functions can optionally accept or omit the `Request` object.

## 💡 Usage Example

```python
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
# Assuming you copied the file as fastapi_throttle.py in your project root
from fastapi_throttle import limit

app = FastAPI()

# It is compatible with both sync and async - whether it passes Request or not!

@app.get("/endpoint1")
@limit(max_calls=10, interval_seconds=1, request_per_seconds=1)
async def endpoint_1(request: Request) -> dict[str, str | dict[str, str]]:
    # async function with Request object
    return {"message": "Return from endpoint 1"}


@app.get("/endpoint-2")
@limit(max_calls=1, interval_seconds=1, request_per_seconds=1)
def endpoint_2() -> dict[str, str]:
    # sync function, no Request object
    return {"status": "ok"}

# You can also use it without `request_per_seconds` if only `max_calls` and `interval_seconds` are sufficient
@app.get("/another-async")
@limit(max_calls=5, interval_seconds=5)
async def another_async_endpoint() -> dict[str, str]:
    # async function, no Request object
    return {"data": "This is another async endpoint"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

## ⚠️ Limitations

- **No `custom_rate_limit_callback`:** The current implementation does not support custom callback functions when a rate limit is exceeded. It will return a default `429 Too Many Requests` HTTP response.
- **In-Memory Storage:** Rate limits are stored in memory, which means they are not persistent across application restarts and will not scale across multiple instances without an external caching mechanism (e.g., Redis).

## 🧑‍💻 Author

**Rakibul Hasan Ratul** <rakibulhasanratul@proton.me>  
Independent Developer,  
Dhaka, Bangladesh

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
