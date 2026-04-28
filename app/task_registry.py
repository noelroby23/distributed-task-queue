"""Task registry and built-in task handlers."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from time import sleep
from typing import Any, Callable, Dict

from PIL import Image

TaskHandler = Callable[..., Any]

TASK_REGISTRY: Dict[str, TaskHandler] = {}


def register_task(task_type: str, handler: TaskHandler | None = None):
    """
    Register a task handler by name.

    Supports both direct calls and decorator usage:

        register_task("foo", handler)

    or:

        @register_task("foo")
        def handler(...):
            ...
    """

    def decorator(func: TaskHandler) -> TaskHandler:
        TASK_REGISTRY[task_type] = func
        return func

    if handler is not None:
        return decorator(handler)

    return decorator


def get_task(task_type: str) -> TaskHandler | None:
    return TASK_REGISTRY.get(task_type)


def available_task_types() -> list[str]:
    return sorted(TASK_REGISTRY)


@register_task("dummy")
def dummy_task(*args: Any, **kwargs: Any) -> dict:
    """Simple demo task that sleeps for a configurable number of seconds."""
    sleep_seconds = kwargs.pop("sleep_seconds", 2)
    sleep(sleep_seconds)
    kwargs.pop("_retry_count", None)
    return {
        "message": "dummy task completed",
        "sleep_seconds": sleep_seconds,
        "received_args": list(args),
        "received_kwargs": kwargs,
    }


@register_task("add")
def add_task(*args: Any, **kwargs: Any) -> Any:
    """Add numeric arguments together for an easy worker smoke test."""
    kwargs.pop("_retry_count", None)
    if len(args) >= 2:
        return args[0] + args[1]
    return sum(args)


@register_task("flaky")
def flaky_task(fail_count: int = 2, _retry_count: int = 0, **_: Any) -> dict:
    """
    Fail the first `fail_count` attempts, then succeed.

    This is intentionally useful for testing retry behavior.
    """
    if _retry_count < fail_count:
        raise RuntimeError(
            f"Simulated failure (attempt {_retry_count + 1}, "
            f"will succeed after {fail_count} failures)"
        )

    return {
        "message": f"succeeded after {_retry_count} retries",
        "total_attempts": _retry_count + 1,
    }


@register_task("word_count")
def word_count_task(
    text: str | None = None,
    input_path: str | None = None,
    **_: Any,
) -> dict:
    """Count words from inline text or a local text file path."""
    if text is None and input_path is None:
        raise ValueError("Provide either 'text' or 'input_path'.")
    if text is not None and input_path is not None:
        raise ValueError("Provide only one of 'text' or 'input_path'.")

    if input_path is not None:
        text = Path(input_path).read_text(encoding="utf-8")

    assert text is not None
    words = text.split()
    counts = Counter(word.lower() for word in words)
    top_words = counts.most_common(5)
    return {
        "word_count": len(words),
        "unique_words": len(counts),
        "top_words": top_words,
    }


@register_task("fibonacci")
def fibonacci_task(n: int, **_: Any) -> dict:
    """Return the nth Fibonacci number using an iterative implementation."""
    if n < 0:
        raise ValueError("n must be non-negative")

    a, b = 0, 1
    for _index in range(n):
        a, b = b, a + b
    return {"n": n, "value": a}


@register_task("image_resize")
def image_resize_task(
    input_path: str,
    output_path: str,
    width: int,
    height: int,
    **_: Any,
) -> dict:
    """Resize a local image file and save it to a local destination path."""
    if width <= 0 or height <= 0:
        raise ValueError("width and height must be positive integers")

    source = Path(input_path)
    target = Path(output_path)
    if not source.exists():
        raise FileNotFoundError(f"Input image not found: {input_path}")

    target.parent.mkdir(parents=True, exist_ok=True)

    with Image.open(source) as image:
        resized = image.resize((width, height))
        resized.save(target)

    return {
        "output_path": str(target),
        "width": width,
        "height": height,
    }
