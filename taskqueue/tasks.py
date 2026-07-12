"""Task registry. Workers look up handlers here by the job's task_name."""

import time
from collections.abc import Callable
from typing import Any

REGISTRY: dict[str, Callable[..., Any]] = {}


def task(name: str):
    def wrap(fn):
        REGISTRY[name] = fn
        return fn

    return wrap


# --- built-in tasks (demos + failure injection for tests) -------------------


@task("echo")
def echo(_attempt=None, **payload):
    return payload


@task("sleep")
def sleep(seconds: float = 1, **_):
    time.sleep(seconds)
    return {"slept": seconds}


@task("fail")
def fail(message: str = "boom", **_):
    raise RuntimeError(message)


@task("flaky")
def flaky(succeed_on_attempt: int = 3, _attempt: int = 1, **_):
    """Fails until the worker's attempt counter reaches succeed_on_attempt."""
    if _attempt < succeed_on_attempt:
        raise RuntimeError(f"flaky failure on attempt {_attempt}")
    return {"succeeded_on_attempt": _attempt}
