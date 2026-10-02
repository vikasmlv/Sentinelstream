from __future__ import annotations

import logging
import random
import time
from typing import Callable, TypeVar

log = logging.getLogger(__name__)
T = TypeVar("T")


class PermanentError(Exception):
    """Do not retry (e.g. HTTP 4xx). Caller should route to the DLQ."""


def retry(
    fn: Callable[[], T],
    *,
    attempts: int,
    base_delay: float,
    max_delay: float = 2.0,
    what: str = "operation",
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Exponential backoff with jitter. `attempts` is the TOTAL number of tries."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except PermanentError:
            raise
        except Exception as exc:  # noqa: BLE001 - deliberate broad catch for retry
            if i == attempts:
                raise
            delay = min(max_delay, base_delay * 2 ** (i - 1)) * random.uniform(0.5, 1.0)
            log.warning("retrying %s", what, extra={"ctx": {"attempt": i, "delay_s": round(delay, 3), "error": repr(exc)}})
            sleep(delay)
    raise RuntimeError("unreachable")
