from dataclasses import dataclass
from collections import defaultdict
from time import time, sleep
from utils.logging import LoggedClass
from typing import Optional

@dataclass
class RateLimit:
    calls: int
    window: int = 1


class RateLimiter(LoggedClass):
    """Handles rate limit tracking and enforcement."""

    def __init__(self) -> None:
        super().__init__()
        self._call_times: dict[str, list[float]] = defaultdict(list)
        self._limits: dict[str, RateLimit] = {}

    def add_limit(self, resource: str, calls: Optional[int], window: int = 1) -> None:
        """Add a rate limit for a resource.

        Parameters
        ----------
        resource: str
            Resource identifier
        calls: int
            Maximum number of calls allowed (the rate limit)
        window: int, optional
            Rate limit window in seconds, defaults to 1
        """
        # If there is no rate limit for the resource, return
        if calls is None:
            return

        # If this resource is already being tracked, don't re-add
        if resource in self._limits:
            return

        self._limits[resource] = RateLimit(calls=calls, window=window)
        self.debug(
            f"Added rate limit for {resource}: {calls} calls per {window} seconds"
        )

    def check_limit(self, resource: str) -> None:
        if resource not in self._limits:
            return

        limit = self._limits[resource]
        now = time()

        # Remove old timestamps outside the window
        self._call_times[resource] = [
            t for t in self._call_times[resource] if now - t <= limit.window
        ]

        # If under limit, good to go
        if len(self._call_times[resource]) < limit.calls:
            return

        # Calculate sleep time needed using time of the oldest call, adding
        # the rate limit window, subtractiving the current time, and adding
        # a small buffer time to avoid edge cases
        sleep_time = self._call_times[resource][0] + limit.window - now + 0.1

        self.debug(
            f"Rate limit reached for {resource}, "
            f"sleeping for {sleep_time:.2f} seconds"
        )
        sleep(sleep_time)

        # After sleeping, check again to ensure we are good
        self.check_limit(resource)

    def record_call(self, resource: str) -> None:
        if resource not in self._limits:
            return

        self._call_times[resource].append(time())
