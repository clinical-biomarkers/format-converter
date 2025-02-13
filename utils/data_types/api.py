from abc import ABC, abstractmethod
from typing import Optional, TypedDict, Callable, TypeAlias, TYPE_CHECKING
from requests import Response

from utils.data_types.json_types import CacheableDataModelObject

if TYPE_CHECKING:
    from utils.data_types import RateLimiter
from utils.logging import LoggedClass


class APIHandler(ABC, LoggedClass):
    """Parent structure for direct API call resources."""

    @abstractmethod
    def __call__(
        self, response: Response, id: str, **kwargs
    ) -> Optional[CacheableDataModelObject]:
        pass


class LibraryHandler(ABC, LoggedClass):
    """Parent structure for resources that manage their API calls
    through libraries. Note, the handler will have to manage their
    own error handling and api call counts.
    """

    @abstractmethod
    def __call__(
        self,
        id: str,
        resource: str,
        max_retries: int = 3,
        timeout: int = 5,
        sleep_time: int = 1,
        rate_limiter: Optional["RateLimiter"] = None,
        **kwargs
    ) -> tuple[int, Optional[CacheableDataModelObject]]:
        pass

    def _check_limit(
        self, resource: str, rate_limiter: Optional["RateLimiter"]
    ) -> None:
        if rate_limiter is None:
            return
        rate_limiter.check_limit(resource=resource)

    def _record_call(
        self, resource: str, rate_limiter: Optional["RateLimiter"]
    ) -> None:
        if rate_limiter is None:
            return
        rate_limiter.record_call(resource=resource)


APIHandlerAlias: TypeAlias = Callable[
    [Response, str], Optional[CacheableDataModelObject]
]
LibraryHandlerAlias: TypeAlias = Callable[
    [str, str, int, int, int, Optional["RateLimiter"]],
    tuple[int, Optional[CacheableDataModelObject]],
]


class EntityHandlerMap(TypedDict):
    api: dict[str, APIHandlerAlias]
    library: dict[str, LibraryHandlerAlias]
