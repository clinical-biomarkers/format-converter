from abc import ABC, abstractmethod
from typing import Optional, TypedDict, Callable, TypeAlias
from requests import Response

from utils.data_types import CacheableDataModelObject


class APIHandler(ABC):
    """Parent structure for direct API call resources."""

    @abstractmethod
    def __call__(
        self, response: Response, id: str
    ) -> Optional[CacheableDataModelObject]:
        pass


class LibraryHandler(ABC):
    """Parent structure for resources that manage their API calls
    through libraries. Note, the handler will have to manage their
    own error handling, api call counts, and rate limiting.
    """

    @abstractmethod
    def __call__(
        self,
        id: str,
        max_retries: int = 3,
        timeout: int = 5,
        sleep_time: int = 1,
    ) -> tuple[int, Optional[CacheableDataModelObject]]:
        pass


APIHandlerAlias: TypeAlias = Callable[
    [Response, str], Optional[CacheableDataModelObject]
]
LibraryHandlerAlias: TypeAlias = Callable[
    [str, int, int, int], tuple[int, Optional[CacheableDataModelObject]]
]


class EntityHandlerMap(TypedDict):
    api: dict[str, APIHandlerAlias]
    library: dict[str, LibraryHandlerAlias]


# EntityAPIHandlerAlias: TypeAlias = Callable[
#     [Response, str], Optional[AssessedBiomarkerEntity]
# ]
# EntityLibraryHandlerAlias: TypeAlias = Callable[
#     [str, int, int, int], tuple[int, Optional[AssessedBiomarkerEntity]]
# ]
#
#
# class EntityHandler(TypedDict):
#     api: dict[str, EntityAPIHandlerAlias]
#     library: dict[str, EntityLibraryHandlerAlias]
#
#
# # Citation handlers
#
# CitationAPIHandlerAlias: TypeAlias = Callable[[Response, str], Optional[Citation]]
# CitationLibraryHandlerAlias: TypeAlias = Callable[
#     [str, int, int, int], tuple[int, Optional[Citation]]
# ]
#
#
# class CitationHandler(TypedDict):
#     api: dict[str, CitationAPIHandlerAlias]
#     library: dict[str, CitationLibraryHandlerAlias]
#
#
# # Condition handlers
#
# ConditionAPIHandlerAlias: TypeAlias = Callable[[Response, str], Optional[Condition]]
# ConditionLibraryHandlerAlias: TypeAlias = Callable[
#     [str, int, int, int], tuple[int, Optional[Condition]]
# ]
#
#
# class ConditionHandler(TypedDict):
#     api: dict[str, ConditionAPIHandlerAlias]
#     library: dict[str, ConditionLibraryHandlerAlias]
