from abc import ABC, abstractmethod
from typing import Any, Optional, TypedDict, Callable, TypeAlias
from requests import Response

from utils.data_types import AssessedBiomarkerEntity, Citation


class APIHandler(ABC):

    @abstractmethod
    def __call__(self, response: Response) -> Optional[Any]:
        pass


class LibraryHandler(ABC):

    @abstractmethod
    def __call__(
        self,
        id: str,
        max_retries: int = 3,
        timeout: int = 5,
        sleep_time: int = 1,
    ) -> tuple[int, Any]:
        pass


### Define handler types

# Entity handlers

EntityAPIHandlerAlias: TypeAlias = Callable[
    [Response], Optional[AssessedBiomarkerEntity]
]
EntityLibraryHandlerAlias: TypeAlias = Callable[
    [str, int, int, int], tuple[int, Optional[AssessedBiomarkerEntity]]
]


class EntityHandler(TypedDict, total=False):
    api: dict[str, EntityAPIHandlerAlias]
    library: dict[str, EntityLibraryHandlerAlias]


# Citation handlers

CitationAPIHandlerAlias: TypeAlias = Callable[[Response], Optional[Citation]]
CitationLibraryHandlerAlias: TypeAlias = Callable[
    [str, int, int, int], tuple[int, Optional[Citation]]
]


class CitationHandler(TypedDict, total=False):
    api: dict[str, CitationAPIHandlerAlias]
    library: dict[str, CitationLibraryHandlerAlias]
