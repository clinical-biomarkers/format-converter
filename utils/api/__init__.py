from typing import Callable, Optional
from requests import Response

from .uniprot import uniprot_handler
from .pubmed import pubmed_handler
from .data_types import EntityHandler, CitationHandler
from utils.data_types import AssessedBiomarkerEntity, Citation

# If an endpoint is listed as a library call in the namespace map, pass processing entirely to the handler
LIBRARY_CALL = "library_call"


# KEYS SHOULD MATCH FORMAT IN THE NAMESPACE MAP

entity_api_handlers: dict[
    str, Callable[[Response], Optional[AssessedBiomarkerEntity]]
] = {"upkb": uniprot_handler}
entity_library_handlers: dict[
    str,
    Callable[
        [str, int, int, int],
        tuple[int, Optional[AssessedBiomarkerEntity]],
    ],
] = {}

citation_api_handlers: dict[str, Callable[[Response], Optional[Citation]]] = {}
citation_library_handlers: dict[
    str,
    Callable[
        [str, int, int, int],
        tuple[int, Optional[Citation]],
    ],
] = {"pubmed": pubmed_handler}


ENTITY_HANDLERS: EntityHandler = {
    "api": entity_api_handlers,
    "library": entity_library_handlers,
}
CITATION_HANDLERS: CitationHandler = {
    "api": citation_api_handlers,
    "library": citation_library_handlers,
}
