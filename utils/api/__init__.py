from .uniprot import uniprot_handler
from .pubmed import pubmed_handler
from .cell_ontology import cell_ontology_handler
from .chebi import chebi_handler
from .doid import doid_handler
from .data_types import (
    EntityHandler,
    CitationHandler,
    ConditionHandler,
    EntityAPIHandlerAlias,
    EntityLibraryHandlerAlias,
    CitationAPIHandlerAlias,
    CitationLibraryHandlerAlias,
    ConditionAPIHandlerAlias,
    ConditionLibraryHandlerAlias,
)

# If an endpoint is listed as a library call in the namespace map, pass processing entirely to the handler
LIBRARY_CALL = "library_call"


# KEYS SHOULD MATCH FORMAT IN THE NAMESPACE MAP

entity_api_handlers: dict[str, EntityAPIHandlerAlias] = {
    "upkb": uniprot_handler,
    "co": cell_ontology_handler,
    "chebi": chebi_handler,
}
entity_library_handlers: dict[str, EntityLibraryHandlerAlias] = {}

citation_api_handlers: dict[str, CitationAPIHandlerAlias] = {}
citation_library_handlers: dict[str, CitationLibraryHandlerAlias] = {
    "pubmed": pubmed_handler
}

condition_api_handlers: dict[str, ConditionAPIHandlerAlias] = {"doid": doid_handler}
condition_library_handlers: dict[str, ConditionLibraryHandlerAlias] = {}

ENTITY_HANDLERS: EntityHandler = {
    "api": entity_api_handlers,
    "library": entity_library_handlers,
}
CITATION_HANDLERS: CitationHandler = {
    "api": citation_api_handlers,
    "library": citation_library_handlers,
}
CONDITION_HANDLERS: ConditionHandler = {
    "api": condition_api_handlers,
    "library": condition_library_handlers,
}
