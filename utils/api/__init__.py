from .uniprot import uniprot_handler
from .pubmed import pubmed_handler
from .cell_ontology import cell_ontology_handler
from .chebi import chebi_handler
from .doid import doid_handler
from utils.data_types import EntityHandlerMap

# If an endpoint is listed as a library call in the namespace map, pass processing entirely to the handler
LIBRARY_CALL = "library_call"

METADATA_HANDLERS: EntityHandlerMap = {
    "api": {
        "upkb": uniprot_handler,
        "co": cell_ontology_handler,
        "chebi": chebi_handler,
        "doid": doid_handler,
    },
    "library": {
        "pubmed": pubmed_handler,
    },
}
