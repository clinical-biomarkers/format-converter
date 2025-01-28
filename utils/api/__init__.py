from .uniprot import uniprot_handler

# Keys should match format in the namespace map
API_HANDLER_MAP = {"upkb": uniprot_handler}
