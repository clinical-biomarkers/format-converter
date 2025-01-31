from typing import Optional
from requests import Response
import logging

from .data_types import APIHandler
from utils.logging import LoggedClass, log_once
from utils.data_types import AssessedBiomarkerEntity, Synonym


class CellOntologyHandler(APIHandler, LoggedClass):
    """Handles Cell Ontology API responses."""

    def __call__(self, response: Response, id: str) -> Optional[AssessedBiomarkerEntity]:
        try:
            co_data = response.json()
            recommended_name = co_data["label"]
            synonyms = [syn for syn in co_data.get("synonyms", [])]
            return_data = AssessedBiomarkerEntity(
                recommended_name=recommended_name,
                synonyms=[Synonym(synonym=s) for s in synonyms],
            )
            return return_data
        except KeyError as e:
            log_once(
                self.logger,
                f"Missing required field in Cell Ontology response for ID: {id}\n{e}",
                logging.ERROR,
            )
            return None
        except Exception as e:
            log_once(
                self.logger,
                f"Error processing Cell Ontology response for ID: {id}\n{e}",
                logging.ERROR,
            )
            return None


cell_ontology_handler = CellOntologyHandler()
