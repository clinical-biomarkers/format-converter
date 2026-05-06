from typing import Optional
from requests import Response
import logging

from utils.logging import LoggedClass, log_once
from utils.data_types import (
    AssessedBiomarkerEntity,
    Synonym,
    APIHandler,
    CacheableDataModelObject,
)


class ChebiHandler(APIHandler, LoggedClass):

    def __call__(
        self, response: Response, id: str, **kwargs
    ) -> Optional[CacheableDataModelObject]:
        try:
            data = response.json()
            chebi_name = data.get("ascii_name")
            if chebi_name is None:
                raise ValueError("")
            synonyms: list[str] = []
            for name_list in data.get("names", {}).values():
                for entry in name_list:
                    syn_text = entry.get("ascii_name")
                    if syn_text and syn_text != chebi_name:
                        synonyms.append(syn_text)

            return AssessedBiomarkerEntity(
                recommended_name=chebi_name,
                synonyms=[Synonym(synonym=s) for s in synonyms],
            )
        except ValueError as e:
            log_once(
                self.logger,
                f"Error parsing recommended name for Chebi ID: {id}\n{e}",
                logging.ERROR,
            )
            return None
        except Exception as e:
            log_once(
                self.logger,
                f"Error processing data for Chebi ID: {id}\n{e}",
                logging.ERROR,
            )
            return None


chebi_handler = ChebiHandler()
