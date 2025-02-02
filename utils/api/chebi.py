from typing import Optional
from requests import Response
import logging
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

from utils.logging import LoggedClass, log_once
from utils.data_types import (
    AssessedBiomarkerEntity,
    Synonym,
    APIHandler,
    CacheableDataModelObject,
)


class ChebiHandler(APIHandler, LoggedClass):

    def __call__(
        self, response: Response, id: str
    ) -> Optional[CacheableDataModelObject]:
        try:
            root = ET.fromstring(response.content)
            ns = {"chebi": "https://www.ebi.ac.uk/webservices/chebi"}

            chebi_name_element = root.find(".//chebi:chebiAsciiName", ns)
            chebi_name = chebi_name_element.text if chebi_name_element else None
            if chebi_name is None:
                raise ValueError(f"")

            synonyms: list[str] = []
            synonym_elements = root.findall(".//chebi:Synonyms", ns)
            for syn_el in synonym_elements:
                syn = syn_el.find("chebi:data", ns)
                if syn is not None:
                    syn_text = syn.text
                    if syn_text is not None:
                        synonyms.append(syn_text)

            return AssessedBiomarkerEntity(
                recommended_name=chebi_name,
                synonyms=[Synonym(synonym=s) for s in synonyms],
            )
        except ParseError as e:
            log_once(
                self.logger,
                f"Error parsing return data for Chebi ID: {id}\n{e}",
                logging.ERROR,
            )
            return None
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
