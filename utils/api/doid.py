from typing import Optional
from requests import Response
import re
import logging

from utils.logging import LoggedClass, log_once
from utils.data_types import (
    Condition,
    ConditionSynonym,
    ConditionRecommendedName,
    APIHandler,
    CacheableDataModelObject,
)


class DoidHandler(APIHandler, LoggedClass):
    """Handles Disease Ontology API responses."""

    def __call__(
        self, response: Response, id: str, **kwargs
    ) -> Optional[CacheableDataModelObject]:
        resource = str(kwargs.get("resource_name", ""))
        url = str(kwargs.get("condition_url", "")).format(id)

        try:
            doid_data = response.json()
            name = doid_data.get("name", "")

            description = doid_data.get("definition", "")
            description = re.search('"(.*)"', description)
            if description is not None:
                description = description.group(1)

            synonyms = doid_data.get("synonyms", [])
            synonyms = [
                s.replace("EXACT", "").strip() for s in synonyms if "EXACT" in synonyms
            ]
            condition = Condition(
                id=f"{resource}:{id}",
                recommended_name=ConditionRecommendedName(
                    id=f"{resource}:{id}",
                    name=name,
                    description=description if description else "",
                    resource=resource,
                    url=url,
                ),
                synonyms=[
                    ConditionSynonym(id=f"{resource}:{id}", name=s, resource="", url="")
                    for s in synonyms
                ],
            )
            return condition
        except KeyError as e:
            log_once(
                self.logger,
                f"Missing required field in DOID response for ID: {id}\n{e}",
                logging.ERROR,
            )
            return None
        except Exception as e:
            log_once(
                self.logger,
                f"Error processing DOID response for ID: {id}\n{e}",
                logging.ERROR,
            )
            return None


doid_handler = DoidHandler()
