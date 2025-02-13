import requests
import os
import logging
from typing import Optional
from time import sleep
import xml.etree.ElementTree as ET

from utils.logging import LoggedClass, log_once
from utils.data_types import (
    LibraryHandler,
    AssessedBiomarkerEntity,
    Synonym,
    CacheableDataModelObject,
    RateLimiter,
)

ENDPOINT = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db={db}&id={id}&api_key={api_key}&email={email}"


class NCBIHandler(LibraryHandler, LoggedClass):
    """Handles NCBI gene responses."""

    def __call__(
        self,
        id: str,
        resource: str,
        max_retries: int = 3,
        timeout: int = 5,
        sleep_time: int = 1,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs,
    ) -> tuple[int, Optional[CacheableDataModelObject]]:
        assessed_entity_type = kwargs.get("assessed_entity_type")
        if not assessed_entity_type or assessed_entity_type.lower().strip() != "gene":
            log_once(
                self.logger,
                f"Unsupported entity type `{assessed_entity_type}` for NCBI, only support genes",
                logging.WARNING,
            )
            return 0, None

        email = os.getenv("EMAIL")
        if email is None:
            log_once(
                self.logger,
                "Failed to find EMAIL environment variable. Check .env file. Skipping PubMed API calls...",
                logging.ERROR,
            )
            return 0, None

        api_key = os.getenv("PUBMED_API_KEY")
        if api_key is None:
            log_once(
                self.logger,
                "Failed to find PUBMED_API_KEY environment variable. Check .env file. PubMed API calls will likely rate limit",
                logging.ERROR,
            )
            return 0, None

        endpoint = ENDPOINT.format(
            db="gene", id=id.strip(), api_key=api_key, email=email
        )
        self.debug(f"Attempting NCBI API call for endpoint: {endpoint}")

        attempt = 0
        while attempt < max_retries:
            try:
                # Check rate limit before call
                self._check_limit(resource=resource, rate_limiter=rate_limiter)

                response = requests.get(endpoint, timeout=timeout)

                # Record the API call
                self._record_call(resource=resource, rate_limiter=rate_limiter)

                if response.status_code != 200:
                    self.error(
                        f"NCBI API call failed for ID {id}:\n"
                        f"Status: {response.status_code}\n"
                        f"Response: {response.text}"
                    )
                    return attempt + 1, None

                # Parse XML response
                root = ET.fromstring(response.content)
                doc_summary = root.find(".//DocumentSummary")

                if doc_summary is None:
                    self.error(f"No DocumentSummary found for NCBI ID {id}")
                    return attempt + 1, None

                name_elem = doc_summary.find("Name")
                aliases_elem = doc_summary.find("OtherAliases")

                if name_elem is None:
                    self.error(f"No Name element found for NCBI ID {id}")
                    return attempt + 1, None

                # Get name and synonyms
                name = name_elem.text
                synonyms = []
                if aliases_elem is not None and aliases_elem.text:
                    synonyms = [s.strip() for s in aliases_elem.text.split(",")]

                return attempt + 1, AssessedBiomarkerEntity(
                    recommended_name=name,
                    synonyms=[Synonym(synonym=s) for s in synonyms],
                )

            except (requests.ConnectionError, requests.Timeout) as e:
                self.warning(
                    f"Connection error on attempt {attempt + 1} for NCBI ID {id}: {e}"
                )
                attempt += 1
                if attempt < max_retries:
                    sleep(sleep_time)
                continue

            except Exception as e:
                self.exception(f"Error processing NCBI ID {id}: {e}")
                return attempt + 1, None

        self.error(
            f"Failed to retrieve NCBI data for ID {id} after {max_retries} attempts"
        )
        return attempt + 1, None


ncbi_handler = NCBIHandler()
