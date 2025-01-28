from utils import ROOT_DIR, load_json_type_safe
from utils.logging import LoggedClass, log_once
from utils.data_types import AssessedBiomarkerEntity
from .api import API_HANDLER_MAP
from typing import Optional
import logging
import json
from pathlib import Path
from time import sleep
import requests


class Metadata(LoggedClass):

    def __init__(
        self, max_retries: int = 3, timeout: int = 5, sleep_time: int = 1
    ) -> None:
        super().__init__()
        self._mapping_file_path = ROOT_DIR / "mapping_data" / "namespace_map.json"
        self.debug(f"Loading namespace map from {self._mapping_file_path}")
        self.namespace_map = load_json_type_safe(self._mapping_file_path, "dict")
        self._max_retries = max_retries
        self._timeout = timeout
        self._sleep_time = sleep_time

    def get_resource_data(self, resource: str) -> Optional[dict[str, str]]:
        exists, resource_clean = self._check_resource_existence(resource)
        if exists:
            self.debug(f"Resource {resource} does not exist in namespace map")
            return None
        self.debug(f"Getting resource data for {resource_clean}")
        return self.namespace_map[resource_clean]

    def get_full_name(self, resource: Optional[str]) -> Optional[str]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            self.debug(f"Resource {resource} does not exist in namespace map")
            return None
        full_name = self.namespace_map[resource_clean].get("full_name")
        if not full_name:
            self.debug(f"No full name found for {resource_clean}")
            return None
        return full_name.title()

    def get_api_endpoint(self, resource: str) -> Optional[str]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            self.debug(f"Resource {resource} does not exist in namespace map")
            return None
        endpoint = self.namespace_map[resource_clean].get("api_endpoint")
        if not endpoint:
            self.debug(f"No api endpoint found for {resource_clean}")
            return None
        return endpoint

    def get_url_template(self, resource: str) -> Optional[str]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            self.debug(f"Resource {resource} does not exist in namespace map")
            return None
        url = self.namespace_map[resource_clean].get("url_template")
        if not url:
            self.debug(f"No url template found for {resource_clean}")
            return None
        return url

    def get_cache_path(self, resource: str) -> Optional[Path]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            self.debug(f"Resource {resource} does not exist in namespace map")
            return None
        cache_file_name = self.namespace_map[resource_clean].get("cache")
        if not cache_file_name:
            self.debug(f"No cache file found for {resource_clean}")
            return None
        return ROOT_DIR / "mapping_data" / cache_file_name

    def entity_type_api_call(
        self, resource: str, id: str, entity_type: Optional[str] = None
    ) -> tuple[int, Optional[AssessedBiomarkerEntity]]:
        """Get the entity type recommended name and synonym data.

        Parameters
        ----------
        resource: str
            The resource to make the API call to.
        id: str
            The accession to make the API call with.
        entity_type: str or None, optional
            The assessed entity type.

        Returns
        -------
        (int, AssessedBiomarkerEntity or None)
            An int indicating how many API calls were made, and the AssessedBiomarkerEntity
            data or None if the process failed.
        """
        # Check the resource exists in the namespace map
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            self.debug(f"Resource {resource} does not exist in namespace map")
            return 0, None

        # Check that the API endpoint exists in the namespace map
        endpoint = self.get_api_endpoint(resource_clean)
        if not endpoint:
            msg = (
                f"No API endpoint availble for {resource} of type {entity_type}"
                if entity_type
                else f"No API endpoint availble for {resource}"
            )
            log_once(self.logger, msg, logging.WARNING)
            return 0, None

        # Begin API call handling
        self.debug(f"Making API call for {resource} with ID {id} (type: {entity_type})")

        # First check the cache and make sure an API call is absolutely necessary
        cache_path = self.get_cache_path(resource)
        if cache_path is None:
            self.error(f"No cache path found for {resource}")
            return 0, None

        cache = load_json_type_safe(cache_path, "dict")
        if id in cache:
            self.debug(f"Found cached data for {id}")
            cached_record = cache[id]
            assessed_biomarker_entity = AssessedBiomarkerEntity(
                recommended_name=cached_record["recommended_name"],
                synonyms=cached_record["synonyms"],
            )
            return 0, assessed_biomarker_entity

        # Check that the corresponding API call handler exists for this resource
        handler = API_HANDLER_MAP.get(resource_clean)
        if handler is None:
            self.warning(f"Corresponding API handler for {resource} does not exist")
            return 0, None

        # Attempt API calls
        attempt = 0
        while attempt < self._max_retries:
            try:
                # Make request
                response = requests.get(endpoint.format(id), timeout=self._timeout)
                if response.status_code != 200:
                    self.error(
                        f"API call failed for {id}: {response.status_code} - {response.text}"
                    )
                    return attempt + 1, None

                self.info(f"Made successful API call for {resource}:{id}")

                # Pass the return response to the resource specific API handler
                processed_data = API_HANDLER_MAP[resource_clean](response)
                if processed_data is None:
                    log_once(
                        self.logger,
                        f"Unable to process data for {resource}:{id}",
                        logging.WARNING,
                    )
                    return attempt + 1, None
                else:
                    self.debug(f"Adding entry for {resource}:{id} to {cache_path}")
                    cache[id] = processed_data.to_dict()
                    with open(cache_path, "w") as f:
                        json.dump(cache, f, index=2)
                    return attempt + 1, processed_data

            except requests.Timeout as e:
                self.warning(
                    f"Request timeout on attempt {attempt + 1} for {resource}:{id}\n{e}"
                )
                attempt += 1
                self.debug(f"Sleeping for {self._sleep_time} seconds...")
                sleep(self._sleep_time)
            except requests.ConnectionError as e:
                self.warning(
                    f"Connection error on attempt {attempt + 1} for {resource}:{id}\n{e}"
                )
                attempt += 1
                self.debug(f"Sleeping for {self._sleep_time} seconds...")
                sleep(self._sleep_time)
            except Exception as e:
                self.exception(
                    f"Unexpected error during API call for {resource}:{id}\n{e}"
                )
                attempt += 1
                self.debug(f"Sleeping for {self._sleep_time} seconds...")
                sleep(self._sleep_time)

        log_once(
            self.logger,
            f"Failed to reach API for {resource}:{id} after {self._max_retries} attempts",
            logging.ERROR,
        )
        return attempt + 1, None

    def _check_resource_existence(self, resource: Optional[str]) -> tuple[bool, str]:
        if resource is None:
            return False, ""
        resource_clean = self._clean_string(resource)
        if resource_clean not in self.namespace_map:
            return False, resource_clean
        return True, resource_clean

    def _clean_string(self, string: str) -> str:
        return string.strip().lower()
