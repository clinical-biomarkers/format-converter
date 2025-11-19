from typing import Optional, Union
import logging
from pathlib import Path
from time import sleep
import requests
from requests import Response
from enum import Enum
from dotenv import load_dotenv

from utils import ROOT_DIR, load_json_type_safe, write_json
from utils.logging import LoggedClass, log_once
from utils.data_types import (
    CacheableDataModelObject,
    AssessedBiomarkerEntity,
    Citation,
    Condition,
    RateLimiter,
)
from .api import LIBRARY_CALL, METADATA_HANDLERS


class ApiCallType(Enum):
    ENTITY_TYPE = 1
    CITATION = 2
    CONDITION = 3


class Metadata(LoggedClass):

    def __init__(
        self,
        max_retries: int = 3,
        timeout: int = 5,
        sleep_time: int = 1,
        preload_caches: bool = False,
    ) -> None:
        super().__init__()
        load_dotenv()
        self._mapping_file_path = ROOT_DIR / "mapping_data" / "namespace_map.json"
        self.debug(f"Loading namespace map from {self._mapping_file_path}")
        self.namespace_map = load_json_type_safe(self._mapping_file_path, "dict")
        self._max_retries = max_retries
        self._timeout = timeout
        self._sleep_time = sleep_time
        self._rate_limiter = RateLimiter()

        self._preloaded_caches: dict[str, dict] = {}
        if preload_caches:
            self._preload_cache_files()

    def get_resource_data(self, resource: str) -> Optional[dict[str, str]]:
        exists, resource_clean = self._check_resource_existence(resource)
        if exists:
            return None
        self.debug(f"Getting resource data for {resource_clean}")
        return self.namespace_map[resource_clean]

    def get_display_name(self, resource: Optional[str]) -> Optional[str]:
        """Get the display name for a resource (used for frontend display).

        Falls back to full_name if display_name not present.
        """
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            return None

        # Try display_name first
        display_name = self.namespace_map[resource_clean].get("display_name")
        if display_name:
            return display_name

        # Fall back to full_name
        full_name = self.namespace_map[resource_clean].get("full_name")
        if not full_name:
            self.debug(f"No display name or full name found for {resource_clean}")
            return None

        return full_name

    def get_full_name(self, resource: Optional[str]) -> Optional[str]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            return None
        full_name = self.namespace_map[resource_clean].get("full_name")
        if not full_name:
            self.debug(f"No full name found for {resource_clean}")
            return None
        return full_name.title()

    def get_api(self, resource: str) -> tuple[Optional[str], Optional[int]]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            return None, None
        endpoint = self.namespace_map[resource_clean].get("api_endpoint")
        # If there is no endpoint, we just assume no rate limit (at least there shouldn't be)
        if not endpoint:
            log_once(
                self.logger,
                f"No API endpoint found for {resource_clean}",
                logging.WARNING,
            )
            return None, None
        rate_limit = self.namespace_map[resource_clean].get("rate_limit")
        if not rate_limit:
            log_once(
                self.logger,
                f"API endpoint found for {resource_clean} but no rate limit found",
                logging.WARNING,
            )
            return endpoint, None
        return endpoint, rate_limit

    def get_url_template(self, resource: str) -> Optional[str]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            return None
        url = self.namespace_map[resource_clean].get("url_template")
        if not url:
            self.debug(f"No url template found for {resource_clean}")
            return None
        return url

    def format_url(self, resource: str, id: str) -> Optional[str]:
        url_template = self.get_url_template(resource)
        if url_template is None:
            return None
        return url_template.format(id=id)

    def get_cache_path(self, resource: str) -> Optional[Path]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            return None
        cache_file_name = self.namespace_map[resource_clean].get("cache")
        if not cache_file_name:
            self.debug(f"No cache file found for {resource_clean}")
            return None
        return ROOT_DIR / "mapping_data" / cache_file_name

    def get_name_and_url(
        self, formatted_id: str
    ) -> tuple[Optional[str], Optional[str]]:
        """Takes the `{resource}:{id}` formatted ID and returns the full resource
        name and the formatted url.
        """
        id_parts = formatted_id.split(":")
        id_resource = id_parts[0]
        id = id_parts[-1]

        full_name = self.get_full_name(id_resource)

        url = self.get_url_template(id_resource)
        url = url.format(id=id) if url else url

        return full_name, url

    def get_cache_data(self, resource: str) -> Optional[dict]:
        """Get cache data for a resource."""
        # If caches are preloaded, check memory first
        if resource in self._preloaded_caches:
            return self._preloaded_caches[resource]

        # Otherwise load from disk
        cache_path = self.get_cache_path(resource)
        if cache_path is None or not cache_path.exists():
            return None

        try:
            return load_json_type_safe(filepath=cache_path, return_type="dict")
        except Exception as e:
            self.error(f"Failed to load cache for {resource} from {cache_path}: {e}")
            return None

    def fetch_metadata(
        self,
        fetch_flag: bool,
        call_type: ApiCallType,
        resource: str,
        id: str,
        **kwargs,
    ) -> tuple[int, Optional[CacheableDataModelObject]]:
        """Entry point for making an API call to fetch metadata.

        Parameters
        ----------
        fetch_flag: bool
            Determines whether to attempt an API call if the data isn't found locally
            in the cache files.
        call_type: ApiCallType
            The type of metadata being fetched.
        resource: str
            The resource to make the API call to.
        id: str
            The accession to make the API call with.

        Returns
        -------
        (int, AssessedBiomarkerEntity or Citation or Condition or None)
            An int indicating how many API calls were made, and the AssessedBiomarkerEntity
            data, Citation data, Condition data, or None if the process failed.
        """
        resource_clean = self._clean_string(string=resource, lower=True)
        id = self._clean_string(string=id, lower=False)

        # Check that the API endpoint exists in the namespace map
        base_endpoint, rate_limit = self.get_api(resource_clean)
        if not base_endpoint:
            return 0, None

        # Load the cache file
        cache = self.get_cache_data(resource_clean)
        if cache is None:
            log_once(
                self.logger, f"Failed to load cache for {resource}", logging.WARNING
            )
            return 0, None

        # Check if entry is already in our cache file
        if id in cache:
            self.debug(f"Found cached data for {resource}:{id}")
            found: Optional[Union[AssessedBiomarkerEntity, Citation, Condition]]
            cached_record = cache[id]
            match call_type:
                case ApiCallType.ENTITY_TYPE:
                    found = AssessedBiomarkerEntity.from_cache_dict(data=cached_record)
                case ApiCallType.CITATION:
                    found = Citation.from_cache_dict(data=cached_record)
                case ApiCallType.CONDITION:
                    full_name = self.get_full_name(resource)
                    full_name = full_name if full_name else ""
                    url = self.get_url_template(resource)
                    url = url.format(id=id) if url else ""
                    found = Condition.from_cache_dict(
                        data=cached_record,
                        id=f"{resource}:{id}",
                        resource=full_name,
                        url=url,
                    )
            return 0, found

        if not fetch_flag:
            return 0, None

        self._rate_limiter.add_limit(
            resource=resource_clean, calls=rate_limit, window=1
        )

        # Check that the corresponding API call handler exists for this resource
        if base_endpoint == LIBRARY_CALL:
            lib_handler = METADATA_HANDLERS["library"].get(resource_clean)
            if not lib_handler:
                self.warning(
                    f"No library handler found for {resource}, call type: {call_type}"
                )
                return 0, None
            api_call_count, processed_data = lib_handler(
                id,
                resource_clean,
                self._max_retries,
                self._timeout,
                self._sleep_time,
                self._rate_limiter,
                assessed_entity_type=kwargs.get("assessed_entity_type", None),  # type: ignore
            )
        else:
            api_handler = METADATA_HANDLERS["api"].get(resource_clean)
            if not api_handler:
                self.warning(f"No API handler found for {resource}")
                return 0, None
            api_call_count, response = self._api_call_handling(
                resource=resource_clean, endpoint=base_endpoint.format(id=id)
            )
            if response is None:
                return api_call_count, None
            processed_data = api_handler(response, id, **kwargs)

        # Save fetched data to cache if possible
        if processed_data is not None:
            try:
                save_data = processed_data.to_cache_dict()
                self._update_cache(
                    resource=resource_clean, id=id, data=save_data, cache=cache
                )
            except Exception as e:
                self.error(f"Failed updating cache for {resource}, {id}: {e}")

        return api_call_count, processed_data

    def _api_call_handling(
        self, resource: str, endpoint: str
    ) -> tuple[int, Optional[Response]]:

        attempt = 0
        while attempt < self._max_retries:
            try:
                # Check rate limit before making call
                self._rate_limiter.check_limit(resource=resource)

                response = requests.get(endpoint, timeout=self._timeout)
                # Record api call
                self._rate_limiter.record_call(resource=resource)

                if response.status_code != 200:
                    self.error(
                        (
                            f"API call failed for endpoint: {endpoint}\n"
                            f"Status code: {response.status_code}\nContent: {response.text}"
                        )
                    )
                    return attempt + 1, None

                self.info(
                    f"Made successful API call to {resource}, endpoint: {endpoint}"
                )

                return attempt + 1, response

            except (requests.Timeout, requests.ConnectionError) as e:
                self.warning(
                    (
                        f"Request {type(e).__name__} on attempt {attempt + 1} for "
                        f"endpoint {endpoint} from resource {resource}\n{e}"
                    )
                )
            except Exception as e:
                self.exception(
                    (
                        f"Unexpected error during API call (attempt {attempt + 1}/{self._max_retries}) "
                        f"for endpoint {endpoint} from resource {resource}\n{e}"
                    )
                )

            attempt += 1
            self.debug(f"Sleeping for {self._sleep_time} seconds...")
            sleep(self._sleep_time)

        log_once(
            self.logger,
            f"Failed to reach API for {resource}, at endpoint {endpoint} after {self._max_retries} attempts",
            logging.ERROR,
        )
        return attempt + 1, None

    def _preload_cache_files(self) -> None:
        self.info("Preloading cache files into memory...")
        for resource in self.namespace_map.keys():
            resource_clean = self._clean_string(resource)
            cache_path = self.get_cache_path(resource_clean)
            if cache_path is not None and cache_path.exists():
                self._preloaded_caches[resource_clean] = load_json_type_safe(
                    filepath=cache_path, return_type="dict"
                )
        self.info(f"Preloaded {len(self._preloaded_caches)} cache files")

    def save_cache_files(self) -> None:
        """Saves cache files back to disk if using prefetched cache data."""
        self.info("Saving cache files back to disk...")
        for resource in self.namespace_map.keys():
            resource_clean = self._clean_string(resource)
            cache_path = self.get_cache_path(resource_clean)
            if cache_path is not None and cache_path.exists():
                write_json(
                    filepath=cache_path,
                    data=self._preloaded_caches[resource_clean],
                    indent=2,
                )
        self.info(f"Saved {len(self._preloaded_caches)} cache files back to disk")

    def _update_cache(self, resource: str, id: str, data: dict, cache: dict) -> None:
        # Update memory cache if preloaded
        if resource in self._preloaded_caches:
            self._preloaded_caches[resource][id] = data
            return

        # Update disk cache
        cache_path = self.get_cache_path(resource)
        if cache_path is None:
            self.error(f"No cache path found for {resource}")
            return

        try:
            cache[id] = data
            write_json(filepath=cache_path, data=cache, indent=2)
        except Exception as e:
            self.error(f"Failed updating cache for {resource}:{id}\n{e}")

    def _check_resource_existence(self, resource: Optional[str]) -> tuple[bool, str]:
        if resource is None:
            self.error(f"Got `None` resource")
            return False, ""
        resource_clean = self._clean_string(resource)
        if resource_clean not in self.namespace_map:
            import traceback
            self.warning(
                f"Resource {resource} (cleaned: {resource_clean}) does not exist in namespace map\n"
                f"Call stack:\n{''.join(traceback.format_stack())}"
            )
            log_once(
                self.logger,
                f"Resource {resource} (cleaned: {resource_clean}) does not exist in namespace map",
                logging.WARNING,
            )
            return False, resource_clean
        return True, resource_clean

    def _clean_string(self, string: str, lower: bool = True) -> str:
        if lower:
            return string.strip().lower()
        return string.strip()
