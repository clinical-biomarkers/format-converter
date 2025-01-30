from typing import Optional, Union, overload, Literal
import logging
from pathlib import Path
from time import sleep
import requests
from requests import Response
from enum import Enum
from dotenv import load_dotenv

from utils import ROOT_DIR, load_json_type_safe
from utils.logging import LoggedClass, log_once
from utils.data_types import AssessedBiomarkerEntity, Citation
from .api import ENTITY_HANDLERS, CITATION_HANDLERS, LIBRARY_CALL
from .api.data_types import RateLimiter


class ApiCallType(Enum):
    ENTITY_TYPE = 1
    CITATION = 2


class Metadata(LoggedClass):

    def __init__(
        self, max_retries: int = 3, timeout: int = 5, sleep_time: int = 1
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

    def get_resource_data(self, resource: str) -> Optional[dict[str, str]]:
        exists, resource_clean = self._check_resource_existence(resource)
        if exists:
            return None
        self.debug(f"Getting resource data for {resource_clean}")
        return self.namespace_map[resource_clean]

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
            log_once(self.logger, f"No API endpoint found for {resource_clean}", logging.WARNING)
            return None, None
        rate_limit = self.namespace_map[resource_clean].get("rate_limit")
        if not rate_limit:
            log_once(self.logger, f"API endpoint found for {resource_clean} but no rate limit found", logging.WARNING)
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

    def get_cache_path(self, resource: str) -> Optional[Path]:
        exists, resource_clean = self._check_resource_existence(resource)
        if not exists:
            return None
        cache_file_name = self.namespace_map[resource_clean].get("cache")
        if not cache_file_name:
            self.debug(f"No cache file found for {resource_clean}")
            return None
        return ROOT_DIR / "mapping_data" / cache_file_name

    @overload
    def fetch_metadata(
        self,
        fetch_flag: bool,
        call_type: Literal[ApiCallType.ENTITY_TYPE],
        resource: str,
        id: str,
    ) -> tuple[int, Optional[AssessedBiomarkerEntity]]:
        pass

    @overload 
    def fetch_metadata(
        self,
        fetch_flag: bool,
        call_type: Literal[ApiCallType.CITATION],
        resource: str,
        id: str,
    ) -> tuple[int, Optional[Citation]]:
        pass

    def fetch_metadata(
        self,
        fetch_flag: bool,
        call_type: ApiCallType,
        resource: str,
        id: str,
    ) -> tuple[int, Optional[Union[AssessedBiomarkerEntity, Citation]]]:
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
        entity_type: str or None, optional
            The assessed entity type.

        Returns
        -------
        (int, AssessedBiomarkerEntity or Citation or None)
            An int indicating how many API calls were made, and the AssessedBiomarkerEntity
            data, Citation data, or None if the process failed.
        """
        resource_clean = self._clean_string(string=resource, lower=True)
        id = self._clean_string(string=id, lower=False)

        # Check that the API endpoint exists in the namespace map
        base_endpoint, rate_limit = self.get_api(resource_clean)
        if not base_endpoint:
            log_once(self.logger, f"No API endpoint availble for {resource}", logging.WARNING)
            return 0, None

        # Check that a corresponding cache file path exists
        cache_path = self.get_cache_path(resource)
        if cache_path is None:
            self.error(f"No cache path found for {resource}")
            return 0, None
        if not cache_path.exists():
            self.error(f"Cache file at {cache_path} does not exist")
            return 0, None

        # Load the cache file
        cache = load_json_type_safe(cache_path, "dict")
        # Check if entry is already in our cache file
        if id in cache:
            self.debug(f"Found cached data for {resource}:{id}")
            found: Union[AssessedBiomarkerEntity, Citation]
            cached_record = cache[id]
            match call_type:
                case ApiCallType.ENTITY_TYPE:
                    found = AssessedBiomarkerEntity(
                        recommended_name=cached_record["recommended_name"],
                        synonyms=cached_record["synonyms"],
                    )
                case ApiCallType.CITATION:
                    found = Citation(
                        title=cached_record["title"], 
                        journal=cached_record["journal"], 
                        authors=cached_record["authors"], 
                        date=cached_record["publication_date"], 
                        reference=[], 
                        evidence=[]
                    )
            return 0, found

        if not fetch_flag:
            return 0, None

        self._rate_limiter.add_limit(resource=resource_clean, calls=rate_limit, window=1)

        handler_map = (
            ENTITY_HANDLERS 
            if call_type == ApiCallType.ENTITY_TYPE 
            else CITATION_HANDLERS
        )

        # Check that the corresponding API call handler exists for this resource
        if base_endpoint == LIBRARY_CALL:
            lib_handler = handler_map.get("library", {}).get(resource_clean)
            if not lib_handler:
                self.warning(f"No library handler found for {resource}, call type: {call_type}")
                return 0, None
            return lib_handler(id, self._max_retries, self._timeout, self._sleep_time)
        else:
            api_handler = handler_map["api"].get(resource_clean)
            if not api_handler:
                self.warning(f"No API handler found for {resource}")
                return 0, None
            api_call_count, response = self._api_call_handling(resource=resource_clean, endpoint=base_endpoint.format(id))
            if response is None:
                return api_call_count, None
            processed_data = api_handler(response, id)
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
                        f"API call failed for endpoint: {endpoint}\nStatus code: {response.status_code}\nContent: {response.text}"
                    )
                    return attempt + 1, None

                self.info(
                    f"Made successful API call to {resource}, endpoint: {endpoint}"
                )

                return attempt + 1, response

            except (requests.Timeout, requests.ConnectionError) as e:
                self.warning(
                    f"Request {type(e).__name__} on attempt {attempt + 1} for endpoint {endpoint} from resource {resource}\n{e}"
                )
            except Exception as e:
                self.exception(
                    f"Unexpected error during API call (attempt {attempt + 1}/{self._max_retries}) for endpoint {endpoint} from resource {resource}\n{e}"
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

    def _check_resource_existence(self, resource: Optional[str]) -> tuple[bool, str]:
        if resource is None:
            self.error(f"Got `None` resource")
            return False, ""
        resource_clean = self._clean_string(resource)
        if resource_clean not in self.namespace_map:
            log_once(self.logger, f"Resource {resource} does not exist in namespace map", logging.WARNING)
            return False, resource_clean
        return True, resource_clean

    def _clean_string(self, string: str, lower: bool = True) -> str:
        if lower:
            return string.strip().lower()
        return string.strip()
