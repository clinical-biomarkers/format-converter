# Metadata Workflow

Currently, there are 3 types of objects that can be built from metadata fetching:

1. [`AssessedBiomarkerEntity`](https://github.com/clinical-biomarkers/format-converter/blob/0b6017a8a47eede50f749066d06394e3f46b2068/utils/data_types/json_types.py#L97):
   Attempts to fetch the recommended name and list of synonyms for the entity.
2. [`Condition`](https://github.com/clinical-biomarkers/format-converter/blob/0b6017a8a47eede50f749066d06394e3f46b2068/utils/data_types/json_types.py#L360):
   Attempts to fetch the recommended name and list of synonyms for the condition.
3. [`Citation`](https://github.com/clinical-biomarkers/format-converter/blob/0b6017a8a47eede50f749066d06394e3f46b2068/utils/data_types/json_types.py#L514):
   Attempts to fetch the title, journal, authors, and publication date of a paper.

## Metadata Manager

The central component is the [`Metadata`](https://github.com/clinical-biomarkers/format-converter/blob/f226f756d01ec1c723c5c9f4b421b21df46a80bb/utils/metadata.py#L28)
class, which:

- Handles API rate limiting (if applicable)
- Handles caching of results
- Provides unified access to different metadata sources
- Validates and cleans resource identifiers

## Resource Handlers

There are 2 types of metadata handlers:

1. [`API Handler`](https://github.com/clinical-biomarkers/format-converter/blob/f226f756d01ec1c723c5c9f4b421b21df46a80bb/utils/data_types/api.py#L11):

   - An API handler is a generic handler where the target resource exposes a standard REST API endpoint where the metadata can be retrieved from using a `GET` request
   - Processes standard HTTP responses
   - API handlers take the response data from the `GET` request as input, so API handlers are not responsible for any network requests themselves, just extracting the data from the API response
   - Example: [`UniprotHandler`](https://github.com/clinical-biomarkers/format-converter/blob/main/utils/api/uniprot.py)

2. [`Library Handler`](https://github.com/clinical-biomarkers/format-converter/blob/f226f756d01ec1c723c5c9f4b421b21df46a80bb/utils/data_types/api.py#L21):
   - A library handler is a handler that interacts with a resource's API indirectly or by a REST call that is NOT a `GET` request
   - Unlike an API handler, the library handler itself is responsible for actually making the network request, whether that is through a third party library or a non-`GET` request
   - Example: [`PubmedHandler`](https://github.com/clinical-biomarkers/format-converter/blob/main/utils/api/pubmed.py)

## Fetching Metadata

The entry point to retrieving metadata is the [`fetch_metadata`](https://github.com/clinical-biomarkers/format-converter/blob/0b6017a8a47eede50f749066d06394e3f46b2068/utils/metadata.py#L151) method on the `Metadata` class.

```py
# Check that the API endpoint exists in the namespace map
base_endpoint, rate_limit = self.get_api(resource_clean)
if not base_endpoint:
    return 0, None
```

First, the function starts by checking that the `api_endpoint` value for that resource in the namespace map is not `None`. If it is, then this is not a resource which is configured to retrieve metadata for and `None` is returned.

```py
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
```

All metadata enabled resources should have a corresponding cache file. This allows us to responsibly hit the resource API without repeated unnecessary calls. If a corresponding cache file is not found fo the resource, a warning is logged and the function returns `None`. If the cache file is found, we check if the ID of the resource is already present in the cache file. If it is found, we build the target object from the cached record and return it. If it is not found, we continue to the `fetch_flag` check. The `fetch_flag` allows us to build metadata from cached data but not call APIs, in the case we want to avoid making calls but still attempt to retrieve locally cached data.

```py
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
```

In order to hand off the processing of the metadata to the correct resource specific handler, we check the `api_endpoint` method.

If the resource is specified as a `library_handler`:

- The corresponding resource handler is grabbed
- All the data required by the resource handler is passed along to the library handler
- The library handler will handle the actual request

If the resource is specified as a regular `GET` request API endpoint:

- The corresponding resource handler is grabbed
- The `GET` request is performed by a standardized method, the `_api_call_handling` method
  - The actual request is handled by this method BEFORE passing off to the handler
  - This is the main difference from a library handler
- The response data from the `GET` request is passed off to the handler to process

## Adding a New Metadata Handler

1. Define Resource Configuration
   - Add the resource to the [`namespace_map.json`](https://github.com/clinical-biomarkers/format-converter/blob/main/mapping_data/namespace_map.json)
2. Determine whether the API should be an API or library handler
3. Create the handler in the `format-converter/utils/api` directory
4. Inherit the correct abstract class for either `APIHandler` or `LibraryHandler`
5. Add the singleton instance to the correct map in the `format-converter/utils/api/__init__.py` file

Depending on the handler type, highly recommended to follow conventions set in either the `uniprot.py` for API handlers and `pubmed.py` for library handlers.
