# Namespace Map

The `namespace_map.json` file in the `mapping_data/` directory serves as the central configuration for the TSV to JSON converter logic. Any primary databsae (namespace) used in our data should have an entry in the namespace map containing various metadata. Those fields are responsible for various processes:

### Evidence Object Building

The `Evidence` object from `json_types.py` looks like this:

```py
@dataclass
class Evidence(DataModelObject):
    id: str
    database: str
    url: str
    evidence_list: list[EvidenceItem]
    tags: list[EvidenceTag]
```

An example evidence object in JSON form looks like this:

```json
{
  "id": "32479790",
  "database": "PubMed",
  "evidence_list": [
    {
      "evidence": "IL-6 plays multifaceted roles in regulation of vascular leakage, complement activation, and coagulation pathways, which ultimately causes poor outcomes for acute respiratory distress syndrome, multiple organ dysfunction syndrome, and SARS."
    }
  ],
  "tags": [
    {
      "tag": "biomarker"
    },
    {
      "tag": "assessed_biomarker_entity"
    },
    {
      "tag": "specimen:UBERON:0000178"
    }
  ],
  "url": "https://pubmed.ncbi.nlm.nih.gov/32479790"
}
```

The building of the `Evidence` object requires these fields from the namespace map:

- `full_name`: This is what gets populated as the `database` field in the final `evidence_source` object
- `url_template`: This is what is used to build the `url` field in the final `evidence_source` object

### Metdata Building

Metadata can be automatically populated for assessed biomarker entities, conditions, and citations. Additional information on how to add metadata handlers can be found in the [metadata documentation](./Metadata.md).

If metadata is being retrieved, the required fields from the namespace map are:

- `api_endpoint`: The REST endpoint to grab the information from (a special case exists where the hardcoded value `library_call` can be used if using any type of intermediary library or wrapper is used rather than directly interacting with the API through a `GET` call)
- `rate_limit`: If the target API has a known rate limit, the underlying metadata functionality will automatically enforce it with exponential backoff (if `null`, the code will still retry with exponential backoff on failure but will not assume any rate limit and will potentially hit the target API many times in quick succession)
- `cache`: The name of the local JSON file where retrieved results will be stored, this prevents making repeated API calls for information that has already been retrieved

### Cross Reference Building

When building and injecting cross references, the file listed for the `xref` field is used to find the cross reference metadata.
