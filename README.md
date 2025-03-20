# Data Format Conversion

The project's data can be viewed in multiple formats. The main view (the JSON data model), is the main format that comprehensively captures the
complexity and nested nature of the biomarker data. However, for simple biomarker queries and curation, the data can also be viewed in a table (TSV)
format. Due to the hierarchical and nested nature of the data model, the table view is a simplified version of the data model where each entry is
unrolled into (if applicable) multiple TSV rows. The third format is in N-Triples (NT). The triple conversion is a one-way conversion that goes from JSON to NT.

## Usage

The code in this directory handles the logic for the data conversion. The entry point is the `main.py` script.

```
usage: main.py [-h] [-m] [-p] [-x] [--debug] [--log-dir LOG_DIR] [--rotate-logs] [--no-console] input output

positional arguments:
  input                Path to input file
  output               Path to output file

options:
  -h, --help           show this help message and exit

TSV to JSON options:
  -m, --metadata       Whether to fetch synonym and recommended name metdata from APIs (default true)
  -p, --preload-cache  Whether to preload the cache data (default false)

Cross reference options:
  -x, --xref           Whether to inject cross references, can only be run on its own and not combined with other conversions (accepts directories for input arg)

logging options:
  --debug              Run in debug mode
  --log-dir LOG_DIR    Directory for log file (default: /data/shared/repos/format-converter/logs)
  --rotate-logs        Enable log rotation by date
  --no-console         Disable console logging output
```

## TSV to JSON Notes

The TSV to JSON conversion can handle the filling in of some metadata automatically. Pubmed paper citation data and some `assessed_biomarker_entity`
/ `citation` metdata data can be automatically retrieved from API calls. In order for the TSV to JSON conversion to utilize the NCBI and PubMed
APIs a `.env` file is expected containing your email and and an API key (instructions for obtaining an API key can be found
[here](https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities/)). The `.env` file is expected to be in the root directory
of the repository and should have the following structure:

```
EMAIL='example@example.com'
PUBMED_API_KEY='key'
```

In fetching metadata, the information based on the resource ID/accession is first looked for in its corresponding local mapping file. If not found,
and there is a corresponding handler for the resource, an API call will be made to attempt to automatically fetch the information. By default, these
local mapping (or cache) files are loaded and written on demand, which leads to lower memory overhead but a large amount of slow IO calls.
Depending on the host machine, the `-p`/`--preload-cache` flag can be used to preload all the mapping files in before the conversion is started.
The cache files will be kept in memory until the conversion is finished and then written back out to disk. This approach is much faster at
runtime but not always feasible depending on the resources available. If you would like to avoid attempting API/network calls entirely, the `-m`/
`--metadata` flag can be used. Note that the converter will still attempt to search the local cache files for the data, but will skip passing off
the data to the resource handler if not found.

## Cross-reference Handling

The `-x`/`--xref` argument will inject cross-reference data to a JSON data model file(s). If a directory is passed for the `input` positional arg, 
a directory is expected for the `output` positional arg. Similarly, if a file is passed for the `input` arg, a file is expected for the `output` arg.

### Adding a New Cross-reference
Whenever a new cross-reference JSON file is added to `mapping_data/xrefs`, it needs to be added to the namespace map at `mapping_data/namespace_map.json`.
The namespace map is the central config for this repository. The primary databases in the knowledgebase should all have an entry in the namespace map since
the cross-reference and any secondary cross-references for a particular ID, e.g. PCCID and its secondary cross-reference RefMetID, will not be added without
telling the code that PCCID has an xref file.
