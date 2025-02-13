from pathlib import Path
from typing import Iterator, Optional
import ijson
import logging
from pprint import pformat

from . import Converter
from utils import load_json_type_safe, ROOT_DIR
from utils.logging import LoggedClass, log_once
from utils.data_types import (
    Triple,
    TripleCategory,
    BiomarkerEntry,
    BiomarkerComponent,
    SplittableID,
)


class JSONtoNTConverter(Converter, LoggedClass):

    def __init__(self) -> None:
        LoggedClass.__init__(self)
        self.debug("Initalized JSON to NT converter")
        mapping_dir = ROOT_DIR / "mapping_data"
        self._triples_map = load_json_type_safe(
            filepath=mapping_dir / "triples_map.json", return_type="dict"
        )
        self._namespace_map = load_json_type_safe(
            filepath=mapping_dir / "namespace_map.json", return_type="dict"
        )
        self._final_triples: list[Triple] = []

    def convert(self, input_path: Path, output_path: Path) -> None:
        for entry in self._stream_json(input_path):
            self._process_entry(entry)

        self._write_triples(output_path)

    def _stream_json(self, path: Path) -> Iterator[BiomarkerEntry]:
        """Stream and parse JSON data into BiomarkerEntry objects."""
        try:
            with path.open("rb") as f:
                parser = ijson.items(f, "item")
                for entry_data in parser:
                    try:
                        yield BiomarkerEntry.from_dict(entry_data)
                    except Exception as e:
                        self.error(f"Failed to parse biomarker entry: {e}")
                        raise
        except Exception as e:
            self.exception(f"Failed to stream JSON from {path}")
            raise

    def _process_entry(self, entry: BiomarkerEntry) -> None:
        biomarker_uri = self._create_biomarker_uri(entry.biomarker_id)

        for component in entry.biomarker_component:
            self._process_component(biomarker_uri, component)

    def _process_component(
        self, biomarker_uri: str, component: BiomarkerComponent
    ) -> None:
        # Handle biomarker change triples
        change_triple = self._build_change_triple(
            subject_uri=biomarker_uri,
            biomarker=component.biomarker,
            entity_id=component.assessed_biomarker_entity_id,
            entity_type=component.assessed_entity_type,
        )
        if change_triple:
            self._final_triples.append(change_triple)

        # Handle specimen triples
        for specimen in component.specimen:
            specimen_triple = self._build_specimen_triple(
                subject_uri=biomarker_uri, specimen_id=specimen.id
            )
            if specimen_triple:
                self._final_triples.append(specimen_triple)

    def _build_change_triple(
        self,
        subject_uri: str,
        biomarker: str,
        entity_id: SplittableID,
        entity_type: str,
    ) -> Optional[Triple]:
        bio_change_key = "biomarker_change"
        biomarker_clean = biomarker.lower()
        predicate = TripleCategory.PREDICATES.value

        # Get predicate uri
        predicate_uri = None
        if "increase" in biomarker_clean:
            predicate_uri = str(
                self._triples_map[predicate][bio_change_key]["increase"]
            )
        elif "decrease" in biomarker_clean:
            predicate_uri = str(
                self._triples_map[predicate][bio_change_key]["decrease"]
            )
        elif "absence" in biomarker_clean:
            predicate_uri = str(self._triples_map[predicate][bio_change_key]["absence"])
        elif "presence" in biomarker_clean:
            predicate_uri = str(
                self._triples_map[predicate][bio_change_key]["presence"]
            )
        else:
            log_once(
                self.logger,
                f"No change predicate found for biomarker change: {biomarker}",
                logging.WARNING,
            )
            return None

        # Get object uri
        namespace, accession = entity_id.get_parts()
        namespace = namespace.lower()

        if namespace not in self._namespace_map:
            log_once(
                self.logger,
                f"Namespace {namespace} not found in namespace map",
                logging.WARNING,
            )
            return None

        object_uri = self._get_object_uri(namespace, accession, entity_type)
        if not object_uri:
            return None

        return Triple(subject=subject_uri, predicate=predicate_uri, object=object_uri)

    def _build_specimen_triple(
        self, subject_uri: str, specimen_id: SplittableID
    ) -> Optional[Triple]:
        namespace, accession = specimen_id.get_parts()
        namespace = namespace.lower()

        if namespace not in self._namespace_map:
            log_once(
                self.logger,
                f"No namespace mapping for specimen: {namespace}",
                logging.WARNING,
            )
            return None

        if self._namespace_map[namespace] == "uberon":
            predicate_uri = self._triples_map[TripleCategory.PREDICATES.value][
                "specimen_sampled_from"
            ]
            object_uri = self._triples_map[TripleCategory.SUBJECT_OBJECTS.value][
                "uberon"
            ].format(accession)
            return Triple(
                subject=subject_uri, predicate=predicate_uri, object=object_uri
            )

        return None

    def _get_object_uri(
        self, namespace: str, accession: str, entity_type: str
    ) -> Optional[str]:
        namespace_data = self._namespace_map.get(namespace)
        if not namespace_data:
            log_once(
                self.logger,
                f"Namespace {namespace} not found in namespace map",
                logging.WARNING,
            )
            return None

        namespace_value = namespace_data.get("full_name")
        if not namespace_value:
            log_once(
                self.logger,
                f"Could not determine namespace value for {namespace}",
                logging.WARNING,
            )
            return None
        namespace_value = namespace_value.lower()

        subject_objects = self._triples_map[TripleCategory.SUBJECT_OBJECTS.value]

        # Handle special case NCBI
        if namespace_value == "ncbi":
            if entity_type == "gene":
                return subject_objects["ncbi"]["gene"].format(accession)
            elif entity_type == "chemical element":
                return subject_objects["ncbi"]["compound"].format(accession)
            return None

        # Build URI key map dynamically
        uri_key_map = {}
        for ns, data in self._namespace_map.items():
            uri_key = ns.lower()
            full_name = data.get("full_name", "").lower()
            if full_name:
                uri_key_map[full_name] = uri_key

        self.debug(f"URI key map: {pformat(uri_key_map)}")
        uri_key = uri_key_map.get(namespace_value)
        if not uri_key:
            log_once(
                self.logger,
                f"No URI pattern mapping found for namespace value: {namespace_value}",
                logging.WARNING,
            )
            return None

        uri_pattern = subject_objects.get(uri_key)
        if not uri_pattern:
            log_once(
                self.logger, f"No URI pattern found for key: {uri_key}", logging.WARNING
            )
            return None

        return uri_pattern.format(accession)

    def _create_biomarker_uri(self, biomarker_id: str) -> str:
        return self._triples_map[TripleCategory.SUBJECT_OBJECTS.value][
            "biomarker_id"
        ].format(biomarker_id)

    def _write_triples(self, output_path: Path) -> None:
        with output_path.open("w") as f:
            for triple in self._final_triples:
                f.write(f"{triple}\n")
