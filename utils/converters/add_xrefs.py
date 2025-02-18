from collections.abc import Iterator
from pathlib import Path
import ijson
import sys
from typing import Literal
import json

from . import JSON_LOG_CHECKPOINT, Converter
from utils import load_json_type_safe, write_json, ROOT_DIR
from utils.logging import LoggedClass
from utils.data_types import (
    BiomarkerEntry,
    BiomarkerEntryWCrossReference,
    CrossReference,
    CrossReferenceMap,
)


class XrefConverter(Converter, LoggedClass):
    """Adds cross references to biomarker data."""

    def __init__(self) -> None:
        LoggedClass.__init__(self)
        self.debug("Initialized cross reference process")

        self._xref_dir = ROOT_DIR / "mapping_data" / "xrefs"

        # Dynamic cross references based on the namespace entities
        self._top_level_xrefs_mappings: dict[str, CrossReferenceMap] = {}
        self._second_level_xref_mappings: dict[str, dict[str, CrossReferenceMap]] = {}
        # Hardcoded cross references that have to be manually mapped (i.e. loinc), this
        # map must be updated accordingly if there are any new mappings that cannot be
        # inferred from the namespace map.
        self._hardcoded_xref_file_names: dict[str, str] = {"loinc": "loinc.json"}
        self._hardcoded_xref_maps: dict[str, CrossReferenceMap] = {}
        self._second_level_hardcoded_xref_maps: dict[
            str, dict[str, CrossReferenceMap]
        ] = {}

        self._load_xref_mappings()

    def convert(self, input_path: Path, output_path: Path) -> None:
        input_path_dir_flag = input_path.is_dir()
        output_path_dir_flag = output_path.is_dir()
        self.debug(
            "Checking path compatibility:\n"
            f"\tinput_path: {input_path} (is_dir={input_path_dir_flag})\n"
            f"\toutput_path: {output_path} (is_dir={output_path_dir_flag})"
        )

        if input_path_dir_flag != output_path_dir_flag:
            self.error(
                "Incompatible input and output paths, if input path is a directory, output path "
                f"must be a directory\n\tinput_path: {input_path}\n\toutput_path: {output_path}"
            )
            sys.exit(1)

        if input_path_dir_flag:
            self.debug(f"Processing directory: {input_path}")
            for file in input_path.iterdir():
                if not file.is_file() or file.suffix.lower() != ".json":
                    self.debug(f"Skipping '{file}'")
                    continue
                out_file = output_path / file.name
                self._process_file(file, out_file)
        else:
            self.debug(f"Processing single file: {input_path}")
            self._process_file(input_path, output_path)

    def _load_xref_mappings(self) -> None:

        def load_second_level_maps(
            top_level_maps: dict[str, CrossReferenceMap],
            level: Literal["namespace", "hardcode"],
        ):
            self.debug(f"Loading second level mappings for level: {level}")
            for resource, top_level_xref_map in top_level_maps.items():
                for (
                    second_level_xref_file
                ) in top_level_xref_map.secondary_cross_references:
                    target_map: dict[str, dict[str, CrossReferenceMap]]
                    match level:
                        case "namespace":
                            target_map = self._second_level_xref_mappings
                        case "hardcode":
                            target_map = self._second_level_hardcoded_xref_maps
                    target_map[resource][second_level_xref_file] = (
                        CrossReferenceMap.from_file(
                            filepath=self._xref_dir / second_level_xref_file
                        )
                    )

        self.debug(f"Loading top level xref mappings...")

        namespace_map = load_json_type_safe(
            filepath=ROOT_DIR / "mapping_data" / "namespace_map.json",
            return_type="dict",
        )
        # Load xref maps per the namespace map
        for resource, metadata in namespace_map.items():
            xref_file = metadata.get("xref")
            if xref_file is not None:
                self._top_level_xrefs_mappings[resource] = CrossReferenceMap.from_file(
                    filepath=self._xref_dir / xref_file
                )
                self._second_level_xref_mappings[resource] = {}
        load_second_level_maps(
            top_level_maps=self._top_level_xrefs_mappings, level="namespace"
        )

        # Load the hardcoded top level maps
        for resource, mapping_file_name in self._hardcoded_xref_file_names.items():
            self._top_level_xrefs_mappings[resource] = CrossReferenceMap.from_file(
                filepath=self._xref_dir / mapping_file_name
            )
            self._second_level_hardcoded_xref_maps[resource] = {}
        load_second_level_maps(
            top_level_maps=self._hardcoded_xref_maps, level="hardcode"
        )

    def _process_file(self, input_file: Path, output_file: Path) -> None:
        entries: list[BiomarkerEntryWCrossReference] = []
        total_xrefs = 0
        is_array = self._check_if_array(input_file)

        for idx, entry in enumerate(self._stream_json(input_file)):
            if (idx + 1) % JSON_LOG_CHECKPOINT == 0:
                self.info(
                    f"Hit log checkpoint on entry {idx + 1}\n"
                    f"\tFound {total_xrefs} total cross references"
                )

            crossrefs = self._get_crossrefs(entry)
            found_xrefs = len(crossrefs)
            total_xrefs += found_xrefs
            self.debug(
                f"Entry {entry.biomarker_id}:\n"
                f"\tFound {len(crossrefs)} cross references"
            )

            entry_with_xrefs = BiomarkerEntryWCrossReference.from_biomarker_entry(
                entry=entry, cross_references=crossrefs
            )
            entries.append(entry_with_xrefs)

        self.info(f"Writing {len(entries)} entries to {output_file}")
        json_data: dict | list[dict] = [entry.to_dict() for entry in entries]
        if not is_array:
            if len(json_data) != 1:
                self.error(f"Expected single entry but found {len(json_data)} entries")
                raise ValueError("Single record file contained multiple entries")
            json_data = json_data[0]

        write_json(filepath=output_file, data=json_data, indent=2)

    def _check_if_array(self, path: Path) -> bool:
        """Check if the JSON file contains an array or a single object."""
        try:
            with path.open("rb") as f:
                while True:
                    char = f.read(1).decode()
                    if not char.isspace():
                        return char == "["
        except Exception as e:
            self.error(f"Failed to cehck JSON format of {path}\n{e}")
            raise

    def _stream_json(self, path: Path) -> Iterator[BiomarkerEntry]:
        """Can handle both a full data file (a list of BiomarkerEntry's) or
        a file with a single entry.
        """
        try:
            with path.open("rb") as f:
                # First try parsing as array of records
                parser = ijson.items(f, "item")
                first = next(parser, None)
                if first is not None:
                    yield BiomarkerEntry.from_dict(first)
                    yield from (BiomarkerEntry.from_dict(item) for item in parser)
                else:
                    # No items found, try as single object
                    f.seek(0)
                    record = json.load(f)
                    if record:
                        yield BiomarkerEntry.from_dict(record)
        except Exception as e:
            self.error(f"Failed to stream JSON from {path}\n{e}")
            raise

    def _get_crossrefs(self, entry: BiomarkerEntry) -> list[CrossReference]:
        crossrefs: list[CrossReference] = []
        seen_crossrefs: set[tuple[str, str, str]] = set()

        self.debug(f"Getting cross references for biomarker {entry.biomarker_id}")

        for component_idx, component in enumerate(entry.biomarker_component):
            # Get entity refs
            entity_ns, entity_id = component.assessed_biomarker_entity_id.get_parts()
            self.debug(
                f"Processing component {component_idx}:\n"
                f"\tEntity namespace: {entity_ns}\n"
                f"\tEntity ID: {entity_id}"
            )
            self._add_namespace_xrefs(
                namespace=entity_ns.lower(),
                id=entity_id,
                crossrefs=crossrefs,
                seen=seen_crossrefs,
            )

            for specimen in component.specimen:
                if specimen.loinc_code:
                    self.debug(
                        f"Processing LOINC code {specimen.loinc_code} "
                        f"for specimen {specimen.name}"
                    )

                    loinc_map = self._hardcoded_xref_maps.get("loinc")
                    if loinc_map is not None:
                        xref = CrossReference(
                            id=specimen.loinc_code,
                            url=loinc_map.url.format(id=specimen.loinc_code),
                            database=loinc_map.database,
                            categories=loinc_map.categories,
                        )
                        xref_tuple = (xref.database, xref.id, xref.url)
                        if xref_tuple not in seen_crossrefs:
                            seen_crossrefs.add(xref_tuple)
                            crossrefs.append(xref)
                            self.debug(
                                f"Added LOINC cross reference: {xref.database}:{xref.id}"
                            )

                    # Check for secondary references
                    self._add_secondary_xrefs(
                        resource="loinc",
                        id=specimen.loinc_code,
                        crossrefs=crossrefs,
                        seen=seen_crossrefs,
                        xref_type="hardcode",
                    )

        self.debug(
            f"Found {len(crossrefs)} total cross references "
            f"for biomarker {entry.biomarker_id}"
        )
        return crossrefs

    def _add_namespace_xrefs(
        self,
        namespace: str,
        id: str,
        crossrefs: list[CrossReference],
        seen: set[tuple[str, str, str]],
    ) -> None:
        xref_map = self._top_level_xrefs_mappings.get(namespace)
        if xref_map is None:
            return

        # Add primary xref
        xref = CrossReference(
            id=id,
            url=xref_map.url.format(id=id),
            database=xref_map.database,
            categories=xref_map.categories,
        )
        xref_tuple = (xref.database, xref.id, xref.url)
        if xref_tuple not in seen:
            seen.add(xref_tuple)
            crossrefs.append(xref)

        # Add any secondary xrefs
        self._add_secondary_xrefs(
            resource=namespace,
            id=id,
            crossrefs=crossrefs,
            seen=seen,
            xref_type="namespace",
        )

    def _add_secondary_xrefs(
        self,
        resource: str,
        id: str,
        crossrefs: list[CrossReference],
        seen: set[tuple[str, str, str]],
        xref_type: Literal["namespace", "hardcode"],
    ) -> None:
        secondary_maps = (
            self._second_level_xref_mappings.get(resource, {})
            if xref_type == "namespace"
            else self._second_level_hardcoded_xref_maps.get(resource, {})
        )

        for mapping_name, xref_map in secondary_maps.items():
            if xref_map.id_map and id not in xref_map.id_map:
                source_type = (
                    "hardcoded resource" if xref_type == "hardcode" else "resource"
                )
                self.warning(
                    f"ID '{id}' from {source_type} '{resource}' not found in {mapping_name} ID map"
                )
                continue

            # Transform ID if mapping exists
            mapped_id = xref_map.id_map.get(id, id)

            xref = CrossReference(
                id=mapped_id,
                url=xref_map.url.format(mapped_id),
                database=xref_map.database,
                categories=xref_map.categories,
            )
            xref_tuple = (xref.database, xref.id, xref.url)
            if xref_tuple not in seen:
                seen.add(xref_tuple)
                crossrefs.append(xref)
