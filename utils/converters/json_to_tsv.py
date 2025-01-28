from pathlib import Path
from typing import Iterator, TextIO
import ijson

from . import JSON_LOG_CHECKPOINT, Converter
from utils.logging import LoggedClass
from utils.data_types import (
    BiomarkerEntry,
    Evidence,
    EvidenceTag,
    TSVRow,
    EvidenceState,
    ObjectFieldTags,
)


class JSONtoTSVConverter(Converter, LoggedClass):
    """Converts biomarker JSON data to TSV format using streaming"""

    def __init__(self) -> None:
        LoggedClass.__init__(self)
        self.debug("Initalized JSON to TSV converter")
        self._tsv_headers = TSVRow.get_headers()
        self._evidence_states: dict[str, EvidenceState] = {}

    def convert(self, input_path: Path, output_path: Path) -> None:
        """Convert JSON biomarker data to TSV format."""

        with output_path.open("w") as out_file:
            self.debug("Writing TSV headers")
            out_file.write("\t".join(self._tsv_headers) + "\n")

            count = 0
            for idx, entry in enumerate(self._stream_json(input_path)):
                if (idx + 1) % JSON_LOG_CHECKPOINT == 0:
                    self.debug(f"Hit log checkpoint on entry {idx + 1}")
                self._process_entry(entry, out_file)
                count += 1

            self.info(f"Successfully processed {count + 1} total biomarker entries")

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

    def _initialize_evidence_states(self, entry: BiomarkerEntry) -> None:
        """Initalizes evidence states from top-level evidence sources."""
        self._evidence_states.clear()

        evidence_count = len(entry.evidence_source)
        if evidence_count:
            self.debug(
                f"Initalizing {evidence_count} top-level evidence states for {entry.biomarker_id}"
            )

        for evidence in entry.evidence_source:
            key = f"{evidence.database}:{evidence.id}"
            state = EvidenceState(evidence_texts=set(), tags=set())
            state.combine_evidence(evidence.evidence_list)
            state.combine_tags(evidence.tags, ObjectFieldTags())
            self._evidence_states[key] = state

    def _get_base_row_data(self, entry: BiomarkerEntry) -> dict:
        """Get base row data common to all component rows."""
        entry_dict = entry.to_dict()
        return {
            "biomarker_id": entry.biomarker_id,
            "condition": (
                entry.condition.recommended_name.name
                if "condition" in entry_dict
                else ""
            ),
            "condition_id": entry.condition.id if "condition" in entry_dict else "",
            "best_biomarker_role": TSVRow.get_role_delimiter().join(
                role.role for role in entry.best_biomarker_role
            ),
            "exposure_agent": (
                entry.exposure_agent.recommended_name.name
                if "exposure_agent" in entry_dict
                else ""
            ),
            "exposure_agent_id": (
                entry.exposure_agent.id if "exposure_agent" in entry_dict else ""
            ),
        }

    def _process_entry(self, entry: BiomarkerEntry, out_file: TextIO) -> None:
        """Process a single BiomarkerEntry and write rows to file."""
        self.debug(f"Processing biomarker entry {entry.biomarker_id}")

        self._initialize_evidence_states(entry)
        base_row_data = self._get_base_row_data(entry)

        for comp_idx, component in enumerate(entry.biomarker_component):
            self.debug(
                f"Processing component {comp_idx + 1} for biomarker {entry.biomarker_id}"
            )

            curr_row_data = base_row_data.copy()
            curr_row_data.update(
                {
                    "biomarker": component.biomarker,
                    "assessed_biomarker_entity": component.assessed_biomarker_entity.recommended_name,
                    "assessed_biomarker_entity_id": component.assessed_biomarker_entity_id,
                    "assessed_entity_type": component.assessed_entity_type,
                }
            )

            if not component.specimen:
                self.debug(f"No specimen data for component {comp_idx + 1}")
                self._write_rows(
                    row_data=curr_row_data,
                    component_evidence_sources=component.evidence_source,
                    object_fields=ObjectFieldTags(),
                    out_file=out_file,
                )
            else:
                self.debug(
                    f"Processing {len(component.specimen)} specimens for component {comp_idx + 1}"
                )
                for specimen_idx, specimen in enumerate(component.specimen):
                    self.debug(
                        f"Processing specimen {specimen_idx + 1} ({specimen.name})"
                    )
                    specimen_row_data = curr_row_data.copy()
                    specimen_row_data.update(
                        {
                            "specimen": specimen.name,
                            "specimen_id": specimen.id,
                            "loinc_code": specimen.loinc_code,
                        }
                    )
                    self._write_rows(
                        row_data=specimen_row_data,
                        component_evidence_sources=component.evidence_source,
                        object_fields=ObjectFieldTags(
                            specimen=specimen.id, loinc_code=specimen.loinc_code
                        ),
                        out_file=out_file,
                    )

    def _write_rows(
        self,
        row_data: dict[str, str],
        component_evidence_sources: list[Evidence],
        object_fields: ObjectFieldTags,
        out_file: TextIO,
    ) -> None:
        """Write component rows with evidence combination."""
        processed_top_level = set()

        for comp_evidence in component_evidence_sources:
            key = f"{comp_evidence.database}:{comp_evidence.id}"
            self.debug(f"Processing component evidence {key}")

            state = EvidenceState(evidence_texts=set(), tags=set())

            # If there's matching top-level evidence, combine it
            if key in self._evidence_states:
                self.debug(f"Found matching top-level evidence for {key}")
                top_level_state = self._evidence_states[key]
                state.evidence_texts.update(top_level_state.evidence_texts)
                state.tags.update(top_level_state.tags)
                processed_top_level.add(key)

            # Add component evidence
            state.combine_evidence(comp_evidence.evidence_list)
            state.combine_tags(comp_evidence.tags, object_fields)

            final_row_data = row_data.copy()
            final_row_data.update(
                {
                    "evidence_source": key,
                    "evidence": state.evidence_text,
                    "tag": state.tag_string,
                }
            )

            row = TSVRow(**final_row_data)
            values = [str(getattr(row, header)) for header in self._tsv_headers]
            out_file.write("\t".join(values) + "\n")

        unprocessed = set(self._evidence_states.keys()) - processed_top_level
        if unprocessed:
            self.debug(
                f"Processing {len(unprocessed)} unprocessed top-level evidence entries"
            )

        for key in unprocessed:
            self.debug(f"Processing top-level evidence {key}")
            top_state = self._evidence_states[key]

            # Create new state just for this top-level evidence
            state = EvidenceState(evidence_texts=set(), tags=set())
            state.evidence_texts.update(top_state.evidence_texts)
            state.combine_tags(
                [EvidenceTag(tag=tag) for tag in top_state.tags], object_fields
            )

            final_row_data = row_data.copy()
            final_row_data.update(
                {
                    "evidence_source": key,
                    "evidence": state.evidence_text,
                    "tag": state.tag_string,
                }
            )

            row = TSVRow(**final_row_data)
            values = [str(getattr(row, header)) for header in self._tsv_headers]
            out_file.write("\t".join(values) + "\n")
