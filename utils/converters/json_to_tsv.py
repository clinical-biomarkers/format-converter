# TODO : This could be cleaned up even further but its a significant improvement over the legacy code for now

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, TextIO
import ijson
from ..data_types import BiomarkerEntry, Evidence, EvidenceItem, EvidenceTag


@dataclass
class TSVRow:
    """Represents a single row in the TSV format"""

    biomarker_id: str
    biomarker: str
    assessed_biomarker_entity: str
    assessed_biomarker_entity_id: str
    assessed_entity_type: str
    condition: str = ""
    condition_id: str = ""
    exposure_agent: str = ""
    exposure_agent_id: str = ""
    best_biomarker_role: str = ""
    specimen: str = ""
    specimen_id: str = ""
    loinc_code: str = ""
    evidence_source: str = ""
    evidence: str = ""
    tag: str = ""


@dataclass
class ObjectFieldTags:
    """Represents the fields that are referenced with a value in the JSON tags."""

    specimen: str = ""
    loinc_code: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }


@dataclass
class EvidenceState:
    """Tracks evidence state for a specific evidence source."""

    evidence_texts: set[str]  # Stores unique evidence text entries
    tags: set[str]  # Stores unique tags

    def combine_evidence(self, new_evidence: list[EvidenceItem]) -> None:
        for item in new_evidence:
            self.evidence_texts.add(item.evidence)

    def combine_tags(
        self, new_tags: list[EvidenceTag], object_fields: ObjectFieldTags
    ) -> None:
        object_fields_dict = object_fields.to_dict()
        for tag in new_tags:
            tag_parts = tag.tag.split(":", 1)
            tag_type = tag_parts[0]
            tag_value = tag_parts[1] if len(tag_parts) > 1 else ""

            # Handle object field specific tags
            if tag_type in object_fields_dict:
                if tag_value:
                    # Only add tag if it matches specified value
                    if tag_value == object_fields_dict[tag_type]:
                        self.tags.add(tag_type)
                continue

            # Handle component singular fields
            if tag_type in JSONtoTSVConverter.COMP_SINGULAR_EVIDENCE_FIELDS:
                self.tags.add(tag_type)
                continue

            # Hanle top level fields
            if tag_type in JSONtoTSVConverter.TOP_LEVEL_EVIDENCE_FIELDS:
                self.tags.add(tag_type)

    @property
    def evidence_text(self) -> str:
        return ";|".join(sorted(self.evidence_texts))

    @property
    def tag_string(self) -> str:
        return ";".join(sorted(self.tags))


class JSONtoTSVConverter:
    """Converts biomarker JSON data to TSV format using streaming"""

    # Biomarker component fields that are singular, for tags
    COMP_SINGULAR_EVIDENCE_FIELDS = {
        "biomarker",
        "assessed_biomarker_entity",
        "assessed_biomarker_entity_id",
        "assessed_entity_type",
    }

    # Possible top level evidence tags
    TOP_LEVEL_EVIDENCE_FIELDS = {"condition", "exposure_agent", "best_biomarker_role"}

    def __init__(self) -> None:
        self._tsv_headers = [
            "biomarker_id",
            "biomarker",
            "assessed_biomarker_entity",
            "assessed_biomarker_entity_id",
            "assessed_entity_type",
            "condition",
            "condition_id",
            "exposure_agent",
            "exposure_agent_id",
            "best_biomarker_role",
            "specimen",
            "specimen_id",
            "loinc_code",
            "evidence_source",
            "evidence",
            "tag",
        ]
        self._evidence_states: dict[str, EvidenceState] = {}

    def convert(self, input_path: Path, output_path: Path) -> None:
        """Convert JSON biomarker data to TSV format."""
        with output_path.open("w") as out_file:
            out_file.write("\t".join(self._tsv_headers) + "\n")
            for entry in self._stream_json(input_path):
                self._process_entry(entry, out_file)

    def _stream_json(self, path: Path) -> Iterator[BiomarkerEntry]:
        """Stream and parse JSON data into BiomarkerEntry objects."""
        with path.open("rb") as f:
            parser = ijson.items(f, "item")
            for entry_data in parser:
                yield BiomarkerEntry.from_dict(entry_data)

    def _initialize_evidence_states(self, entry: BiomarkerEntry) -> None:
        """Initalizes evidence states from top-level evidence sources."""
        self._evidence_states.clear()
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
            "best_biomarker_role": ";".join(
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
        self._initialize_evidence_states(entry)
        base_row_data = self._get_base_row_data(entry)

        for component in entry.biomarker_component:
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
                self._write_rows(
                    row_data=curr_row_data,
                    component_evidence_sources=component.evidence_source,
                    object_fields=ObjectFieldTags(),
                    out_file=out_file,
                )
            else:
                for specimen in component.specimen:
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

            state = EvidenceState(evidence_texts=set(), tags=set())

            # If there's matching top-level evidence, combine it
            if key in self._evidence_states:
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

        for key, top_state in self._evidence_states.items():
            if key not in processed_top_level:
                # Create new state just for this top-level evidence
                state = EvidenceState(evidence_texts=set(), tags=set())
                state.evidence_texts.update(top_state.evidence_texts)

                # Need to process tags again with current object fields
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
