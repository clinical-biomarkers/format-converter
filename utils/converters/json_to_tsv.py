# utils/converters/json_to_tsv.py

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, TextIO
import ijson
from ..data_types import BiomarkerEntry, Evidence


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


class JSONtoTSVConverter:
    """Converts biomarker JSON data to TSV format using streaming"""

    def __init__(self):
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

    def _simplify_tag(self, tag: str) -> str:
        """Remove provenance information from tag."""
        base_tag = tag.split(":")[0]
        return (
            base_tag
            if base_tag
            in {
                "biomarker",
                "specimen",
                "condition",
                "exposure_agent",
                "best_biomarker_role",
                "assessed_biomarker_entity",
            }
            else tag
        )

    def _combine_evidence(self, evidence_texts: list[str]) -> str:
        """Combine evidence texts with correct delimiter."""
        return ";|".join(text for text in evidence_texts if text)

    def _process_evidence_source(
        self, evidence: Evidence, base_tags: set[str]
    ) -> tuple[str, str, str]:
        """Process evidence source and return formatted strings."""
        evidence_source = f"{evidence.database}:{evidence.id}"
        evidence_text = ";|".join(item.evidence for item in evidence.evidence_list)
        tags = {self._simplify_tag(tag.tag) for tag in evidence.tags}
        tags.update(base_tags)
        return evidence_source, evidence_text, ";".join(sorted(tags))

    def convert(self, input_path: Path, output_path: Path) -> None:
        """Convert JSON biomarker data to TSV format."""
        with output_path.open("w") as out_file:
            # Write headers
            out_file.write("\t".join(self._tsv_headers) + "\n")

            # Stream and convert entries
            for entry in self._stream_json(input_path):
                self._process_entry(entry, out_file)

    def _stream_json(self, path: Path) -> Iterator[BiomarkerEntry]:
        """Stream and parse JSON data into BiomarkerEntry objects."""
        with path.open("rb") as f:
            parser = ijson.items(f, "item")
            for entry_data in parser:
                yield BiomarkerEntry.from_dict(entry_data)

    def _process_entry(self, entry: BiomarkerEntry, out_file: TextIO) -> None:
        """Process a single BiomarkerEntry and write rows directly to file."""
        base_data = {
            "biomarker_id": entry.biomarker_id,
            "condition": (
                entry.condition.recommended_name.name if entry.condition else ""
            ),
            "condition_id": entry.condition.id if entry.condition else "",
            "best_biomarker_role": ";".join(
                role.role for role in entry.best_biomarker_role
            ),
        }

        # Keep track of processed evidence to avoid duplicates
        processed_evidence = {}  # key: evidence_id, value: (evidence_text, tags)

        # Process top-level evidence first
        for evidence in entry.evidence_source:
            evidence_key = f"{evidence.database}:{evidence.id}"
            evidence_text = self._combine_evidence(
                [item.evidence for item in evidence.evidence_list]
            )
            tags = {self._simplify_tag(tag.tag) for tag in evidence.tags}
            processed_evidence[evidence_key] = (evidence_text, tags)

        # Process components
        for component in entry.biomarker_component:
            component_data = base_data.copy()
            component_data.update(
                {
                    "biomarker": component.biomarker,
                    "assessed_biomarker_entity": component.assessed_biomarker_entity.recommended_name,
                    "assessed_biomarker_entity_id": component.assessed_biomarker_entity_id,
                    "assessed_entity_type": component.assessed_entity_type,
                }
            )

            if not component.specimen:
                self._write_component_evidence(
                    component_data,
                    component.evidence_source,
                    processed_evidence,
                    out_file,
                )
            else:
                for specimen in component.specimen:
                    specimen_data = component_data.copy()
                    specimen_data.update(
                        {
                            "specimen": specimen.name,
                            "specimen_id": specimen.id,
                            "loinc_code": specimen.loinc_code,
                        }
                    )
                    self._write_component_evidence(
                        specimen_data,
                        component.evidence_source,
                        processed_evidence,
                        out_file,
                    )

    def _write_component_evidence(
        self,
        base_data: dict[str, str],
        evidence_list: list[Evidence],
        processed_evidence: dict[str, tuple[str, set[str]]],
        out_file: TextIO,
    ) -> None:
        """Write evidence rows with proper combining of evidence and tags."""
        for evidence in evidence_list:
            evidence_key = f"{evidence.database}:{evidence.id}"
            current_evidence_text = self._combine_evidence(
                [item.evidence for item in evidence.evidence_list]
            )
            current_tags = {self._simplify_tag(tag.tag) for tag in evidence.tags}

            if evidence_key in processed_evidence:
                # Combine with existing evidence
                existing_text, existing_tags = processed_evidence[evidence_key]
                if current_evidence_text != existing_text:
                    evidence_text = self._combine_evidence(
                        [existing_text, current_evidence_text]
                    )
                else:
                    evidence_text = existing_text
                tags = existing_tags.union(current_tags)
                processed_evidence[evidence_key] = (evidence_text, tags)
            else:
                evidence_text = current_evidence_text
                tags = current_tags
                processed_evidence[evidence_key] = (evidence_text, tags)

            row_data = base_data.copy()
            row_data.update(
                {
                    "evidence_source": evidence_key,
                    "evidence": evidence_text,
                    "tag": ";".join(sorted(tags)),
                }
            )

            row = TSVRow(**row_data)
            values = [str(getattr(row, header)) for header in self._tsv_headers]
            out_file.write("\t".join(values) + "\n")
