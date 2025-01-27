from pathlib import Path
from typing import Iterator
import json
import csv
from . import Converter
from ..data_types import (
    BiomarkerEntry,
    BiomarkerComponent,
    AssessedBiomarkerEntity,
    BiomarkerRole,
    Condition,
    Evidence,
    EvidenceItem,
    EvidenceTag,
    ExposureAgent,
    RecommendedName,
    Specimen,
    TSVRow,
)


class TSVtoJSONConverter(Converter):
    """Converts biomarker TSV data to JSON format."""

    def __init__(self, fetch_metadata: bool = True) -> None:
        self._fetch_metadata = fetch_metadata
        self._entries: dict[str, BiomarkerEntry] = {}

    def convert(self, input_path: Path, output_path: Path) -> None:
        for row in self._stream_tsv(input_path):
            self._process_row(row)

        entries = list(self._entries.values())
        self._write_json(entries, output_path)

    def _stream_tsv(self, path: Path) -> Iterator[TSVRow]:
        with path.open() as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                yield TSVRow.from_dict(row)

    def _process_row(self, row: TSVRow) -> None:
        entry = self._entries.get(row.biomarker_id)
        if not entry:
            entry = self._create_entry(row)
            self._entries[row.biomarker_id] = entry

        self._process_component(entry, row)

    def _create_entry(self, row: TSVRow) -> BiomarkerEntry:
        roles = [
            BiomarkerRole(role=role.strip())
            for role in row.best_biomarker_role.split(TSVRow.get_role_delimiter())
            if role.strip()
        ]

        condition = Condition(
            id=row.condition_id,
            recommended_name=RecommendedName(
                id=row.condition_id,
                name=row.condition,
                description=None,
                resource="",
                url="",
            ),
        )

        exposure_agent = ExposureAgent(
            id=row.exposure_agent_id,
            recommended_name=RecommendedName(
                id=row.exposure_agent_id,
                name=row.exposure_agent,
                description=None,
                resource="",
                url="",
            ),
        )

        component = self._create_component(row)

        # TODO : missing top level evidence, citation, and xrefs right now
        return BiomarkerEntry(
            biomarker_id=row.biomarker_id,
            biomarker_component=[component],
            best_biomarker_role=roles,
            condition=condition,
            exposure_agent=exposure_agent,
        )

    def _create_component(self, row: TSVRow) -> BiomarkerComponent:
        component = BiomarkerComponent(
            biomarker=row.biomarker,
            assessed_biomarker_entity=AssessedBiomarkerEntity(
                recommended_name=row.assessed_biomarker_entity
            ),
            assessed_biomarker_entity_id=row.assessed_biomarker_entity_id,
            assessed_entity_type=row.assessed_entity_type,
        )

        if row.specimen:
            component.specimen.append(
                Specimen(
                    name=row.specimen,
                    id=row.specimen_id,
                    name_space="",
                    url="",
                    loinc_code=row.loinc_code,
                )
            )

        if row.evidence_source:
            evidence = Evidence(
                id=row.evidence_source.split(":")[-1],
                database=row.evidence_source.split(":")[0],
                url="",
                evidence_list=[
                    EvidenceItem(evidence=evidence.strip())
                    for evidence in row.evidence.split(
                        TSVRow.get_evidence_text_delimiter()
                    )
                    if evidence.strip()
                ],
                tags=[
                    EvidenceTag(tag=tag.strip())
                    for tag in row.tag.split(TSVRow.get_tag_delimiter())
                    if tag.strip()
                ],
            )
            component.evidence_source.append(evidence)

        return component

    def _process_component(self, entry: BiomarkerEntry, row: TSVRow) -> None:
        # Check if we already have this component
        matching_component = None
        for component in entry.biomarker_component:
            if (
                component.biomarker == row.biomarker
                and component.assessed_biomarker_entity.recommended_name
                == row.assessed_biomarker_entity
                and component.assessed_biomarker_entity_id
                == row.assessed_biomarker_entity_id
                and component.assessed_entity_type == row.assessed_entity_type
            ):
                matching_component = component
                break

        if matching_component is None:
            # New component - create and add it
            new_component = self._create_component(row)
            entry.biomarker_component.append(new_component)
        else:
            # Update existing component
            if row.specimen:
                specimen_exists = any(
                    s.name == row.specimen
                    and s.id == row.specimen_id
                    and s.loinc_code == row.loinc_code
                    for s in matching_component.specimen
                )
                if not specimen_exists:
                    matching_component.specimen.append(
                        Specimen(
                            name=row.specimen,
                            id=row.specimen_id,
                            name_space="",
                            url="",
                            loinc_code=row.loinc_code,
                        )
                    )

            # Add evidence if present and not duplicate
            if row.evidence_source:
                evidence = Evidence(
                    id=row.evidence_source.split(":")[-1],
                    database=row.evidence_source.split(":")[0],
                    url="",
                    evidence_list=[
                        EvidenceItem(evidence=e.strip())
                        for e in row.evidence.split(";|")
                        if e.strip()
                    ],
                    tags=[
                        EvidenceTag(tag=t.strip())
                        for t in row.tag.split(";")
                        if t.strip()
                    ],
                )

                # Check if identical evidence exists
                if not any(
                    e.id == evidence.id
                    and e.database == evidence.database
                    and e.evidence_list == evidence.evidence_list
                    for e in matching_component.evidence_source
                ):
                    matching_component.evidence_source.append(evidence)

    def _write_json(self, entries: list[BiomarkerEntry], path: Path) -> None:
        json_data = [entry.to_dict() for entry in entries]
        with path.open("w") as f:
            json.dump(json_data, f, indent=2)
