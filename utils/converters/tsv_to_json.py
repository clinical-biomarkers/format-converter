from pathlib import Path
from typing import Iterator, Optional
import csv
import logging

from utils.logging import LoggedClass
from utils.metadata import Metadata, ApiCallType
from utils import write_json
from . import TSV_LOG_CHECKPOINT, Converter
from utils.logging import log_once
from utils.data_types import (
    COMPONENT_SINGULAR_EVIDENCE_FIELDS,
    BiomarkerEntry,
    BiomarkerComponent,
    AssessedBiomarkerEntity,
    BiomarkerRole,
    Citation,
    Condition,
    Evidence,
    EvidenceItem,
    EvidenceTag,
    ExposureAgent,
    ConditionRecommendedName,
    Specimen,
    TSVRow,
    EvidenceState,
    ObjectFieldTags,
)


class TSVtoJSONConverter(Converter, LoggedClass):
    """Converts biomarker TSV data to JSON format."""

    def __init__(
        self, fetch_metadata: bool = True, preload_caches: bool = False
    ) -> None:
        LoggedClass.__init__(self)
        self.debug("Initializing TSV to JSON converter")
        self._fetch_metadata = fetch_metadata
        self._entries: dict[str, BiomarkerEntry] = {}
        self._preload_caches = preload_caches
        self._metadata = Metadata(preload_caches=self._preload_caches)
        self._api_calls = 0
        # Track evidence states per biomarker_id
        self._evidence_states: dict[str, dict[str, EvidenceState]] = {}

    def convert(self, input_path: Path, output_path: Path) -> None:
        """Main conversion workflow entry point."""

        for idx, row in enumerate(self._stream_tsv(input_path)):
            if (idx + 1) % TSV_LOG_CHECKPOINT == 0:
                self.debug(f"Hit log checkpoint on row {idx + 1}")
            self._process_row(row, idx)

        self.info(f"Writing {len(self._entries)} entries to {output_path}")
        self.info(f"Made {self._api_calls} API calls")
        entries = list(self._entries.values())
        self._write_json(entries, output_path)
        if self._preload_caches:
            self._metadata.save_cache_files()

    def _stream_tsv(self, path: Path) -> Iterator[TSVRow]:
        with path.open() as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                yield TSVRow.from_dict(row)

    def _process_row(self, row: TSVRow, idx: int) -> None:
        """Process a single row, updating entries and evidence."""
        log_once(
            self.logger,
            f"Processing row #{idx + 1} for biomarker ID: {row.biomarker_id}",
            logging.DEBUG,
        )

        entry = self._entries.get(row.biomarker_id)
        # If we don't find the existing entry, create it and add
        if not entry:
            entry = self._create_entry(row)
            self._entries[row.biomarker_id] = entry
        # If we do find an existing entry for that, handle the component
        else:
            self._handle_component_for_existing_entry(entry, row)

        if row.evidence_source:
            self._handle_evidence(entry, row)

        self._add_citations(entry)

    def _create_entry(self, row: TSVRow) -> BiomarkerEntry:
        """Creates a base entry for the biomarker from the TSV row."""
        roles = [
            BiomarkerRole(role=role.strip())
            for role in row.best_biomarker_role.split(TSVRow.get_role_delimiter())
            if role.strip()
        ]

        condition_data = row.condition_id.split(":")
        condition_resource = condition_data[0]
        condition_id = condition_data[-1]
        condition_resource_name = self._metadata.get_full_name(condition_resource)
        condition_resource_name = (
            condition_resource_name if condition_resource_name else ""
        )
        condition_url = self._metadata.get_url_template(condition_resource)
        condition_url = condition_url.format(condition_id) if condition_url else ""
        cond_api_calls, condition = self._metadata.fetch_metadata(
            fetch_flag=self._fetch_metadata,
            call_type=ApiCallType.CONDITION,
            resource=condition_resource,
            id=condition_id,
            resource_name=condition_resource_name,
            condition_url=condition_url,
        )
        self._api_calls += cond_api_calls
        if condition is None:
            condition = Condition(
                id=row.condition_id,
                recommended_name=ConditionRecommendedName(
                    id=row.condition_id,
                    name=row.condition,
                    description="",
                    resource=condition_resource_name,
                    url=condition_url,
                ),
            )
        else:
            if row.condition.lower() != condition.recommended_name.name.lower():
                log_once(
                    self.logger,
                    (
                        f"TSV condition name ({row.condition}) does NOT match "
                        f"resource recommended name ({condition.recommended_name.name})"
                    ),
                    logging.WARNING,
                )

        # TODO : not handling exposure agent metadata right now
        exposure_agent = ExposureAgent(
            id=row.exposure_agent_id,
            recommended_name=ConditionRecommendedName(
                id=row.exposure_agent_id,
                name=row.exposure_agent,
                description="",
                resource="",
                url="",
            ),
        )

        component = self._create_component(row)

        # TODO : missing xrefs right now
        return BiomarkerEntry(
            biomarker_id=row.biomarker_id,
            biomarker_component=[component],
            best_biomarker_role=roles,
            condition=condition,
            exposure_agent=exposure_agent,
        )

    def _create_component(self, row: TSVRow) -> BiomarkerComponent:
        """Creates the biomarker component from the TSV row. Does not
        create the component level evidence source as that is handled
        separately.
        """
        assessed_biomarker_entity_resource, assessed_biomarker_entity_id = (
            row.assessed_biomarker_entity_id.split(":")
        )
        api_calls, assessed_biomarker_entity = self._metadata.fetch_metadata(
            fetch_flag=self._fetch_metadata,
            call_type=ApiCallType.ENTITY_TYPE,
            resource=assessed_biomarker_entity_resource,
            id=assessed_biomarker_entity_id,
        )
        self._api_calls += api_calls
        if (
            assessed_biomarker_entity is not None
            and assessed_biomarker_entity.recommended_name.lower()
            != row.assessed_biomarker_entity.lower()
        ):
            log_once(
                self.logger,
                (
                    f"TSV assessed biomarker entity name ({row.assessed_biomarker_entity}) "
                    f"does NOT match resource recommended name ({assessed_biomarker_entity.recommended_name})"
                ),
                logging.WARNING,
            )

        component = BiomarkerComponent(
            biomarker=row.biomarker,
            assessed_biomarker_entity=(
                assessed_biomarker_entity
                if assessed_biomarker_entity
                else AssessedBiomarkerEntity(
                    recommended_name=row.assessed_biomarker_entity
                )
            ),
            assessed_biomarker_entity_id=row.assessed_biomarker_entity_id,
            assessed_entity_type=row.assessed_entity_type,
        )

        if row.specimen:
            component.specimen.append(self._to_specimen(row))

        return component

    def _handle_evidence(self, entry: BiomarkerEntry, row: TSVRow) -> None:
        """Handle evidence allocation based on tags."""
        split_ev = row.evidence_source.split(":")
        database = split_ev[0]
        id = split_ev[-1]

        url = self._metadata.get_url_template(resource=database)
        if url is not None:
            url = url.format(id)
        else:
            url = ""

        # Parse base evidence details
        evidence_base = {
            "id": id,
            "database": database.title(),
            "url": url,
            "evidence_list": [
                EvidenceItem(evidence=e.strip())
                for e in row.evidence.split(TSVRow.get_evidence_text_delimiter())
                if e.strip()
            ],
        }

        # Separate tags by level
        component_tags = []
        top_level_tags = []
        object_fields = ObjectFieldTags(
            specimen=row.specimen_id, loinc_code=row.loinc_code
        )

        for tag in row.tag.split(TSVRow.get_tag_delimiter()):
            tag = tag.strip()
            if not tag:
                continue

            tag_parts = tag.split(":", 1)
            tag_type = tag_parts[0]
            tag_value = tag_parts[1] if len(tag_parts) > 1 else ""

            if tag_type in COMPONENT_SINGULAR_EVIDENCE_FIELDS:
                component_tags.append(EvidenceTag(tag=tag_type))
            elif tag_type in ObjectFieldTags.get_fields():
                field_value = getattr(object_fields, tag_type)
                if field_value and (not tag_value or tag_value == field_value):
                    component_tags.append(EvidenceTag(tag=f"{tag_type}:{field_value}"))
            else:
                top_level_tags.append(EvidenceTag(tag=tag))

        # Add evidence to component level if it has component tags
        if component_tags:
            component_evidence = Evidence(**evidence_base, tags=component_tags)  # type: ignore
            self._add_evidence(
                entry.biomarker_component[-1].evidence_source, component_evidence
            )

        # Add evidence to top level if it has top level tags
        if top_level_tags:
            top_level_evidence = Evidence(**evidence_base, tags=top_level_tags)  # type: ignore
            self._add_evidence(entry.evidence_source, top_level_evidence)

    def _add_citations(self, entry: BiomarkerEntry) -> None:

        def collect_evidence_sources(entry: BiomarkerEntry) -> dict[str, set[str]]:
            sources: dict[str, set[str]] = {}

            for component in entry.biomarker_component:
                for component_evidence in component.evidence_source:
                    if component_evidence.database not in sources:
                        sources[component_evidence.database] = set()
                    sources[component_evidence.database].add(component_evidence.id)
            for top_level_evidence in entry.evidence_source:
                if top_level_evidence.database not in sources:
                    sources[top_level_evidence.database] = set()
                sources[top_level_evidence.database].add(top_level_evidence.id)

            return sources

        def merge_citations(
            existing_citation: Citation, new_citation: Citation
        ) -> None:
            existing_refs = {
                (ref.id, ref.type, ref.url) for ref in existing_citation.reference
            }
            for new_ref in new_citation.reference:
                ref_tuple = (new_ref.id, new_ref.type, new_ref.url)
                if ref_tuple not in existing_refs:
                    existing_citation.reference.append(new_ref)

            existing_evidence = {
                (ev.database, ev.id, ev.url) for ev in existing_citation.evidence
            }
            for new_ev in new_citation.evidence:
                ev_tuple = (new_ev.database, new_ev.id, new_ev.url)
                if ev_tuple not in existing_evidence:
                    existing_citation.evidence.append(new_ev)

        def add_or_merge_citation(entry: BiomarkerEntry, citation: Citation) -> None:
            for existing_citation in entry.citation:
                if (
                    existing_citation.title == citation.title
                    and existing_citation.journal == citation.journal
                    and existing_citation.authors == citation.authors
                    and existing_citation.date == citation.date
                ):
                    merge_citations(existing_citation, citation)
                    return

            # No match found - append new citation
            entry.citation.append(citation)

        evidence_sources = collect_evidence_sources(entry)
        for resource, ids in evidence_sources.items():
            for id in ids:
                api_calls, citation = self._metadata.fetch_metadata(
                    fetch_flag=self._fetch_metadata,
                    call_type=ApiCallType.CITATION,
                    resource=resource,
                    id=id,
                )
                self._api_calls += api_calls
                if citation is None:
                    continue
                add_or_merge_citation(entry, citation)

    def _add_evidence(
        self, evidence_list: list[Evidence], new_evidence: Evidence
    ) -> None:
        """Adds evidence at appropriate level, combining if duplicates exist."""
        for existing in evidence_list:
            if (
                existing.id == new_evidence.id
                and existing.database == new_evidence.database
            ):
                # Add any new evidence texts
                existing_texts = {e.evidence for e in existing.evidence_list}
                for evidence_item in new_evidence.evidence_list:
                    if evidence_item.evidence not in existing_texts:
                        existing.evidence_list.append(evidence_item)
                # Add any new tags
                existing_tags = {t.tag for t in existing.tags}
                for tag in new_evidence.tags:
                    if tag.tag not in existing_tags:
                        existing.tags.append(tag)
                return
        evidence_list.append(new_evidence)

    def _handle_component_for_existing_entry(
        self, entry: BiomarkerEntry, row: TSVRow
    ) -> None:
        """Entry point to process component handling for existing entries."""

        def find_matching_component(
            entry: BiomarkerEntry, row: TSVRow
        ) -> Optional[BiomarkerComponent]:
            """Determines if the component is already in the biomarker entry."""
            for component in entry.biomarker_component:
                if (
                    component.biomarker == row.biomarker
                    and component.assessed_biomarker_entity.recommended_name.lower()
                    == row.assessed_biomarker_entity.lower()
                    and component.assessed_biomarker_entity_id
                    == row.assessed_biomarker_entity_id
                    and component.assessed_entity_type.lower() == row.assessed_entity_type.lower()
                ):
                    return component
            return None

        matching_component = find_matching_component(entry, row)

        if matching_component:
            # Update existing component with new data
            self._update_component(matching_component, row)
        else:
            # No match found - create and add new component
            new_component = self._create_component(row)
            entry.biomarker_component.append(new_component)

    def _update_component(self, component: BiomarkerComponent, row: TSVRow) -> None:
        """Update existing component with new data. Does not merge evidence data, that
        is handled separately.
        """
        if row.specimen:
            # Check if this exact specimen already exists
            specimen_exists = any(
                s.name == row.specimen
                and s.id == row.specimen_id
                and s.loinc_code == row.loinc_code
                for s in component.specimen
            )
            # Add if it doesn't
            if not specimen_exists:
                component.specimen.append(self._to_specimen(row))

    def _to_specimen(self, row: TSVRow) -> Specimen:
        specimen_parts = row.specimen_id.split(":")
        specimen_resource = specimen_parts[0]
        specimen_id = specimen_parts[-1]
        specimen_resource_name = self._metadata.get_full_name(specimen_resource)
        specimen_resource_name = (specimen_resource_name if specimen_resource_name else "")
        specimen_resource_url = self._metadata.get_url_template(specimen_resource)
        specimen_resource_url = specimen_resource_url.format(specimen_id) if specimen_resource_url else ""
        return Specimen(
            name=row.specimen,
            id=row.specimen_id,
            name_space=specimen_resource_name,
            url=specimen_resource_url,
            loinc_code=row.loinc_code
        )

    def _write_json(self, entries: list[BiomarkerEntry], path: Path) -> None:
        json_data = [entry.to_dict() for entry in entries]
        write_json(filepath=path, data=json_data, indent=2)
