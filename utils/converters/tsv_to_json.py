from pathlib import Path
from typing import Iterator, Optional
import csv
import logging

from utils.data_types.json_types import Citation
from utils.logging import LoggedClass
from utils.metadata import Metadata, ApiCallType
from utils import write_json
from . import TSV_LOG_CHECKPOINT, Converter
from utils.logging import log_once
from utils.data_types import (
    COMPONENT_SINGULAR_EVIDENCE_FIELDS,
    SplittableID,
    BiomarkerEntry,
    BiomarkerComponent,
    AssessedBiomarkerEntity,
    BiomarkerRole,
    Condition,
    Evidence,
    EvidenceItem,
    EvidenceTag,
    ExposureAgent,
    ConditionRecommendedName,
    Specimen,
    TSVRow,
    ObjectFieldTags,
)


class TSVtoJSONConverter(Converter, LoggedClass):
    """Converts biomarker TSV data to the full JSON data model format.

    Parameters
    ----------
    fetch_metadata: bool, optional
        Whether to try and fetch metadata from API calls if the data is not
        found locally. Defaults to True.
    preload_caches: bool, optional
        Whether to preload the locally cached metadata. This will not only
        prefetch the cache data and load it into memory, but will keep the
        data in memory until the conversion is finished. Much faster but
        also higher memory usage. Defaults to False.
    """

    def __init__(
        self, fetch_metadata: bool = True, preload_caches: bool = False
    ) -> None:
        LoggedClass.__init__(self)
        self.debug("Initializing TSV to JSON converter")
        self._fetch_metadata = fetch_metadata
        self._entries: dict[str, BiomarkerEntry] = {}  # Tracks process entries by ID
        self._preload_caches = preload_caches
        self._metadata = Metadata(preload_caches=self._preload_caches)
        self._api_calls = 0  # Tracks total API calls made

    def convert(self, input_path: Path, output_path: Path) -> None:
        """Main conversion workflow entry point.

        Parameters
        ----------
        input_path: Path
            Path to input TSV file.
        output_path: Path
            Path to write the JSON output.
        """
        # Process each row, building entries incrementally
        for idx, row in enumerate(self._stream_tsv(input_path)):
            if (idx + 1) % TSV_LOG_CHECKPOINT == 0:
                self.debug(f"Hit log checkpoint on row {idx + 1}")
            self._process_row(row, idx)

        self.info(f"Writing {len(self._entries)} entries to {output_path}")
        self.info(f"Made {self._api_calls} API calls")

        # Wrie the convertted JSON output
        entries = list(self._entries.values())
        self._write_json(entries, output_path)
        if self._preload_caches:
            self._metadata.save_cache_files()

    def _stream_tsv(self, path: Path) -> Iterator[TSVRow]:
        """Stream and parse TSV file rows.

        Parameters
        ----------
        path: Path
            Path to the TSV file to stream.

        Yields
        -------
        Iterator[TSVRow]
            An iterator of TSV rows.
        """
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

        condition_id = SplittableID(id=row.condition_id)
        condition_resource, condition_accession = condition_id.get_parts()
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
            id=condition_accession,
            resource_name=condition_resource_name,
            condition_url=condition_url,
        )
        self._api_calls += cond_api_calls
        if condition is None or not Condition.type_guard(condition):
            condition = Condition(
                id=condition_id,
                recommended_name=ConditionRecommendedName(
                    id=condition_id,
                    name=row.condition,
                    description="",
                    resource=condition_resource_name,
                    url=condition_url,
                ),
            )
        else:
            if not condition.recommended_name.check_match(
                tsv_val=row.condition, strict=False, logger=self.logger
            ):
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
                id=condition_id,
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
        """Creates the base biomarker component from the TSV row. Does not
        create the component level evidence.
        """
        assessed_biomarker_entity_id = SplittableID(id=row.assessed_biomarker_entity_id)
        assessed_biomarker_entity_resource, assessed_biomarker_entity_accession = (
            assessed_biomarker_entity_id.get_parts()
        )
        api_calls, assessed_biomarker_entity = self._metadata.fetch_metadata(
            fetch_flag=self._fetch_metadata,
            call_type=ApiCallType.ENTITY_TYPE,
            resource=assessed_biomarker_entity_resource,
            id=assessed_biomarker_entity_accession,
        )
        self._api_calls += api_calls
        if assessed_biomarker_entity is None or not AssessedBiomarkerEntity.type_guard(
            assessed_biomarker_entity
        ):
            assessed_biomarker_entity = AssessedBiomarkerEntity(
                recommended_name=row.assessed_biomarker_entity
            )
        else:
            if not assessed_biomarker_entity.check_match(
                tsv_val=row.assessed_biomarker_entity, strict=False, logger=self.logger
            ):
                log_once(
                    self.logger,
                    (
                        f"TSV assessed biomarker entity name ({row.assessed_biomarker_entity}) does NOT match "
                        f"resource recommended name ({assessed_biomarker_entity.recommended_name})"
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
            assessed_biomarker_entity_id=assessed_biomarker_entity_id,
            assessed_entity_type=row.assessed_entity_type,
        )

        if row.specimen:
            specimen_id = SplittableID(id=row.specimen_id)
            specimen_resource, specimen_accession = specimen_id.get_parts()
            url = self._metadata.format_url(
                resource=specimen_resource, id=specimen_accession
            )
            url = url if url else ""
            component.specimen.append(Specimen.from_row(row=row, url=url))

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
        """Adds the base citation data to the entry."""

        evidence_sources = entry.collect_unique_evidence_sources()
        for resource, ids in evidence_sources.items():
            for id in ids:
                api_calls, citation = self._metadata.fetch_metadata(
                    fetch_flag=self._fetch_metadata,
                    call_type=ApiCallType.CITATION,
                    resource=resource,
                    id=id,
                )
                self._api_calls += api_calls
                if citation is None or not Citation.type_guard(citation):
                    continue
                entry.add_or_merge_citation(citation)

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
                if row.core_equal_component(component):
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
        if not row.specimen:
            return
        # Check if this exact specimen already exists
        specimen_exists = any(
            s.name == row.specimen
            and s.id == row.specimen_id
            and s.loinc_code == row.loinc_code
            for s in component.specimen
        )
        # Add if it doesn't
        if not specimen_exists:
            specimen_id = SplittableID(id=row.specimen_id)
            resource, id = specimen_id.get_parts()
            url = self._metadata.format_url(resource=resource, id=id)
            url = url if url else ""
            component.specimen.append(Specimen.from_row(row=row, url=url))

    def _write_json(self, entries: list[BiomarkerEntry], path: Path) -> None:
        json_data = [entry.to_dict() for entry in entries]
        write_json(filepath=path, data=json_data, indent=2)
