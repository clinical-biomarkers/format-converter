from pathlib import Path
from typing import Iterator, Optional
import csv
import logging

from utils.data_types.json_types import Citation, Reference
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

        # Write the converted JSON output
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
            # Check header spelling
            if reader.fieldnames:
                corrected_headers = self._check_header_spelling(list(reader.fieldnames))
                # If headers were corrected, create a new reader with corrected fieldnames
                f.seek(0) # Reset file pointer to beginning
                reader = csv.DictReader(f, delimiter="\t", fieldnames=corrected_headers)
                next(reader) # Skip the original header row

            for row in reader:
                yield TSVRow.from_dict(row)

    def _check_header_spelling(self, headers: list[str]) -> list[str]:
        """Check TSV headers for spelling errors agains expected field names.
        
        Parameters
        ----------
        headers: list[str]
            List of header names from the TSV file.

        Returns
        -------
        list[str]
            Corrected list of header names.
        """
        # Define expected headers based on TSVRow fields
        expected_headers = {
            'biomarker_id',
            'biomarker',
            'assessed_biomarker_entity',
            'assessed_biomarker_entity_id',
            'assessed_entity_type',
            'best_biomarker_role',
            'condition',
            'condition_id',
            'exposure_agent',
            'exposure_agent_id',
            'specimen',
            'specimen_id',
            'loinc_code',
            'evidence_source',
            'evidence',
            'tag'
        }

        # Check for exact matches first
        header_set = set(headers)
        missing_headers = expected_headers - header_set
        unexpected_headers = header_set - expected_headers

        if missing_headers:
            self.warning(f"Missing expected headers: {missing_headers}")
        corrected_headers = list(headers) # Create a copy to modify
        if unexpected_headers:
            self.warning(f"Unexpected headers found: {unexpected_headers}")
            # For unexpected headers, suggest corrections and ask user
            for i, header in enumerate(corrected_headers):
                if header in unexpected_headers:
                    suggestions = self._suggest_header_corrections(header, expected_headers)
                    if suggestions:
                        response = self._ask_user_correction(header, suggestions[0])
                        if response:
                            corrected_headers[i] = suggestions[0]
                            self.info(f"Corrected '{header}' to '{suggestions[0]}'")

        return corrected_headers

    def _ask_user_correction(self, wrong_header: str, suggested_header: str) -> bool:
        """Ask user if they want to correct a misspelled header.

        Parameters
        ----------
        wrong_header: str
            The potentially misspelled header.
        suggested_header: str
            The suggested correction.

        Returns
        -------
        bool
            True if user wants to make the correction, False otherwise.
        """
        while True:
            response = input(f"WARNING - Did you mean '{suggested_header}' instead of '{wrong_header}'? (y/n): ").strip().lower()
            if response in ['y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                print("Please enter 'y' for yes or 'n' for no.")

    def _suggest_header_corrections(self, header: str, expected_headers: set[str]) -> list[str]:
        """Suggest corrections for misspelled headers using simple edit distance.

        Parameters
        ----------
        header: str
            The potentially misspelled header.
        expected_headers: set[str]
            Set of expected header names.

        Returns
        -------
        list[str]
            List of suggested corrections, sorted by similarity.            
        """
        from difflib import get_close_matches
        
        # Use difflib for fuzzy matching
        suggestions = get_close_matches(
            header.lower(),
            [h.lower() for h in expected_headers],
            n=3,
            cutoff=0.6
        )
        
        # Map back to original case
        result = []
        for suggestion in suggestions:
            for expected in expected_headers:
                if expected.lower() == suggestion:
                    result.append(expected)
                    break

        return result

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

        # TODO : this should be handled better, but fine for now
        condition: Optional[Condition] = None
        exposure_agent: Optional[ExposureAgent] = None
        if row.condition_id:
            condition_id = SplittableID(id=row.condition_id)
            condition_resource, condition_accession = condition_id.get_parts()
            self.debug(f"Condition ID: {row.condition_id}, condition_resource: '{condition_resource}', condition_accession: '{condition_accession}'")
            condition_resource_name = self._metadata.get_full_name(condition_resource)
            condition_resource_name = (
                condition_resource_name if condition_resource_name else ""
            )
            condition_url = self._metadata.get_url_template(condition_resource)
            condition_url = (
                condition_url.format(id=condition_accession)
                if condition_url
                else ""
            )
            cond_api_calls, condition = self._metadata.fetch_metadata(  # type: ignore
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
        else:
            # TODO : not handling exposure agent metadata right now
            # TODO : this should be handled better with the condition, but fine for now
            exposure_agent_id = SplittableID(id=row.exposure_agent_id)
            expsore_agent_resource, exposure_agent_accession = (
                exposure_agent_id.get_parts()
            )
            expsore_agent_url = self._metadata.get_url_template(expsore_agent_resource)
            expsore_agent_url = (
                expsore_agent_url.format(id=exposure_agent_accession)
                if expsore_agent_url
                else ""
            )
            exposure_agent = ExposureAgent(
                id=exposure_agent_id,
                recommended_name=ConditionRecommendedName(
                    id=exposure_agent_id,
                    name=row.exposure_agent,
                    description="",
                    resource=expsore_agent_resource,
                    url=expsore_agent_url,
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
            assessed_entity_type=row.assessed_entity_type,
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
        elif row.loinc_code:
            specimen_id = SplittableID(id="")
            url = ""
            component.specimen.append(Specimen.from_row(row=row, url=url))

        return component

    def _normalize_database_name(self, database: str) -> str:
        """Normalize database name to match the official casing from namespace map.
        
        Uses display_name for user-facing source names.
        Parameters
        ----------
        database: str
            The database name from the TSV (may have incorrect casing)

        Returns
        -------
        str
            The properly cased database name, or original if not found in map
        """
        database_lower = database.strip().lower()
        display_name = self._metadata.get_display_name(database_lower)

        # If display_name is found in namespace_map, use it
        if display_name:
            return display_name

        # Otherwise, preserve original casing
        return database.strip()

    def _handle_evidence(self, entry: BiomarkerEntry, row: TSVRow) -> None:
        """Handle evidence allocation based on tags."""
        split_ev = row.evidence_source.split(":")
        database = split_ev[0]
        id = split_ev[-1]

        # Normalize the database name using namespace map
        database = self._normalize_database_name(database)
        url = self._metadata.get_url_template(resource=database.lower())
        if url is not None:
            url = url.format(id=id)
        else:
            url = ""

        # Parse base evidence details
        evidence_base = {
            "id": id,
            ## Preserves original casing of the database name
            "database": database, # foremerly database.title() which converts the first letter of each word to uppercase and the rest to lowercase
            "url": url,
            "evidence_list": [
                EvidenceItem(evidence=e.strip())
                for e in row.evidence.split(TSVRow.get_evidence_text_delimiter())
                if e.strip()
            ],
        }

        self.debug(f"evidence_base: {evidence_base}")

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

        self.debug(f"Evidence source: {row.evidence_source}")
        self.debug(f"Component tags: {component_tags}")
        self.debug(f"Top level tags: {top_level_tags}")

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

        # Default untagged evidence to component level
        if not component_tags and not top_level_tags:
            self.debug(f"No tags found for evidence source {row.evidence_source}, defaulting to component level")
            component_evidence = Evidence(**evidence_base, tags=[])
            self._add_evidence(
                entry.biomarker_component[-1].evidence_source, component_evidence
            )

    def _add_citations(self, entry: BiomarkerEntry) -> None:
        """Adds the base citation data to the entry."""

        evidence_sources = entry.collect_unique_evidence_sources()
        self.debug(f"Evidence sources collected: {evidence_sources}")
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
                # Add in the original evidence source as a reference
                reference_full_name = self._metadata.get_full_name(resource=resource)
                reference_full_name = (
                    reference_full_name
                    if reference_full_name is not None
                    else resource.title()
                )
                reference_url = self._metadata.format_url(resource=resource, id=id)
                reference_url = reference_url if reference_url is not None else ""
                citation.reference.append(
                    Reference(id=id, type=reference_full_name, url=reference_url)
                )
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
        if not row.specimen and not row.loinc_code:
            return
        # Check if this exact specimen already exists
        specimen_exists = any(
            s.name.strip().lower() == row.specimen.strip().lower()
            and s.id.id.strip() == row.specimen_id.strip()
            and s.loinc_code.strip() == row.loinc_code.strip()
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
