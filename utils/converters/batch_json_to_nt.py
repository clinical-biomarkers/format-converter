from pathlib import Path
from typing import Iterator, Optional
import ijson
import logging
import re

from . import Converter, JSON_LOG_CHECKPOINT
from utils import load_json_type_safe, ROOT_DIR
from utils.logging import LoggedClass, log_once
from utils.data_types import (
    Triple,
    TripleSubjectObjects,
    TriplePredicates,
    BiomarkerEntry,
    BiomarkerComponent,
    SplittableID,
    BiomarkerRole,
    Condition,
)

NCBI_IN_BIOMARKER_RE = re.compile(r"NCBI:(\d+)", re.IGNORECASE)
BATCH_FILE_RE = re.compile(r"^batch\.\d+\.json$")


class BatchJSONtoNTConverter(Converter, LoggedClass):

    def __init__(self) -> None:
        LoggedClass.__init__(self)
        self.debug("Initialized Batch JSON to NT converter")
        mapping_dir = ROOT_DIR / "mapping_data"
        self._triples_map = load_json_type_safe(
            filepath=mapping_dir / "triples_map.json", return_type="dict"
        )
    
    def convert(self, input_dir: Path, output_dir: Path) -> None:
        batch_files = sorted(
            f for f in input_dir.iterdir() if BATCH_FILE_RE.match(f.name)
        )
        if not batch_files:
            self.warning(f"No batch files found in {input_dir}")
            return

        self.info(f"Found {len(batch_files)} batch files to process")

        all_triples: list[Triple] = []
        total_count = 0

        for batch_file in batch_files:
            self.info(f"Processing {batch_file.name}")
            count = 0

            for idx, entry in enumerate(self._stream_json(batch_file)):
                if (idx + 1) % JSON_LOG_CHECKPOINT == 0:
                    self.debug(f"[{batch_file.name}] Hit log checkpoint on entry {idx + 1}")
                all_triples.extend(self._process_entry(entry))
                count += 1

            self.info(f"[{batch_file.name}] Successfully processed {count} biomarker entries")
            total_count += count

        self.info(f"Total biomarker entries processed: {total_count}")
        self._write_triples(output_dir, all_triples)

    def _stream_json(self, path: Path) -> Iterator[BiomarkerEntry]:
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

    def _process_entry(self, entry: BiomarkerEntry) -> list[Triple]:
        biomarker_id = entry.biomarker_id
        self.debug(("-" * 25) + "\n" + f"Processing triples for entry: {biomarker_id}")

        biomarker_uri = self._create_biomarker_uri(biomarker_id)
        entry_triples: list[Triple] = []

        for idx, component in enumerate(entry.biomarker_component):
            self.debug(f"Processing component #{idx + 1}" + ("+" * 10))
            entry_triples.extend(
                self._process_component(subject_uri=biomarker_uri, component=component)
            )

        condition_triple = self._build_condition_triple(
            subject_uri=biomarker_uri,
            condition=entry.condition,
            roles=entry.best_biomarker_role,
        )
        if condition_triple:
            entry_triples.extend(condition_triple)

        role_triples = self._build_role_triples(
            subject_uri=biomarker_uri,
            roles=entry.best_biomarker_role,
        )
        if role_triples:
            entry_triples.extend(role_triples)

        self.info(f"Generated {len(entry_triples)} triples for entry {biomarker_id}")
        return entry_triples

    def _process_component(
        self, subject_uri: str, component: BiomarkerComponent
    ) -> list[Triple]:
        component_triples: list[Triple] = []

        predicate_uri = self._get_change_predicate_uri(component.biomarker)
        if predicate_uri is None:
            return component_triples

        # Triple for assessed_biomarker_entity_id
        change_triple = self._build_change_triple(
            subject_uri=subject_uri,
            biomarker=component.biomarker,
            entity_id=component.assessed_biomarker_entity_id,
            entity_type=component.assessed_entity_type,
        )
        if change_triple:
            component_triples.append(change_triple)
        """
        # Triples for any NCBI gene references embedded in the biomarker string
        for ncbi_triple in self._build_ncbi_biomarker_triples(
            subject_uri=subject_uri,
            biomarker=component.biomarker,
            predicate_uri=predicate_uri,
        ):
            component_triples.append(ncbi_triple)
        """
        # Specimen triples
        for specimen in component.specimen:
            specimen_triple = self._build_specimen_triple(
                subject_uri=subject_uri, specimen_id=specimen.id
            )
            if specimen_triple:
                component_triples.append(specimen_triple)

        return component_triples

    def _get_change_predicate_uri(self, biomarker: str) -> Optional[str]:
        bio_change_key = TriplePredicates.change_key()
        predicate = TriplePredicates.name()
        biomarker_clean = biomarker.lower()

        if "increase" in biomarker_clean:
            return str(self._triples_map[predicate][bio_change_key]["increase"])
        elif "decrease" in biomarker_clean:
            return str(self._triples_map[predicate][bio_change_key]["decrease"])
        elif "absence" in biomarker_clean:
            return str(self._triples_map[predicate][bio_change_key]["absence"])
        elif "presence" in biomarker_clean:
            return str(self._triples_map[predicate][bio_change_key]["presence"])
        else:
            log_once(
                self.logger,
                f"No change predicate found for biomarker change: {biomarker}",
                logging.WARNING,
            )
            return None

    def _build_change_triple(
        self,
        subject_uri: str,
        biomarker: str,
        entity_id: SplittableID,
        entity_type: str,
    ) -> Optional[Triple]:
        self.debug("Attempting to build change triple...")
        predicate_uri = self._get_change_predicate_uri(biomarker)
        if predicate_uri is None:
            return None
        object_uri = self._get_object_uri(id=entity_id, entity_type=entity_type)
        if not object_uri:
            return None
        return Triple(subject=subject_uri, predicate=predicate_uri, object=object_uri)

    def _build_ncbi_biomarker_triples(
        self, subject_uri: str, biomarker: str, predicate_uri: str
    ) -> list[Triple]:
        self.debug("Attempting to build NCBI biomarker triples from biomarker string...")
        triples: list[Triple] = []
        matches = NCBI_IN_BIOMARKER_RE.findall(biomarker)
        for accession in matches:
            ncbi_uri = self._triples_map[TripleSubjectObjects.name()]["ncbi"]["gene"].format(accession)
            triples.append(
                Triple(subject=subject_uri, predicate=predicate_uri, object=ncbi_uri)
            )
        return triples

    def _build_specimen_triple(
        self, subject_uri: str, specimen_id: SplittableID
    ) -> Optional[Triple]:
        self.debug("Attempting to build specimen triple...")
        object_uri = self._get_object_uri(id=specimen_id, entity_type=None)
        if object_uri is None:
            return None
        predicate_uri = self._triples_map[TriplePredicates.name()][
            TriplePredicates.specimen_key()
        ]
        return Triple(subject=subject_uri, predicate=predicate_uri, object=object_uri)

    def _build_condition_triple(
        self,
        subject_uri: str,
        condition: Optional[Condition],
        roles: list[BiomarkerRole],
    ) -> list[Triple]:
        self.debug("Attempting to build condition triples...")
        if condition is None:
            self.debug("Condition is None")
            return []
        triples: list[Triple] = []
        for role in roles:
            cleaned_role = role.role.strip().lower()
            if not TriplePredicates.condition_role_check(role.role):
                continue
            predicate_uri = self._triples_map[TriplePredicates.name()][
                TriplePredicates.condition_key()
            ][cleaned_role]
            object_uri = self._get_object_uri(condition.id, entity_type=None)
            if object_uri is None:
                continue
            triples.append(
                Triple(subject=subject_uri, predicate=predicate_uri, object=object_uri)
            )
        return triples

    def _build_role_triples(
        self, subject_uri: str, roles: list[BiomarkerRole]
    ) -> list[Triple]:
        self.debug("Attempting to build role triples...")
        triples: list[Triple] = []
        predicate_uri = self._triples_map[TriplePredicates.name()][
            TriplePredicates.role_key()
        ]
        for role in roles:
            cleaned_role = role.role.strip().lower()
            if not TripleSubjectObjects.role_check(cleaned_role):
                log_once(
                    logger=self.logger,
                    message=f"Found invalid role: {role.role}",
                    level=logging.ERROR,
                )
                continue
            object_uri = self._triples_map[TripleSubjectObjects.name()][
                TripleSubjectObjects.role_key()
            ][cleaned_role]
            triples.append(
                Triple(subject=subject_uri, predicate=predicate_uri, object=object_uri)
            )
        return triples

    def _get_object_uri(
        self, id: SplittableID, entity_type: Optional[str]
    ) -> Optional[str]:
        namespace, accession = id.get_parts()
        namespace = namespace.lower().strip()

        self.debug(f"\tAttempting to grab object URI for {namespace}:{accession}...")

        subject_objects = self._triples_map[TripleSubjectObjects.name()]

        if namespace == "ncbi":
            if entity_type == "gene":
                return subject_objects["ncbi"]["gene"].format(accession)
            elif entity_type == "chemical element":
                return subject_objects["ncbi"]["compound"].format(accession)
            log_once(
                logger=self.logger,
                message=f"Unknown NCBI entity type '{entity_type}' for accession: {accession}",
                level=logging.WARNING,
            )
            return None

        uri = subject_objects.get(namespace)
        if uri is None:
            log_once(
                logger=self.logger,
                message=f"No object URI found for namespace: {namespace}, accession: {accession}",
                level=logging.WARNING,
            )
            return None

        return uri.format(accession)

    def _create_biomarker_uri(self, biomarker_id: str) -> str:
        return self._triples_map[TripleSubjectObjects.name()][
            TripleSubjectObjects.id_key()
        ].format(biomarker_id)

    def _write_triples(self, output_path: Path, triples: list[Triple]) -> None:
        with output_path.open("w") as f:
            for triple in triples:
                f.write(f"{triple}\n")
