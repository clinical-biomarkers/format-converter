from pathlib import Path
from typing import Iterator, Optional
import ijson
import logging

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


class JSONtoNTConverter(Converter, LoggedClass):

    def __init__(self) -> None:
        LoggedClass.__init__(self)
        self.debug("Initalized JSON to NT converter")
        mapping_dir = ROOT_DIR / "mapping_data"
        self._triples_map = load_json_type_safe(
            filepath=mapping_dir / "triples_map.json", return_type="dict"
        )
        self._final_triples: list[Triple] = []

    def convert(self, input_path: Path, output_path: Path) -> None:
        count = 0
        for idx, entry in enumerate(self._stream_json(input_path)):
            if (idx + 1) % JSON_LOG_CHECKPOINT == 0:
                self.debug(f"Hit log checkpoint on entry {idx + 1}")
            self._process_entry(entry)
            count += 1

        self.info(f"Successfully processed {count} biomarker entries")
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
        """Processes all the possible triples for a single biomarker entry."""
        biomarker_id = entry.biomarker_id
        self.debug(("-" * 25) + "\n" + f"Processing triples for entry: {biomarker_id}")

        biomarker_uri = self._create_biomarker_uri(biomarker_id)
        entry_triples: list[Triple] = []

        # Build component triples
        for idx, component in enumerate(entry.biomarker_component):
            self.debug(f"Processing component #{idx + 1}" + ("+" * 10))
            entry_triples.extend(
                self._process_component(subject_uri=biomarker_uri, component=component)
            )

        # Build top level triples
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
        self._final_triples.extend(entry_triples)

    def _process_component(
        self, subject_uri: str, component: BiomarkerComponent
    ) -> list[Triple]:
        """Processes all the possible triples for a single biomarker component."""
        component_triples: list[Triple] = []

        # Handle biomarker change triples
        change_triple = self._build_change_triple(
            subject_uri=subject_uri,
            biomarker=component.biomarker,
            entity_id=component.assessed_biomarker_entity_id,
            entity_type=component.assessed_entity_type,
        )
        if change_triple:
            component_triples.append(change_triple)

        # Handle specimen triples
        for specimen in component.specimen:
            specimen_triple = self._build_specimen_triple(
                subject_uri=subject_uri, specimen_id=specimen.id
            )
            if specimen_triple:
                component_triples.append(specimen_triple)

        return component_triples

    def _build_change_triple(
        self,
        subject_uri: str,
        biomarker: str,
        entity_id: SplittableID,
        entity_type: str,
    ) -> Optional[Triple]:
        """Creates the biomarker change triple for a biomarker component.

        Parameters
        ----------
        subject_uri: str
            The subject URI (the biomarker URI).
        biomarker: str
            The biomarker field from the component.
        entity_id: SplittableID
            The assessed biomarker entity ID.
        entity_type: str
            The assessed biomarker entity type.
        """
        self.debug("Attempting to build change triples...")

        bio_change_key = TriplePredicates.change_key()
        predicate = TriplePredicates.name()
        biomarker_clean = biomarker.lower()

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
        object_uri = self._get_object_uri(id=entity_id, entity_type=entity_type)
        if not object_uri:
            return None

        return Triple(subject=subject_uri, predicate=predicate_uri, object=object_uri)

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

        # Handle special case NCBI
        if namespace == "ncbi":
            if entity_type == "gene":
                return subject_objects["ncbi"]["gene"].format(accession)
            elif entity_type == "chemical element":
                return subject_objects["ncbi"]["compound"].format(accession)
            return None

        # Build URI key map dynamically
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
        """Returns the formatted biomarker subject URI."""
        return self._triples_map[TripleSubjectObjects.name()][
            TripleSubjectObjects.id_key()
        ].format(biomarker_id)

    def _write_triples(self, output_path: Path) -> None:
        with output_path.open("w") as f:
            for triple in self._final_triples:
                f.write(f"{triple}\n")
