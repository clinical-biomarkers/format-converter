from dataclasses import dataclass, field
from pprint import pformat
from pathlib import Path
from typing import Any, Optional, Union, TYPE_CHECKING, TypeGuard
from abc import ABC, abstractmethod
from logging import Logger

from utils import load_json_type_safe

if TYPE_CHECKING:
    from . import TSVRow


class DataModelObject(ABC):
    """Base abstract class for a JSON data model object."""

    @abstractmethod
    def to_dict(self) -> Any:
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict[str, Any]) -> Any:
        pass


class CacheableDataModelObject(ABC):
    """Abstract class for a data model object which
    can be saved to a local cache.
    """

    @abstractmethod
    def to_cache_dict(self) -> Any:
        pass

    @classmethod
    @abstractmethod
    def from_cache_dict(cls, data: Any) -> Any:
        pass

    @staticmethod
    @abstractmethod
    def type_guard(obj: "CacheableDataModelObject") -> TypeGuard[Any]:
        pass


class RecommendedNameObject(ABC):

    @abstractmethod
    def check_match(
        self, tsv_val: str, strict: bool = False, logger: Optional[Logger] = None
    ) -> bool:
        pass


class SplittableID(DataModelObject):

    def __init__(self, id: str) -> None:
        self.id = id

    def get_parts(self) -> tuple[str, str]:
        parts = self.id.split(":", maxsplit=1)
        return parts[0], parts[-1]

    def to_dict(self) -> str:
        return self.id

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SplittableID":
        return SplittableID(id=data["id"])


@dataclass
class Synonym(DataModelObject, CacheableDataModelObject):
    synonym: str

    def to_dict(self) -> dict[str, str]:
        return {"synonym": self.synonym}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Synonym":
        return Synonym(synonym=data["synonym"])

    def to_cache_dict(self) -> str:
        return self.synonym

    @classmethod
    def from_cache_dict(cls, data: Any) -> "Synonym":
        return Synonym(data)

    @staticmethod
    def type_guard(obj: "CacheableDataModelObject") -> TypeGuard["Synonym"]:
        return isinstance(obj, Synonym) and hasattr(obj, "synonym")


@dataclass
class AssessedBiomarkerEntity(
    DataModelObject, CacheableDataModelObject, RecommendedNameObject
):
    recommended_name: str
    synonyms: list[Synonym] = field(default_factory=list)

    def to_dict(self) -> dict[str, Union[str, list[dict[str, str]]]]:
        return {
            "recommended_name": self.recommended_name,
            "synonyms": [s.to_dict() for s in self.synonyms],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AssessedBiomarkerEntity":
        return AssessedBiomarkerEntity(
            recommended_name=data["recommended_name"],
            synonyms=[Synonym.from_dict(s) for s in data["synonyms"]],
        )

    def to_cache_dict(self) -> dict[str, Union[str, list[str]]]:
        return {
            "recommended_name": self.recommended_name,
            "synonyms": [s.synonym for s in self.synonyms],
        }

    @classmethod
    def from_cache_dict(cls, data: Any) -> "AssessedBiomarkerEntity":
        return AssessedBiomarkerEntity(
            recommended_name=data["recommended_name"],
            synonyms=[Synonym.from_cache_dict(s) for s in data["synonyms"]],
        )

    def check_match(
        self, tsv_val: str, strict: bool = False, logger: Optional[Logger] = None
    ) -> bool:
        if logger:
            logger.debug(
                f"Checking match (strict: {strict}) between assessed biomarker entity name `{self.recommended_name}` and `{tsv_val}`"
            )
        if not strict:
            return self.recommended_name.lower().strip() == tsv_val.lower().strip()
        return self.recommended_name == tsv_val

    @staticmethod
    def type_guard(
        obj: "CacheableDataModelObject",
    ) -> TypeGuard["AssessedBiomarkerEntity"]:
        return (
            isinstance(obj, AssessedBiomarkerEntity)
            and hasattr(obj, "recommended_name")
            and hasattr(obj, "synonyms")
            and isinstance(obj.synonyms, list)
        )


@dataclass
class Specimen(DataModelObject):
    name: str
    id: SplittableID
    name_space: str
    url: str
    loinc_code: str

    def to_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "id": self.id.to_dict(),
            "name_space": self.name_space,
            "url": self.url,
            "loinc_code": self.loinc_code,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Specimen":
        return Specimen(
            name=data["name"],
            id=SplittableID.from_dict(data),
            name_space=data["name_space"],
            url=data["url"],
            loinc_code=data["loinc_code"],
        )

    @classmethod
    def from_row(cls, row: "TSVRow", url: str) -> "Specimen":
        specimen_id = SplittableID(id=row.specimen_id)
        resource, _ = specimen_id.get_parts()
        return Specimen(
            name=row.specimen,
            id=SplittableID(id=row.specimen_id),
            name_space=resource,
            url=url,
            loinc_code=row.loinc_code,
        )


@dataclass
class EvidenceTag(DataModelObject):
    tag: str

    def to_dict(self) -> dict[str, str]:
        return {"tag": self.tag}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EvidenceTag":
        return EvidenceTag(tag=data["tag"])


@dataclass
class EvidenceItem(DataModelObject):
    evidence: str

    def to_dict(self) -> dict[str, str]:
        return {"evidence": self.evidence}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EvidenceItem":
        return EvidenceItem(evidence=data["evidence"])


@dataclass
class Evidence(DataModelObject):
    id: str
    database: str
    url: str
    evidence_list: list[EvidenceItem]
    tags: list[EvidenceTag]

    def to_dict(self) -> dict[str, Union[str, list]]:
        return {
            "id": self.id,
            "database": self.database,
            "url": self.url,
            "evidence_list": [e.to_dict() for e in self.evidence_list],
            "tags": [t.to_dict() for t in self.tags],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Evidence":
        return Evidence(
            id=data["id"],
            database=data["database"],
            url=data["url"],
            evidence_list=[EvidenceItem.from_dict(e) for e in data["evidence_list"]],
            tags=[EvidenceTag.from_dict(t) for t in data["tags"]],
        )


@dataclass
class ConditionRecommendedName(
    DataModelObject, CacheableDataModelObject, RecommendedNameObject
):
    id: SplittableID
    name: str
    description: str
    resource: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return {
            "id": self.id.to_dict(),
            "name": self.name,
            "description": self.description,
            "resource": self.resource,
            "url": self.url,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConditionRecommendedName":
        return ConditionRecommendedName(
            id=SplittableID.from_dict(data),
            name=data["name"],
            description=data["description"],
            resource=data["resource"],
            url=data["url"],
        )

    def to_cache_dict(self) -> str:
        return self.name

    @classmethod
    def from_cache_dict(cls, data: Any, **kwargs) -> "ConditionRecommendedName":
        return ConditionRecommendedName(
            id=kwargs.get("id", SplittableID(id="")),
            name=data["recommended_name"],
            description=data["description"],
            resource=kwargs.get("resource", ""),
            url=kwargs.get("url", ""),
        )

    def check_match(
        self, tsv_val: str, strict: bool = False, logger: Optional[Logger] = None
    ) -> bool:
        if logger:
            logger.debug(
                f"Checking match (strict: {strict}) between condition name `{self.name}` and `{tsv_val}`"
            )
        if not strict:
            return self.name.lower().strip() == tsv_val.lower().strip()
        return self.name == tsv_val

    @staticmethod
    def type_guard(
        obj: "CacheableDataModelObject",
    ) -> TypeGuard["ConditionRecommendedName"]:
        return (
            isinstance(obj, ConditionRecommendedName)
            and hasattr(obj, "id")
            and hasattr(obj, "description")
            and hasattr(obj, "resource")
            and hasattr(obj, "url")
        )


@dataclass
class ConditionSynonym(DataModelObject, CacheableDataModelObject):
    id: SplittableID
    name: str
    resource: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return {
            "id": self.id.to_dict(),
            "name": self.name,
            "resource": self.resource,
            "url": self.url,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConditionSynonym":
        return ConditionSynonym(
            id=SplittableID.from_dict(data),
            name=data["name"],
            resource=data["resource"],
            url=data["url"],
        )

    def to_cache_dict(self) -> str:
        return self.name

    @classmethod
    def from_cache_dict(cls, data: Any, **kwargs) -> "ConditionSynonym":
        id: SplittableID = kwargs.get("id", SplittableID(id=""))
        return ConditionSynonym(
            id=id,
            name=kwargs.get("name", ""),
            resource=kwargs.get("resource", ""),
            url=kwargs.get("url", ""),
        )

    @staticmethod
    def type_guard(obj: "CacheableDataModelObject") -> TypeGuard["ConditionSynonym"]:
        return (
            isinstance(obj, ConditionSynonym)
            and hasattr(obj, "name")
            and hasattr(obj, "resource")
            and hasattr(obj, "url")
        )


@dataclass
class Condition(DataModelObject, CacheableDataModelObject):
    id: SplittableID
    recommended_name: ConditionRecommendedName
    synonyms: list[ConditionSynonym] = field(default_factory=list)

    def to_dict(self) -> dict[str, Union[str, dict[str, str], list[dict[str, str]]]]:
        return {
            "id": self.id.to_dict(),
            "recommended_name": self.recommended_name.to_dict(),
            "synonyms": [s.to_dict() for s in self.synonyms],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Condition":
        return Condition(
            id=SplittableID.from_dict(data),
            recommended_name=ConditionRecommendedName.from_dict(
                data["recommended_name"]
            ),
            synonyms=[ConditionSynonym.from_dict(s) for s in data["synonyms"]],
        )

    def to_cache_dict(self) -> dict[str, Union[str, list[str]]]:
        return_data: dict[str, Union[str, list[str]]] = {
            "recommended_name": self.recommended_name.name,
            "description": self.recommended_name.description,
            "synonyms": [ConditionSynonym.to_cache_dict(s) for s in self.synonyms],
        }
        return return_data

    @classmethod
    def from_cache_dict(cls, data: Any, **kwargs) -> "Condition":
        """Expects id, resource, and the list of"""
        id = SplittableID(id=kwargs.get("id", ""))
        resource = kwargs.get("resource", "")
        url = kwargs.get("url", "")
        condition_syns: list[ConditionSynonym] = []
        for syn in data["synonyms"]:
            condition_syns.append(
                ConditionSynonym.from_cache_dict(
                    data=data, name=syn, id=id, resource=resource, url=url
                )
            )
        return Condition(
            id=id,
            recommended_name=ConditionRecommendedName.from_cache_dict(
                data=data, id=id, resource=resource, url=url
            ),
            synonyms=condition_syns,
        )

    @staticmethod
    def type_guard(obj: "CacheableDataModelObject") -> TypeGuard["Condition"]:
        return (
            isinstance(obj, Condition)
            and hasattr(obj, "recommended_name")
            and isinstance(obj.recommended_name, ConditionRecommendedName)
            and hasattr(obj, "synonyms")
            and isinstance(obj.synonyms, list)
            and all(isinstance(s, ConditionSynonym) for s in obj.synonyms)
        )


@dataclass
class ExposureAgent(DataModelObject, CacheableDataModelObject):
    id: str
    recommended_name: ConditionRecommendedName
    synonyms: list[ConditionSynonym] = field(default_factory=list)

    def to_dict(self) -> dict[str, Union[str, dict[str, str], list[dict[str, str]]]]:
        return {
            "id": self.id,
            "recommended_name": self.recommended_name.to_dict(),
            "synonyms": [s.to_dict() for s in self.synonyms],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExposureAgent":
        return ExposureAgent(
            id=data["id"],
            recommended_name=ConditionRecommendedName.from_dict(
                data["recommended_name"]
            ),
            synonyms=[ConditionSynonym.from_dict(s) for s in data["synonyms"]],
        )

    def to_cache_dict(self) -> dict[str, Union[str, list[str]]]:
        return_data: dict[str, Union[str, list[str]]] = {
            "recommended_name": self.recommended_name.name,
            "description": self.recommended_name.description,
            "synonyms": [s.name for s in self.synonyms],
        }
        return return_data

    @classmethod
    def from_cache_dict(cls, data: Any, **kwargs) -> Any:
        id = kwargs.get("id", "")
        resource = kwargs.get("resource", "")
        url = kwargs.get("url", "")
        exp_syns: list[ConditionSynonym] = []
        for syn in data["synonyms"]:
            exp_syns.append(
                ConditionSynonym.from_cache_dict(
                    data=data, name=syn, id=id, resource=resource, url=url
                )
            )
        return ExposureAgent(
            id=id,
            recommended_name=ConditionRecommendedName.from_cache_dict(
                data=data, id=id, resource=resource, url=url
            ),
            synonyms=exp_syns,
        )

    @staticmethod
    def type_guard(obj: "CacheableDataModelObject") -> TypeGuard["ExposureAgent"]:
        return (
            isinstance(obj, ExposureAgent)
            and hasattr(obj, "recommended_name")
            and hasattr(obj, "synonyms")
        )


@dataclass
class Reference(DataModelObject):
    id: str
    type: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return {"id": self.id, "type": self.type, "url": self.url}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Reference":
        return Reference(id=data["id"], type=data["type"], url=data["url"])


@dataclass
class CitationEvidence(DataModelObject):
    database: str
    id: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return {"database": self.database, "id": self.id, "url": self.url}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CitationEvidence":
        return CitationEvidence(
            database=data["database"], id=data["id"], url=data["url"]
        )


@dataclass
class Citation(DataModelObject, CacheableDataModelObject):
    title: str
    journal: str
    authors: str
    date: str
    reference: list[Reference]
    evidence: list[CitationEvidence] = field(default_factory=list)

    def to_dict(self) -> dict[str, Union[str, list, dict]]:
        return {
            "title": self.title,
            "journal": self.journal,
            "authors": self.authors,
            "date": self.date,
            "reference": [ref.to_dict() for ref in self.reference],
            "evidence": [ev.to_dict() for ev in self.evidence],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Citation":
        return Citation(
            title=data["title"],
            journal=data["journal"],
            authors=data["authors"],
            date=data["date"],
            reference=[Reference.from_dict(r) for r in data["reference"]],
            evidence=[CitationEvidence.from_dict(e) for e in data["evidence"]],
        )

    def to_cache_dict(self) -> dict[str, str]:
        return_data: dict[str, str] = {
            "title": self.title,
            "journal": self.journal,
            "authors": self.authors,
            "publication_date": self.date,
        }
        return return_data

    @classmethod
    def from_cache_dict(cls, data: Any) -> "Citation":
        return Citation(
            title=data["title"],
            journal=data["journal"],
            authors=data["authors"],
            date=data["publication_date"],
            reference=[],
            evidence=[],
        )

    @staticmethod
    def type_guard(obj: "CacheableDataModelObject") -> TypeGuard["Citation"]:
        return (
            isinstance(obj, Citation)
            and hasattr(obj, "title")
            and hasattr(obj, "journal")
            and hasattr(obj, "authors")
            and hasattr(obj, "date")
            and hasattr(obj, "reference")
            and isinstance(obj.reference, list)
            and all(isinstance(r, Reference) for r in obj.reference)
        )


@dataclass
class BiomarkerRole(DataModelObject):
    role: str

    def to_dict(self) -> dict:
        return {"role": self.role}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BiomarkerRole":
        return BiomarkerRole(role=data["role"])


@dataclass
class BiomarkerComponent(DataModelObject):
    biomarker: str
    assessed_biomarker_entity: AssessedBiomarkerEntity
    assessed_biomarker_entity_id: SplittableID
    assessed_entity_type: str
    specimen: list[Specimen] = field(default_factory=list)
    evidence_source: list[Evidence] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "biomarker": self.biomarker,
            "assessed_biomarker_entity": self.assessed_biomarker_entity.to_dict(),
            "assessed_biomarker_entity_id": self.assessed_biomarker_entity_id.to_dict(),
            "assessed_entity_type": self.assessed_entity_type,
            "specimen": [s.to_dict() for s in self.specimen],
            "evidence_source": [e.to_dict() for e in self.evidence_source],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BiomarkerComponent":
        return BiomarkerComponent(
            biomarker=data["biomarker"],
            assessed_biomarker_entity=AssessedBiomarkerEntity.from_dict(
                data["assessed_biomarker_entity"]
            ),
            assessed_biomarker_entity_id=SplittableID(
                id=data["assessed_biomarker_entity_id"]
            ),
            assessed_entity_type=data["assessed_entity_type"],
            specimen=[Specimen.from_dict(s) for s in data["specimen"]],
            evidence_source=[Evidence.from_dict(e) for e in data["evidence_source"]],
        )


@dataclass
class BiomarkerEntry(DataModelObject):
    """Main biomarker entry data model."""

    biomarker_id: str
    biomarker_component: list[BiomarkerComponent]
    best_biomarker_role: list[BiomarkerRole]
    condition: Optional[Condition] = None
    exposure_agent: Optional[ExposureAgent] = None
    evidence_source: list[Evidence] = field(default_factory=list)
    citation: list[Citation] = field(default_factory=list)
    # Retain other fields like canonical id
    kwargs: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert entry to dictionary format for JSON serialization."""
        base: dict = {
            "biomarker_id": self.biomarker_id,
            "biomarker_component": [c.to_dict() for c in self.biomarker_component],
            "best_biomarker_role": [r.to_dict() for r in self.best_biomarker_role],
            "evidence_source": [e.to_dict() for e in self.evidence_source],
            "citation": [c.to_dict() for c in self.citation],
        }

        if self.condition:
            base["condition"] = self.condition.to_dict()
        elif self.exposure_agent:
            base["exposure_agent"] = self.exposure_agent.to_dict()
        else:
            raise ValueError(
                f"Didn't find condition or exposure agent in biomarker entry {self.biomarker_id}"
            )

        base.update(self.kwargs)
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BiomarkerEntry":
        known_fields = {
            "biomarker_id": data["biomarker_id"],
            "biomarker_component": [
                BiomarkerComponent.from_dict(c) for c in data["biomarker_component"]
            ],
            "best_biomarker_role": [
                BiomarkerRole.from_dict(r) for r in data["best_biomarker_role"]
            ],
            "evidence_source": [Evidence.from_dict(c) for c in data["evidence_source"]],
            "citation": [Citation.from_dict(c) for c in data["citation"]],
        }

        if "condition" in data:
            known_fields["condition"] = Condition.from_dict(data["condition"])
        elif "exposure_agent" in data:
            known_fields["exposure_agent"] = ExposureAgent.from_dict(
                data["exposure_agent"]
            )
        else:
            raise ValueError(
                f"Didn't find `condition` or `exposure_agent` in biomarker: {pformat(data)}"
            )

        # Preserve any extra fields in kwargs
        extra_fields = {
            k: v
            for k, v in data.items()
            if k not in known_fields and k not in ["condition", "exposure_agent"]
        }
        known_fields["kwargs"] = extra_fields

        return cls(**known_fields)

    def collect_unique_evidence_sources(self) -> dict[str, set[str]]:
        """Returns the unique evidence sources by resource.

        Returns
        -------
        dict[str, set[str]]
            Key is the resource, value is the unique IDs under
            that resource/database.
        """
        sources: dict[str, set[str]] = {}
        for component in self.biomarker_component:
            for component_evidence in component.evidence_source:
                if component_evidence.database not in sources:
                    sources[component_evidence.database] = set()
                sources[component_evidence.database].add(component_evidence.id)
        for top_level_evidence in self.evidence_source:
            if top_level_evidence.database not in sources:
                sources[top_level_evidence.database] = set()
            sources[top_level_evidence.database].add(top_level_evidence.id)
        return sources

    def add_or_merge_citation(self, new_citation: Citation) -> None:
        """Adds or merges a new citation.

        Parameters
        ----------
        new_citation: Citation
            The new citation to add or merge.
        """
        for existing_citation in self.citation:
            if (
                existing_citation.title == new_citation.title
                and existing_citation.journal == new_citation.journal
                and existing_citation.authors == new_citation.authors
                and existing_citation.date == new_citation.date
            ):
                existing_refs = {
                    (ref.id, ref.type, ref.url) for ref in existing_citation.reference
                }
                for new_ref in new_citation.reference:
                    new_ref_tuple = (new_ref.id, new_ref.type, new_ref.url)
                    if new_ref_tuple not in existing_refs:
                        existing_citation.reference.append(new_ref)

                existing_evidence = {
                    (ev.database, ev.id, ev.url) for ev in existing_citation.evidence
                }
                for new_ev in new_citation.evidence:
                    new_ev_tuple = (new_ev.database, new_ev.id, new_ev.url)
                    if new_ev_tuple not in existing_evidence:
                        existing_citation.evidence.append(new_ev)
                return
        self.citation.append(new_citation)


@dataclass
class CrossReference(DataModelObject):
    id: str
    url: str
    database: str
    categories: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Union[str, list[str]]]:
        return {
            "id": self.id,
            "url": self.url,
            "database": self.database,
            "categories": [c for c in self.categories],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Any:
        return CrossReference(
            id=data["id"],
            url=data["url"],
            database=data["database"],
            categories=data.get("categories", []),
        )


@dataclass
class BiomarkerEntryWCrossReference(DataModelObject):
    """Main biomarker entry data model."""

    biomarker_id: str
    biomarker_component: list[BiomarkerComponent]
    best_biomarker_role: list[BiomarkerRole]
    condition: Optional[Condition] = None
    exposure_agent: Optional[ExposureAgent] = None
    evidence_source: list[Evidence] = field(default_factory=list)
    citation: list[Citation] = field(default_factory=list)
    crossref: list[CrossReference] = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert entry to dictionary format for JSON serialization."""
        base: dict = {
            "biomarker_id": self.biomarker_id,
            "biomarker_component": [c.to_dict() for c in self.biomarker_component],
            "best_biomarker_role": [r.to_dict() for r in self.best_biomarker_role],
            "evidence_source": [e.to_dict() for e in self.evidence_source],
            "citation": [c.to_dict() for c in self.citation],
            "crossref": [c.to_dict() for c in self.crossref],
        }

        if self.condition is not None:
            base["condition"] = self.condition.to_dict()
        elif self.exposure_agent is not None:
            base["exposure_agent"] = self.exposure_agent.to_dict()
        else:
            raise ValueError(
                f"Did not find condition or exposure agent in BiomarkerEntryWCrossReference: {self}"
            )

        base.update(self.kwargs)
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BiomarkerEntryWCrossReference":
        known_fields = {
            "biomarker_id": data["biomarker_id"],
            "biomarker_component": [
                BiomarkerComponent.from_dict(c) for c in data["biomarker_component"]
            ],
            "best_biomarker_role": [
                BiomarkerRole.from_dict(r) for r in data["best_biomarker_role"]
            ],
            "evidence_source": [Evidence.from_dict(e) for e in data["evidence_source"]],
            "citation": [Citation.from_dict(c) for c in data["citation"]],
            "crossref": [CrossReference.from_dict(c) for c in data.get("crossref", [])],
        }

        if "condition" in data:
            known_fields["condition"] = Condition.from_dict(data["condition"])
        elif "exposure_agent" in data:
            known_fields["exposure_agent"] = ExposureAgent.from_dict(
                data["exposure_agent"]
            )
        else:
            raise ValueError(
                f"Didn't find `condition` or `exposure_agent` in biomarker: {pformat(data)}"
            )

        extra_fields = {
            k: v
            for k, v in data.items()
            if k not in known_fields and k not in ["condition", "exposure_agent"]
        }
        known_fields["kwargs"] = extra_fields

        return cls(**known_fields)

    @classmethod
    def from_biomarker_entry(
        cls,
        entry: BiomarkerEntry,
        cross_references: Union[CrossReference, list[CrossReference]],
    ) -> "BiomarkerEntryWCrossReference":
        if isinstance(cross_references, CrossReference):
            cross_references = [cross_references]

        return BiomarkerEntryWCrossReference(
            biomarker_id=entry.biomarker_id,
            biomarker_component=entry.biomarker_component,
            best_biomarker_role=entry.best_biomarker_role,
            condition=entry.condition,
            exposure_agent=entry.exposure_agent,
            evidence_source=entry.evidence_source,
            citation=entry.citation,
            crossref=cross_references,
            kwargs=entry.kwargs,
        )


@dataclass
class CrossReferenceMap:
    database: str
    url: str
    id_examples: list[str]
    id_map: dict[str, str]
    categories: list[str]
    secondary_cross_references: list[str]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CrossReferenceMap":
        return CrossReferenceMap(
            database=data["database"],
            url=data["url"],
            id_examples=data["id_examples"],
            id_map=data["id_map"],
            categories=data["categories"],
            secondary_cross_references=data["secondary_cross_references"],
        )

    @classmethod
    def from_file(cls, filepath: Path) -> "CrossReferenceMap":
        return CrossReferenceMap.from_dict(
            load_json_type_safe(filepath=filepath, return_type="dict")
        )
