from dataclasses import dataclass, field
from typing import Any, Optional, Union


@dataclass
class Synonym:
    synonym: str


@dataclass
class AssessedBiomarkerEntity:
    recommended_name: str
    synonyms: list[Synonym] = field(default_factory=list)

    def to_dict(self) -> dict[str, Union[str, list[str]]]:
        return {
            "recommended_name": self.recommended_name,
            "synonyms": [s.synonym for s in self.synonyms],
        }


@dataclass
class Specimen:
    name: str
    id: str
    name_space: str
    url: str
    loinc_code: str


@dataclass
class EvidenceTag:
    tag: str


@dataclass
class EvidenceItem:
    evidence: str


@dataclass
class Evidence:
    id: str
    database: str
    url: str
    evidence_list: list[EvidenceItem]
    tags: list[EvidenceTag]


@dataclass
class RecommendedName:
    id: str
    name: str
    description: Optional[str]
    resource: str
    url: str


@dataclass
class ConditionSynonym:
    id: str
    name: str
    resource: str
    url: str


@dataclass
class Condition:
    id: str
    recommended_name: RecommendedName
    synonyms: list[ConditionSynonym] = field(default_factory=list)


@dataclass
class ExposureAgent:
    id: str
    recommended_name: RecommendedName
    synonyms: list[ConditionSynonym] = field(default_factory=list)


@dataclass
class Reference:
    id: str
    type: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return {"id": self.id, "type": self.type, "url": self.url}


@dataclass
class CitationEvidence:
    database: str
    id: str
    url: str

    def to_dict(self) -> dict[str, str]:
        return {"database": self.database, "id": self.id, "url": self.url}


@dataclass
class Citation:
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


@dataclass
class BiomarkerRole:
    role: str


@dataclass
class BiomarkerComponent:
    biomarker: str
    assessed_biomarker_entity: AssessedBiomarkerEntity
    assessed_biomarker_entity_id: str
    assessed_entity_type: str
    specimen: list[Specimen] = field(default_factory=list)
    evidence_source: list[Evidence] = field(default_factory=list)


@dataclass
class BiomarkerEntry:
    """Main biomarker entry data model."""

    biomarker_id: str
    biomarker_component: list[BiomarkerComponent]
    best_biomarker_role: list[BiomarkerRole]
    condition: Condition
    exposure_agent: ExposureAgent
    evidence_source: list[Evidence] = field(default_factory=list)
    citation: list[Citation] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert entry to dictionary format for JSON serialization."""
        return {
            "biomarker_id": self.biomarker_id,
            "biomarker_component": [
                self._component_to_dict(c) for c in self.biomarker_component
            ],
            "best_biomarker_role": [{"role": r.role} for r in self.best_biomarker_role],
            "condition": (
                self._condition_to_dict(self.condition) if self.condition else None
            ),
            "evidence_source": [
                self._evidence_to_dict(e) for e in self.evidence_source
            ],
            "citation": [self._citation_to_dict(c) for c in self.citation],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BiomarkerEntry":
        """Create BiomarkerEntry from dictionary data."""
        return cls(
            biomarker_id=data["biomarker_id"],
            biomarker_component=[
                cls._dict_to_component(c) for c in data["biomarker_component"]
            ],
            best_biomarker_role=[
                BiomarkerRole(**r) for r in data["best_biomarker_role"]
            ],
            condition=cls._dict_to_condition(data.get("condition", None)),
            exposure_agent=cls._dict_to_exposure_agent(
                data.get("exposure_agent", None)
            ),
            evidence_source=[
                cls._dict_to_evidence(e) for e in data.get("evidence_source", [])
            ],
            citation=[cls._dict_to_citation(c) for c in data.get("citation", [])],
        )

    @staticmethod
    def _component_to_dict(component: BiomarkerComponent) -> dict[str, Any]:
        """Convert BimomarkerComponent to dictionary."""
        return {
            "biomarker": component.biomarker,
            "assessed_biomarker_entity": {
                "recommended_name": component.assessed_biomarker_entity.recommended_name,
                "synonyms": [s for s in component.assessed_biomarker_entity.synonyms],
            },
            "assessed_biomarker_entity_id": component.assessed_biomarker_entity_id,
            "assessed_entity_type": component.assessed_entity_type,
            "specimen": [
                {
                    "name": s.name,
                    "id": s.id,
                    "name_space": s.name_space,
                    "url": s.url,
                    "loinc_code": s.loinc_code,
                }
                for s in component.specimen
            ],
            "evidence_source": [
                BiomarkerEntry._evidence_to_dict(e) for e in component.evidence_source
            ],
        }

    @staticmethod
    def _evidence_to_dict(evidence: Evidence) -> dict[str, Any]:
        """Convert Evidence to dictionary."""
        return {
            "id": evidence.id,
            "database": evidence.database,
            "url": evidence.url,
            "evidence_list": [{"evidence": e.evidence} for e in evidence.evidence_list],
            "tags": [{"tag": t.tag} for t in evidence.tags],
        }

    @staticmethod
    def _condition_to_dict(condition: Optional[Condition]) -> Optional[dict[str, Any]]:
        """Convert Condition to dictionary."""
        if not condition:
            return None
        return {
            "id": condition.id,
            "recommended_name": {
                "id": condition.recommended_name.id,
                "name": condition.recommended_name.name,
                "description": condition.recommended_name.description,
                "resource": condition.recommended_name.resource,
                "url": condition.recommended_name.url,
            },
            "synonyms": [
                {"id": s.id, "name": s.name, "resource": s.resource, "url": s.url}
                for s in condition.synonyms
            ],
        }

    @staticmethod
    def _citation_to_dict(citation: Citation) -> dict[str, Any]:
        """Convert Citation to dictionary."""
        return {
            "title": citation.title,
            "journal": citation.journal,
            "authors": citation.authors,
            "date": citation.date,
            "reference": [
                {"id": r.id, "type": r.type, "url": r.url} for r in citation.reference
            ],
            "evidence": citation.evidence,
        }

    @classmethod
    def _dict_to_component(cls, data: dict[str, Any]) -> BiomarkerComponent:
        """Create BiomarkerComponent from dictionary data."""
        specimen_data = data.get("specimen")
        if specimen_data is None:
            specimen_data = []

        return BiomarkerComponent(
            biomarker=data["biomarker"],
            assessed_biomarker_entity=AssessedBiomarkerEntity(
                recommended_name=data["assessed_biomarker_entity"]["recommended_name"],
                synonyms=[
                    Synonym(**s)
                    for s in data["assessed_biomarker_entity"].get("synonyms", [])
                ],
            ),
            assessed_biomarker_entity_id=data["assessed_biomarker_entity_id"],
            assessed_entity_type=data["assessed_entity_type"],
            specimen=[Specimen(**s) for s in specimen_data],
            evidence_source=[
                cls._dict_to_evidence(e) for e in data.get("evidence_source", [])
            ],
        )

    @staticmethod
    def _dict_to_evidence(data: dict[str, Any]) -> Evidence:
        """Create Evidence from dictionary data."""
        return Evidence(
            id=data["id"],
            database=data["database"],
            url=data["url"],
            evidence_list=[EvidenceItem(**e) for e in data["evidence_list"]],
            tags=[EvidenceTag(**t) for t in data["tags"]],
        )

    @classmethod
    def _dict_to_condition(cls, data: Optional[dict[str, Any]]) -> Condition:
        """Create Condition from dictionary data."""
        if data is None:
            return Condition(
                id="",
                recommended_name=RecommendedName(
                    id="", name="", description="", resource="", url=""
                ),
                synonyms=[],
            )
        return Condition(
            id=data["id"],
            recommended_name=RecommendedName(**data["recommended_name"]),
            synonyms=[ConditionSynonym(**s) for s in data.get("synonyms", [])],
        )

    @classmethod
    def _dict_to_exposure_agent(cls, data: Optional[dict[str, Any]]) -> ExposureAgent:
        """Create ExposureAgent from dictionary data."""
        if data is None:
            return ExposureAgent(
                id="",
                recommended_name=RecommendedName(
                    id="", name="", description="", resource="", url=""
                ),
                synonyms=[],
            )
        return ExposureAgent(
            id=data["id"],
            recommended_name=RecommendedName(**data["recommended_name"]),
            synonyms=[ConditionSynonym(**s) for s in data.get("synonyms", [])],
        )

    @staticmethod
    def _dict_to_citation(data: dict[str, Any]) -> Citation:
        """Create Citation from dictionary data."""
        return Citation(
            title=data["title"],
            journal=data["journal"],
            authors=data["authors"],
            date=data["date"],
            reference=[Reference(**r) for r in data["reference"]],
            evidence=data.get("evidence", []),
        )


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

    @classmethod
    def from_dict(cls, row: dict[str, str]) -> "TSVRow":
        cleaned_row = {}

        for field in cls.__dataclass_fields__:
            cleaned_row[field] = row.get(field, "").strip()

        return cls(**cleaned_row)

    @property
    def headers(self) -> list[str]:
        return list(self.__dataclass_fields__.keys())

    @classmethod
    def get_headers(cls) -> list[str]:
        return list(cls.__dataclass_fields__.keys())

    @classmethod
    def get_role_delimiter(cls) -> str:
        return ";"

    @classmethod
    def get_evidence_text_delimiter(cls) -> str:
        return ";|"

    @classmethod
    def get_tag_delimiter(cls) -> str:
        return ";"


@dataclass
class ObjectFieldTags:
    """Represents the fields that are referenced with a value in tags."""

    specimen: str = ""
    loinc_code: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }

    @classmethod
    def get_fields(cls) -> set[str]:
        return set(cls.__dataclass_fields__.keys())


COMPONENT_SINGULAR_EVIDENCE_FIELDS = {
    "biomarker",
    "assessed_biomarker_entity",
    "assessed_biomarker_entity_id",
    "assessed_entity_type",
}

TOP_LEVEL_EVIDENCE_FIELDS = {"condition", "exposure_agent", "best_biomarker_role"}


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
            if tag_type in COMPONENT_SINGULAR_EVIDENCE_FIELDS:
                self.tags.add(tag_type)
                continue

            # Hanle top level fields
            if tag_type in TOP_LEVEL_EVIDENCE_FIELDS:
                self.tags.add(tag_type)

    @property
    def evidence_text(self) -> str:
        return TSVRow.get_evidence_text_delimiter().join(sorted(self.evidence_texts))

    @property
    def tag_string(self) -> str:
        return TSVRow.get_tag_delimiter().join(sorted(self.tags))
