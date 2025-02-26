from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from . import BiomarkerComponent, EvidenceTag, EvidenceItem


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

    def core_equal_component(self, component: "BiomarkerComponent") -> bool:
        """Whether the core component fields match the row.

        Parameters
        ----------
        component: BiomarkerComponent
            The component to check against.

        Returns
        -------
        bool
            True if they match, False otherwise.
        """
        if (
            component.biomarker == self.biomarker
            and component.assessed_biomarker_entity_id.to_dict()
            == self.assessed_biomarker_entity_id
            and component.assessed_entity_type.lower()
            == self.assessed_entity_type.lower()
        ):
            return True
        return False

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

    def combine_evidence(self, new_evidence: list["EvidenceItem"]) -> None:
        for item in new_evidence:
            self.evidence_texts.add(item.evidence)

    def combine_tags(
        self, new_tags: list["EvidenceTag"], object_fields: ObjectFieldTags
    ) -> None:
        object_fields_dict = object_fields.to_dict()

        for tag in new_tags:
            tag_parts = tag.tag.split(":", 1)
            tag_type = tag_parts[0]
            tag_value = tag_parts[1] if len(tag_parts) > 1 else ""

            # TODO : this is ugly
            # Handle object field specific tags
            if tag_type in object_fields_dict:
                field_value = object_fields_dict[tag_type]
                if tag_value:
                    if field_value:
                        if not tag_value or tag_value == field_value:
                            self.tags.add(tag_type)
                continue

            # Handle component singular fields
            if tag_type in COMPONENT_SINGULAR_EVIDENCE_FIELDS:
                self.tags.add(tag_type)
                continue

            # Hanle top level fields
            if tag_type in TOP_LEVEL_EVIDENCE_FIELDS:
                self.tags.add(tag_type)
                continue

            self.tags.add(tag.tag)

    @property
    def evidence_text(self) -> str:
        return TSVRow.get_evidence_text_delimiter().join(sorted(self.evidence_texts))

    @property
    def tag_string(self) -> str:
        return TSVRow.get_tag_delimiter().join(sorted(self.tags))
