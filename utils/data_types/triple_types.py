from dataclasses import dataclass


@dataclass
class Triple:
    subject: str
    predicate: str
    object: str

    def __str__(self) -> str:
        return f"<{self.subject}> <{self.predicate}> <{self.object}> ."


@dataclass
class TripleSubjectObjects:

    @classmethod
    def name(cls) -> str:
        return "subject_objects"

    @classmethod
    def id_key(cls) -> str:
        return "biomarker_id"

    @classmethod
    def canonical_key(cls) -> str:
        return "biomarker_canonical_id"

    @classmethod
    def role_key(cls) -> str:
        return "best_biomarker_role"

    @classmethod
    def role_check(cls, role: str) -> bool:
        return role in [
            "risk",
            "diagnostic",
            "prognostic",
            "monitoring",
            "predictive",
            "response",
            "safety",
        ]


@dataclass
class TriplePredicates:

    @classmethod
    def name(cls) -> str:
        return "predicates"

    @classmethod
    def change_key(cls) -> str:
        return "biomarker_change"

    @classmethod
    def role_key(cls) -> str:
        return "best_biomarker_role"

    @classmethod
    def specimen_key(cls) -> str:
        return "specimen_sampled_from"

    @classmethod
    def condition_key(cls) -> str:
        return "condition_role_indicator"

    @classmethod
    def condition_role_check(cls, role: str) -> bool:
        return role in ["diagnostic", "risk", "monitoring", "prognostic"]
