from dataclasses import dataclass
from enum import Enum


@dataclass
class Triple:
    subject: str
    predicate: str
    object: str

    def __str__(self) -> str:
        return f"<{self.subject}> <{self.predicate}> <{self.object}> ."


class TripleCategory(Enum):
    SUBJECT_OBJECTS = "subject_objects"
    PREDICATES = "predicates"
