from abc import ABC, abstractmethod
from pathlib import Path

TSV_LOG_CHECKPOINT = 500
JSON_LOG_CHECKPOINT = 250

class Converter(ABC):

    @abstractmethod
    def convert(self, input_path: Path, output_path: Path) -> None:
        pass
