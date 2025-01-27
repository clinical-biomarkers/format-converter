from abc import ABC, abstractmethod
from pathlib import Path


class Converter(ABC):

    @abstractmethod
    def convert(self, input_path: Path, output_path: Path) -> None:
        pass
