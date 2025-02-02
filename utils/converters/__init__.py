from abc import ABC, abstractmethod
from pathlib import Path

# Number of rows between logging checkpoints
TSV_LOG_CHECKPOINT = 500
JSON_LOG_CHECKPOINT = 250

class Converter(ABC):
    """Abstract class defining the interface for data converters."""

    @abstractmethod
    def convert(self, input_path: Path, output_path: Path) -> None:
        """Convert data between formats.

        Parameters
        ----------
        input_path: Path
            Path to the input file to convert.
        output_path: Path
            Path where the converted output should be written.
        """
        pass
