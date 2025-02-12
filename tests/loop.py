from pathlib import Path
import pytest
import json
from typing import Iterator
from deepdiff.diff import DeepDiff

from utils.converters.json_to_tsv import JSONtoTSVConverter
from utils.converters.tsv_to_json import TSVtoJSONConverter
from utils.logging import LoggerFactory


class TestBidirectionalConversion:
    """Tests for verifying bidirectional conversion integrity between TSV and JSON formats."""

    @pytest.fixture(autouse=True)
    def setup_logging(self, tmp_path: Path) -> Iterator[None]:
        """Initialize logging before each test."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        LoggerFactory.initialize(
            log_path=log_dir / "test.log", debug=False, console_output=False
        )
        yield
        LoggerFactory._instance = None
        LoggerFactory._initialized = False

    @pytest.fixture
    def json_to_tsv(self) -> JSONtoTSVConverter:
        """Get JSON to TSV converter instance."""
        return JSONtoTSVConverter()

    @pytest.fixture
    def tsv_to_json(self) -> TSVtoJSONConverter:
        """Get TSV to JSON converter instance."""
        return TSVtoJSONConverter(fetch_metadata=False, preload_caches=True)

    @pytest.fixture
    def data_dir(self) -> Path:
        """Get the test data directory."""
        return Path(__file__).parent / "data"

    def normalize_json(self, json_path: Path) -> dict:
        """Load and normalize JSON for comparison."""
        with json_path.open() as f:
            return json.load(f)

    def normalize_tsv(self, tsv_path: Path) -> str:
        """Load and normalize TSV content for comparison."""
        normalized_lines = []
        with tsv_path.open() as f:
            # Skip header
            header = next(f).strip().split("\t")
            header_len = len(header)

            # Process each line
            for line_num, line in enumerate(
                f, start=2
            ):  # Start at 2 to account for header
                parts = line.strip().split("\t")

                # Validate line has correct number of columns
                if len(parts) != header_len:
                    raise ValueError(
                        f"Line {line_num} has {len(parts)} columns, expected {header_len}: {line}"
                    )

                # Normalize empty fields
                parts = [part if part else "" for part in parts]

                # Handle tags specially (last column)
                if parts[-1]:
                    try:
                        tags = sorted(filter(None, parts[-1].split(";")))
                        parts[-1] = ";".join(tags)
                    except Exception as e:
                        raise ValueError(
                            f"Error processing tags on line {line_num}: {parts[-1]}"
                        ) from e

                normalized_lines.append("\t".join(parts))

        return "\n".join(sorted(normalized_lines))

    @pytest.mark.parametrize(
        "tsv_name", ["mw.tsv", "gwas.tsv", "PMC_biomarker_sets.tsv"]
    )
    def test_bidirectional_conversion(
        self,
        json_to_tsv: JSONtoTSVConverter,
        tsv_to_json: TSVtoJSONConverter,
        data_dir: Path,
        tmp_path: Path,
        tsv_name: str,
    ) -> None:
        """
        Test that converting from TSV -> JSON -> TSV -> JSON produces identical results.
        """
        logger = LoggerFactory.get_logger("test_bidirectional")
        logger.info(f"Starting conversion test for {tsv_name}" + ("-" * 50))

        # Setup test paths
        source_tsv = data_dir / "tsv" / tsv_name
        test_dir = tmp_path / tsv_name.replace(".tsv", "")
        test_dir.mkdir()

        # Create conversion paths
        tsv_conversions = [test_dir / f"conversion{i}.tsv" for i in range(3)]
        json_conversions = [test_dir / f"conversion{i}.json" for i in range(3)]

        # Initial copy of source TSV
        with source_tsv.open("r") as src, tsv_conversions[0].open("w") as dst:
            dst.write(src.read())

        # Perform conversions
        for i in range(3):
            # TSV to JSON
            tsv_to_json = TSVtoJSONConverter(fetch_metadata=False, preload_caches=True)
            tsv_to_json.convert(tsv_conversions[i], json_conversions[i])

            if i < 2:  # Skip final TSV conversion as we don't need it
                # JSON to TSV
                json_to_tsv = JSONtoTSVConverter()
                json_to_tsv.convert(json_conversions[i], tsv_conversions[i + 1])

        # Compare TSV files
        for i in range(len(tsv_conversions) - 1):
            tsv1_content = self.normalize_tsv(tsv_conversions[i])
            tsv2_content = self.normalize_tsv(tsv_conversions[i + 1])

            if tsv1_content != tsv2_content:
                # Convert string content back to sets of lines for comparison
                tsv1_lines = set(tsv1_content.split("\n"))
                tsv2_lines = set(tsv2_content.split("\n"))

                # Find differences
                only_in_tsv1 = tsv1_lines - tsv2_lines
                only_in_tsv2 = tsv2_lines - tsv1_lines

                # Format error message
                error_msg = [
                    f"TSV content mismatch between conversion {i} and {i+1} for {tsv_name}:"
                ]

                if only_in_tsv1:
                    error_msg.append("\nLines only in original TSV:")
                    for line in sorted(only_in_tsv1):
                        error_msg.append(f"  {line}")

                if only_in_tsv2:
                    error_msg.append("\nLines only in converted TSV:")
                    for line in sorted(only_in_tsv2):
                        error_msg.append(f"  {line}")

                raise AssertionError("\n".join(error_msg))

        # Compare JSON files
        for i in range(len(json_conversions) - 1):
            json1_content = self.normalize_json(json_conversions[i])
            json2_content = self.normalize_json(json_conversions[i + 1])

            diff = DeepDiff(json1_content, json2_content, ignore_order=True)
            if diff:
                error_msg = [
                    f"JSON content mismatch between conversion {i} and {i+1} for {tsv_name}:",
                    "Differences found:",
                    str(diff),
                ]
                raise AssertionError("\n".join(error_msg))
