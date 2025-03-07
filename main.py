from pathlib import Path
from pprint import pformat
from utils.converters import Converter
from utils.converters.json_to_tsv import JSONtoTSVConverter
from utils.converters.tsv_to_json import TSVtoJSONConverter
from utils.converters.json_to_nt import JSONtoNTConverter
from utils.converters.add_xrefs import XrefConverter
from utils.logging import LoggerFactory
from argparse import ArgumentParser, Namespace
from traceback import format_exc
import sys
from time import time

DEFAULT_LOG_DIR = Path(__file__).parent / "logs"


def parse_args() -> Namespace:
    """Parse command line arguments."""

    parser = ArgumentParser()

    # Required args
    parser.add_argument("input", type=Path, help="Path to input file")
    parser.add_argument("output", type=Path, help="Path to output file")

    # TSV to JSON args
    tsvJSON_group = parser.add_argument_group("TSV to JSON options")
    tsvJSON_group.add_argument(
        "-m",
        "--metadata",
        action="store_false",
        dest="metadata",
        help="Whether to fetch synonym and recommended name metdata from APIs (default true)",
    )
    tsvJSON_group.add_argument(
        "-p",
        "--preload-cache",
        action="store_true",
        dest="preload_cache",
        help="Whether to preload the cache data (default false)",
    )

    # Cross reference args
    xref_group = parser.add_argument_group("Cross reference options")
    xref_group.add_argument(
        "-x",
        "--xref",
        action="store_true",
        dest="xref",
        help="Whether to inject cross references, can only be run on its own and not combined with other conversions (accepts directories for input arg)",
    )

    # Logging args
    log_group = parser.add_argument_group("logging options")
    log_group.add_argument("--debug", action="store_true", help="Run in debug mode")
    log_group.add_argument(
        "--log-dir",
        type=Path,
        default=DEFAULT_LOG_DIR,
        help=f"Directory for log file (default: {DEFAULT_LOG_DIR})",
    )
    log_group.add_argument(
        "--rotate-logs",
        action="store_true",
        dest="rotate_logs",
        help="Enable log rotation by date",
    )
    log_group.add_argument(
        "--no-console",
        action="store_false",
        dest="console_output",
        help="Disable console logging output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    LoggerFactory.initialize(
        log_path=args.log_dir / "conversion.log",
        debug=args.debug,
        console_output=args.console_output,
        rotate_logs=args.rotate_logs,
    )
    logger = LoggerFactory.get_logger("main")
    logger.info(f"Starting conversion process with args:\n{pformat(vars(args))}")

    input: Path = args.input
    output: Path = args.output
    metadata: bool = args.metadata
    xref: bool = args.xref
    preload_cache: bool = args.preload_cache
    converter: Converter

    if not input.exists():
        msg = f"Input file or directory does not exist: {args.input}"
        logger.error(msg)
        raise FileNotFoundError(msg)
    if not output.parent.exists():
        msg = f"Output directory does not exist: {args.output.parent}"
        logger.error(msg)
        raise ValueError(msg)

    start_time = time()

    if xref:
        logger.info(f"Running cross reference injection on file(s) from: {input}")
        converter = XrefConverter()
    else:
        # JSON to TSV conversion
        if input.suffix.lower() == ".json" and output.suffix.lower() == ".tsv":
            msg = f"Converting JSON to TSV: {input} -> {output}"
            logger.info(msg)
            converter = JSONtoTSVConverter()
        # TSV to JSON conversion
        elif input.suffix.lower() == ".tsv" and output.suffix.lower() == ".json":
            msg = f"Converting TSV to JSON: {input} -> {output}"
            logger.info(msg)
            converter = TSVtoJSONConverter(
                fetch_metadata=metadata, preload_caches=preload_cache
            )
        # JSON to NT conversion
        elif input.suffix.lower() == ".json" and output.suffix.lower() == ".nt":
            msg = f"Converting JSON to NT: {input} -> {output}"
            logger.info(msg)
            converter = JSONtoNTConverter()
        # Unsupported conversion
        else:
            msg = f"Invalid conversion: {input.suffix} -> {output.suffix}"
            logger.error(msg)
            sys.exit(1)

    try:
        converter.convert(input, output)
    except Exception as e:
        msg = f"Conversion failed: {e}\n{format_exc()}"
        logger.error(msg)
        raise

    elapsed_time = time() - start_time
    finish_msg = f"Conversion completed successfully! Took {elapsed_time:.2f} seconds."
    logger.info(finish_msg)


if __name__ == "__main__":
    main()
