from pathlib import Path
from utils.converters.json_to_tsv import JSONtoTSVConverter
from argparse import ArgumentParser, Namespace
import sys


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("input", type=Path, help="Path to input file")
    parser.add_argument("output", type=Path, help="Path to output file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input: Path = args.input
    output: Path = args.output

    if not input.exists():
        raise FileNotFoundError(f"Input file does not exist: {args.input}")
    if not output.parent.exists():
        raise ValueError(f"Output directory does not exist: {args.output.parent}")

    if input.suffix.lower() == ".json" and output.suffix.lower() == ".tsv":
        print(f"Converting JSON to TSV: {input} -> {output}")
        converter = JSONtoTSVConverter()
    else:
        print(f"Invalid conversion: {input.suffix} -> {output.suffix}")
        sys.exit(1)

    converter.convert(input, output)

    print("Conversion completed successfully!")


if __name__ == "__main__":
    main()
