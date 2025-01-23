from pathlib import Path
from utils.converters.json_to_tsv import JSONtoTSVConverter


def main():
    converter = JSONtoTSVConverter()
    input_path = Path("input.json")
    output_path = Path("output.tsv")
    converter.convert(input_path, output_path)


if __name__ == "__main__":
    main()
