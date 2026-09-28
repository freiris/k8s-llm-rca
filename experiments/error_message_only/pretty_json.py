"""Export JSONL as an indented JSON array for reading and json.load()."""

import argparse
import json
from pathlib import Path


def export_jsonl(source, destination=None):
    source = Path(source)
    destination = Path(destination) if destination is not None else source.with_suffix(".json")
    if source.resolve() == destination.resolve():
        raise ValueError("Output must differ from the source JSONL")
    records = []
    with source.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {number}: {exc.msg}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Line {number} must be a JSON object")
            records.append(record)
    destination.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Existing results.jsonl or another JSONL file")
    parser.add_argument("--output", type=Path, help="Defaults to the same path with a .json suffix")
    args = parser.parse_args()
    try:
        print(export_jsonl(args.input, args.output))
    except (OSError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
