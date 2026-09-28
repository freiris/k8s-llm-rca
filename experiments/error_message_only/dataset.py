"""Preserve metadata for outputs; model calls receive only the message."""

import csv
import json
from pathlib import Path

from cases import CASES


def load_jsonl(path):
    samples = []
    seen = set()
    for line_number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON at line {line_number}: {exc.msg}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"Line {line_number} must be a JSON object")
        for field in ("sample_id", "error_message"):
            if not isinstance(row.get(field), str) or not row[field].strip():
                raise ValueError(f"Line {line_number} needs a nonempty string {field}")
        if row["sample_id"] in seen:
            raise ValueError(f"Duplicate sample_id at line {line_number}: {row['sample_id']}")
        seen.add(row["sample_id"])
        sample = {field: row[field] for field in ("sample_id", "error_message")}
        for field in ("namespace", "timestamp", "uuid", "dataset", "reason", "type"):
            value = row.get(field)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"Line {line_number}: {field} must be a string or null")
            sample[field] = value
        samples.append(sample)
    if not samples:
        raise ValueError("Input contains no samples")
    return samples


def load_csv(path, dataset):
    """Keep CSV occurrence IDs stable when selecting a file or a directory."""
    labels = [(reason, kind) for reason, kind, prefix, _, _, _ in CASES
              if (path.stem + "-").lower().startswith(prefix.lower())]
    reason, kind = labels[0] if len(labels) == 1 else (None, None)
    samples = []
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"Empty CSV: {path}")
        # Historical exports use namespace2/message; new CSVs may use canonical names.
        columns = {}
        for field, aliases in (("error_message", ("error_message", "message")),
                               ("namespace", ("namespace", "namespace2")),
                               ("timestamp", ("timestamp",)), ("uuid", ("uuid",))):
            indices = [index for index, name in enumerate(header) if name in aliases]
            if len(indices) > 1 or (field == "error_message" and not indices):
                raise ValueError(f"CSV needs one unambiguous {field} column: {path}")
            columns[field] = indices[0] if indices else None
        for number, row in enumerate(reader, 1):
            if not row or not any(value.strip() for value in row):
                continue
            if len(row) != len(header):
                raise ValueError(f"CSV row {number} has {len(row)} columns, expected {len(header)}: {path}")
            sample = {field: row[index] if index is not None else None for field, index in columns.items()}
            if not sample["error_message"].strip():
                raise ValueError(f"CSV row {number} has an empty error message: {path}")
            # Filename avoids collisions between multiple files of the same type.
            sample.update(sample_id=f"{dataset or 'csv'}:{path.name}:{number:04d}",
                          dataset=dataset, reason=reason, type=kind)
            samples.append(sample)
    if not samples:
        raise ValueError(f"CSV contains no samples: {path}")
    return samples


def load_samples(path, dataset=None, limit_per_file=None):
    path = Path(path)
    if not path.exists():
        raise ValueError(f"Input does not exist: {path}")
    if limit_per_file is not None and (type(limit_per_file) is not int or limit_per_file < 1):
        raise ValueError("limit_per_file must be a positive integer or null")
    files = sorted(path.glob("*.csv")) if path.is_dir() else [path]
    if not files:
        raise ValueError(f"Input directory contains no CSV files: {path}")
    samples, seen = [], set()
    for file in files:
        if file.suffix.lower() == ".csv":
            rows = load_csv(file, dataset)
        elif file.suffix.lower() == ".jsonl":
            rows = load_jsonl(file)
            if dataset is not None:
                for row in rows:
                    if row["dataset"] not in (None, dataset):
                        raise ValueError(f"Configured dataset conflicts with input metadata: {file}")
                    row["dataset"] = dataset
        else:
            raise ValueError(f"Input must be CSV, JSONL or a CSV directory: {file}")
        for row in rows[:limit_per_file]:
            if row["sample_id"] in seen:
                raise ValueError(f"Duplicate sample_id across input files: {row['sample_id']}")
            seen.add(row["sample_id"])
            samples.append(row)
    return samples


def load_input_config(path):
    """Input paths are relative to their input config, independent of the shell cwd."""
    spec = json.loads(path.read_text(encoding="utf-8"))
    allowed = {"path", "dataset", "limit", "limit_per_file"}
    if not isinstance(spec, dict) or set(spec) != allowed:
        raise ValueError("Input config needs exactly path, dataset, limit, limit_per_file")
    if not isinstance(spec["path"], str) or not spec["path"].strip():
        raise ValueError("Input config path must be a nonempty string")
    if spec["dataset"] is not None and (not isinstance(spec["dataset"], str) or not spec["dataset"].strip()):
        raise ValueError("Input dataset must be a nonempty string or null")
    for field in ("limit", "limit_per_file"):
        if spec[field] is not None and (type(spec[field]) is not int or spec[field] < 1):
            raise ValueError(f"{field} must be a positive integer or null")
    spec["path"] = str((path.parent / spec["path"]).resolve())
    return spec
