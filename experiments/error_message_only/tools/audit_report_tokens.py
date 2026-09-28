#!/usr/bin/env python3
"""Estimate old saved report lengths by re-tokenizing their JSON, without model calls.

Requires the optional tiktoken package and its o200k_base encoding cache.
The original model response formatting is not retained; these are estimates.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parents[1]
SOURCES = {
    "dataset-1": ["OUTPUT-server-4-azure/output-4-merge-bracket"],
    "dataset-2": ["OUTPUT-server-5-azure/output-5-bracket", "OUTPUT-server-5-azure/output-6-bracket"],
}


def reports(value, path=""):
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = path + "." + key
            if key == "report" and isinstance(child, dict):
                yield child_path, child
            else:
                yield from reports(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from reports(child, path + "[" + str(index) + "]")


def stats(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    result = {"count": len(ordered), "mean": round(sum(ordered) / len(ordered), 1),
              "max": ordered[-1], "over_1536": sum(v > 1536 for v in ordered),
              "over_4096": sum(v > 4096 for v in ordered)}
    for percentile in (50, 90, 95, 99):
        result["p" + str(percentile)] = ordered[math.ceil(len(ordered) * percentile / 100) - 1]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=HERE / "data/local/report-token-audit.json")
    args = parser.parse_args()
    import tiktoken
    encoding = tiktoken.get_encoding("o200k_base")
    result = {"encoding": encoding.name, "note": "Retokenized saved report JSON; not original completion usage.",
              "sources": {}, "datasets": {}}
    for dataset, directories in SOURCES.items():
        items, rows_count, first_rows = [], 0, 0
        for directory in directories:
            for file in sorted((ROOT / directory).glob("*.json")):
                relative = str(file.relative_to(ROOT))
                result["sources"][relative] = hashlib.sha256(file.read_bytes()).hexdigest()
                for row_index, row in enumerate(json.loads(file.read_text(encoding="utf-8")), 1):
                    rows_count += 1
                    first_rows += row.get("attempt") == 1
                    for report_path, report in reports(row):
                        lengths = {}
                        for format_name, options in (("compact", {"separators": (",", ":")}),
                                                     ("normal", {}), ("indent2", {"indent": 2})):
                            text = json.dumps(report, ensure_ascii=False, **options)
                            lengths[format_name] = len(encoding.encode(text, disallowed_special=()))
                        items.append({"file": relative, "row": row_index, "path": report_path,
                                      "attempt": row.get("attempt"), "tokens": lengths,
                                      "complete": all(key in report for key in ("summary", "conclusion", "resolution"))})
        result["datasets"][dataset] = {
            "output_records": rows_count, "first_attempt_records": first_rows,
            "all_reports": {fmt: stats([item["tokens"][fmt] for item in items])
                            for fmt in ("compact", "normal", "indent2")},
            "first_attempt_reports_indent2": stats([item["tokens"]["indent2"] for item in items if item["attempt"] == 1]),
            "complete_reports_indent2": stats([item["tokens"]["indent2"] for item in items if item["complete"]]),
            "longest_reports": sorted(items, key=lambda item: item["tokens"]["indent2"], reverse=True)[:5],
        }
        print(dataset, json.dumps(result["datasets"][dataset], ensure_ascii=False, indent=2))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("Audit:", args.output)


if __name__ == "__main__":
    main()
