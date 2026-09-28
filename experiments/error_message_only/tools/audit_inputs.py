#!/usr/bin/env python3
"""Identify Table 5.10 inputs locally; retain repeated rows and exclude reports."""

import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
FIELDS = ("namespace", "error_message", "timestamp", "uuid")
import sys
sys.path.insert(0, str(HERE))
from cases import CASES



def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(row):
    return tuple(row[field] for field in FIELDS)


def read_csv(path):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.reader(stream)
        next(reader)
        return [dict(zip(FIELDS, row[:4])) for row in reader if row]


def first_attempts(path):
    return [dict(row, source_record_index=i) for i, row in enumerate(json.loads(path.read_text()), 1)
            if row.get("attempt") == 1]


def matching_files(directory, prefix):
    return sorted(p for p in directory.glob("*.json") if p.name.lower().startswith(prefix.lower()))


def compare(expected, actual):
    left, right = Counter(map(identity, expected)), Counter(map(identity, actual))
    message_left = Counter((r["namespace"], r["error_message"]) for r in expected)
    message_right = Counter((r["namespace"], r["error_message"]) for r in actual)
    return {"first_attempt_records": len(actual), "matching_case_occurrences": sum((left & right).values()),
            "missing_case_occurrences": sum((left - right).values()),
            "extra_case_occurrences": sum((right - left).values()),
            "matching_namespace_message_occurrences": sum((message_left & message_right).values())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=HERE / "data/local/input-audit")
    parser.add_argument("--dataset1-dir", type=Path, help="Original dataset-1 CSV directory")
    parser.add_argument("--dataset2-dir", type=Path, help="Original dataset-2 CSV directory")
    args = parser.parse_args()
    input1 = args.dataset1_dir or (ROOT / "INPUT/data-4-n166" if (ROOT / "INPUT/data-4-n166").is_dir() else ROOT / "INPUT/data-4")
    input2 = args.dataset2_dir or next((p for p in (ROOT / "INPUT/data-5-refine-n168", ROOT / "INPUT/data-5-refine") if p.is_dir()), None)
    input1 = input1.resolve()
    input2 = input2.resolve() if input2 else None
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    raw4 = ROOT / "OUTPUT-server-4-azure/output-4-merge-bracket"
    mini4 = ROOT / "OUTPUT-server-4-mini-azure/output-1219-mini-fix-bracket"
    mini5 = ROOT / "OUTPUT-server-5-mini-azure/output-1220-mini-merge-bracket"
    graph5, sources = {}, {}
    graph5_record_count = 0
    for directory in ("output-5-bracket", "output-6-bracket"):
        for path in sorted((ROOT / "OUTPUT-server-5-azure" / directory).glob("*.json")):
            sources[str(path.relative_to(ROOT))] = sha(path)
            output_rows = json.loads(path.read_text())
            graph5_record_count += len(output_rows)
            for row in output_rows:
                graph5.setdefault(identity(row), set()).add(str(path.relative_to(ROOT)))
    full_inputs = {path: read_csv(path) for path in (ROOT / "full-test/data-4").glob("*.csv")}
    checks = []
    datasets = {"dataset-1": [], "dataset-2": []}
    for reason, kind, prefix, count1, count2, filename in CASES:
        check = {"reason": reason, "type": kind, "paper_dataset1_examples": count1,
                 "paper_dataset2_examples": count2}
        for number, expected in ((1, count1), (2, count2)):
            if not expected:
                continue
            dataset = f"dataset-{number}"
            if number == 1:
                local_source = ROOT / "INPUT/data-4" / filename
                local_rows = read_csv(local_source)
                candidates = [p for p in input1.glob("*.csv") if read_csv(p) == local_rows]
                if len(candidates) != 1:
                    raise ValueError(f"Expected one original dataset-1 CSV matching {local_source}")
                source = candidates[0]
                rows = read_csv(source)
                check["dataset1_input"] = str(source.relative_to(ROOT))
                check["dataset1_local_input"] = str(local_source.relative_to(ROOT))
                check["dataset1_local_bytes_equal"] = source.read_bytes() == local_source.read_bytes()
                sources[str(local_source.relative_to(ROOT))] = sha(local_source)
                check["dataset1_run_input"] = [str(p.relative_to(ROOT)) for p, values in full_inputs.items() if values == rows]
                check["dataset1_run_input_bytes_equal"] = {name: source.read_bytes() == (ROOT / name).read_bytes() for name in check["dataset1_run_input"]}
                if not check["dataset1_run_input"]:
                    raise ValueError(f"No identical ordered run input for {source}")
                for name in check["dataset1_run_input"]:
                    sources[name] = sha(ROOT / name)
                check["dataset1_output_checks"] = {}
                for model, directory in (("gpt-4o", raw4), ("gpt-4o-mini", mini4)):
                    for path in matching_files(directory, prefix):
                        sources[str(path.relative_to(ROOT))] = sha(path)
                    check["dataset1_output_checks"][model] = [
                        {"file": str(p.relative_to(ROOT)), **compare(rows, first_attempts(p))}
                        for p in matching_files(directory, prefix)]
            else:
                candidates = matching_files(mini5, prefix)
                if len(candidates) != 1:
                    raise ValueError(f"Expected one final mini output for {prefix}")
                mini_source = candidates[0]
                mini_rows = first_attempts(mini_source)
                check["dataset2_recovered_from"] = str(mini_source.relative_to(ROOT))
                sources[str(mini_source.relative_to(ROOT))] = sha(mini_source)
                if input2:
                    candidates = [p for p in input2.glob("*.csv") if (p.stem + "-").lower().startswith(prefix.lower())]
                    if len(candidates) != 1:
                        raise ValueError(f"Expected one original dataset-2 CSV for {prefix}")
                    source = candidates[0]
                    rows = read_csv(source)
                    check["dataset2_input"] = str(source.relative_to(ROOT))
                    check["dataset2_mini_multiset_comparison"] = compare(rows, mini_rows)
                    check["dataset2_mini_order_equal"] = list(map(identity, rows)) == list(map(identity, mini_rows))
                    if any(check["dataset2_mini_multiset_comparison"][k] for k in ("missing_case_occurrences", "extra_case_occurrences")):
                        raise ValueError(f"Original dataset-2 CSV differs from final mini case multiset: {source}")
                else:
                    source, rows = mini_source, mini_rows
                check["dataset2_gpt4o_exact_identity_coverage"] = sum(identity(r) in graph5 for r in rows)
                if check["dataset2_gpt4o_exact_identity_coverage"] != len(rows):
                    raise ValueError(f"Unmatched GPT-4o case in {source}")
            if len(rows) != expected:
                raise ValueError(f"{source}: expected {expected} examples, found {len(rows)}")
            sources[str(source.relative_to(ROOT))] = sha(source)
            for index, row in enumerate(rows, 1):
                sample = {"sample_id": f"{dataset}:{reason}:{kind}:{index:04d}",
                          "dataset": dataset,
                          **{field: row[field] for field in FIELDS}, "reason": reason, "type": kind,
                          "source_file": str(source.relative_to(ROOT)),
                          "source_row_or_record": index + 1 if number == 1 or input2 else row["source_record_index"],
                          "provenance": "original_csv" if number == 1 or input2 else "recovered_from_output_attempt1"}
                datasets[dataset].append(sample)
        checks.append(check)
    for name, rows in datasets.items():
        path = out / (name + (".jsonl" if name == "dataset-1" or input2 else "-recovered.jsonl"))
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")
    input_keys = set(map(identity, datasets["dataset-2"]))
    output_keys = set(graph5)
    input_messages = {row["error_message"] for row in datasets["dataset-2"]}
    output_messages = {key[1] for key in output_keys}
    manifest = {"scope": "Table 5.10 example counts and sample provenance; correctness totals are not recomputed",
                "grouping": "Preserve input occurrences; output recovery selects attempt==1, never UUID deduplication",
                "dataset1_original_directory": str(input1.relative_to(ROOT)),
                "dataset1_reported_original_location": "n166:~/k8s-llm-rca-azure/full-test/data-4",
                "dataset2_original_directory": str(input2.relative_to(ROOT)) if input2 else None,
                "dataset2_reported_original_location": "n168:~/k8s-llm-rca-azure/full-test/data-5-refine",
                "dataset2_original_verification": "All original occurrences found by full identity in GPT-4o outputs; exact multiplicity matches final mini output" if input2 else "CSV contents not yet compared",
                "dataset2_status": "Original CSV verified" if input2 else "Recovered output cases; awaiting original CSV comparison",
                "dataset2_gpt4o_global_comparison": {
                    "output_directories": ["OUTPUT-server-5-azure/output-5-bracket", "OUTPUT-server-5-azure/output-6-bracket"],
                    "output_records_including_retries": graph5_record_count,
                    "input_unique_identities": len(input_keys), "output_unique_identities": len(output_keys),
                    "missing_input_identities": len(input_keys - output_keys),
                    "extra_output_identities": len(output_keys - input_keys),
                    "input_unique_messages": len(input_messages), "output_unique_messages": len(output_messages),
                    "missing_input_messages": len(input_messages - output_messages),
                    "extra_output_messages": len(output_messages - input_messages)},
                "counts": {k: len(v) for k, v in datasets.items()},
                "event_timestamp_ranges": {k: [min(r['timestamp'] for r in v), max(r['timestamp'] for r in v)] for k, v in datasets.items()},
                "sources_sha256": sources, "case_checks": checks}
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    with (out / "counts.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["reason", "type", "dataset1_examples", "dataset2_examples", "dataset1_input", "dataset2_input", "dataset2_recovered_from"])
        for check in checks:
            writer.writerow([check["reason"], check["type"], check["paper_dataset1_examples"], check["paper_dataset2_examples"],
                             check.get("dataset1_input", ""), check.get("dataset2_input", ""), check.get("dataset2_recovered_from", "")])
    print(f"dataset-1: {len(datasets['dataset-1'])} original CSV occurrences")
    origin = "original CSV" if input2 else "recovered"
    print(f"dataset-2: {len(datasets['dataset-2'])} {origin} occurrences, all found in GPT-4o outputs")
    print(f"Audit and candidate inputs: {out}")


if __name__ == "__main__":
    main()
