#!/usr/bin/env python3
"""Rebuild compact result usage from saved request IDs; never make model requests."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from pretty_json import export_jsonl
from progress import account_execution, read_journal


def repair_usage(run_dir):
    run_dir = Path(run_dir).resolve()
    with (run_dir / ".run.lock").open("a") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Run is active; stop it before rebuilding usage")
        source = run_dir / "results.jsonl"
        results = read_journal(source)
        requests = read_journal(run_dir / "requests.jsonl")
        request_positions = {row.get("request_id"): index for index, row in enumerate(requests)}
        for record in results:
            ids = record.get("request_ids", {})
            if not ids:
                if record["status"] != "planned":
                    raise ValueError("This legacy result has no request IDs; usage cannot be rebuilt reliably")
                account_execution(record, [])
                continue
            if any(request_id not in request_positions for request_id in ids.values()):
                raise ValueError("Result references a missing request")
            cutoff = max(request_positions[request_id] for request_id in ids.values())
            history = [row for row in requests[:cutoff + 1] if row["sample_id"] == record["sample_id"] and
                       row["attempt"] == record["attempt"] and row["status"] != "planned"]
            account_execution(record, history)
        backup = source.with_name(source.name + ".before-usage-fix-" + uuid4().hex[:8])
        backup.write_bytes(source.read_bytes())
        temporary = source.with_name(source.name + ".tmp-" + uuid4().hex)
        try:
            with temporary.open("w", encoding="utf-8") as stream:
                for record in results:
                    stream.write(json.dumps(record, ensure_ascii=False) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(source)
        finally:
            if temporary.exists():
                temporary.unlink()
        export_jsonl(source)
        return backup


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()
    try:
        backup = repair_usage(args.run_dir)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(f"Rebuilt results.jsonl and results.json; original backup: {backup}")


if __name__ == "__main__":
    main()
