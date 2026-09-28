"""Durable journals, checkpoints, and recovery plans for local batch runs."""
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

from client import sum_usage

TERMINAL = {"quality_passed", "max_attempts_reached", "dry_run"}
FAILED = {"api_error", "response_error", "quality_check_error"}


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp-" + uuid4().hex)
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def append_json(stream, value):
    stream.write(json.dumps(value, ensure_ascii=False) + "\n")
    stream.flush()
    os.fsync(stream.fileno())


def read_journal(path, repair=False):
    """Only repair a torn last line; preserve its bytes before truncating."""
    path = Path(path)
    if not path.exists():
        return []
    data = path.read_bytes()
    lines = data.splitlines(keepends=True)
    rows, offset = [], 0
    for index, line in enumerate(lines):
        try:
            value = json.loads(line.decode("utf-8")) if line.strip() else None
            if value is not None and not isinstance(value, dict):
                raise ValueError("Journal records must be JSON objects")
        except (UnicodeDecodeError, json.JSONDecodeError):
            if index != len(lines) - 1 or line.endswith(b"\n"):
                raise ValueError(f"Corrupt journal record at {path}:{index + 1}")
            if repair:
                backup = path.with_name(path.name + ".partial-" + uuid4().hex[:8])
                backup.write_bytes(data[offset:])
                with path.open("r+b") as stream:
                    stream.truncate(offset)
                    stream.flush()
                    os.fsync(stream.fileno())
                print(f"Recovered torn last line in {path.name}; retained bytes in {backup.name}", file=sys.stderr)
            break
        else:
            if value is not None:
                rows.append(value)
            offset += len(line)
    else:
        if repair and data and not data.endswith(b"\n"):
            with path.open("ab") as stream:
                stream.write(b"\n")
                stream.flush()
                os.fsync(stream.fileno())
    return rows


def latest_results(rows):
    return {(row["sample_id"], row["attempt"]): row for row in rows}


def latest_samples(rows):
    latest = {}
    for (sample_id, attempt), row in latest_results(rows).items():
        if sample_id not in latest or attempt >= latest[sample_id]["attempt"]:
            latest[sample_id] = row
    return latest


def candidate_and_quality(row):
    report = row["analysis"][0]["report"]
    candidate = {"destkind": row["destkind"], "report": {
        key: report[key] for key in ("summary", "conclusion", "resolution", "missing_information")}}
    quality = {key: report[key] for key in ("overall_score", "further_investigation", "quality_feedback")}
    return candidate, quality


def plan_sample(sample_id, rows, retry_failed=False):
    latest = latest_samples(rows).get(sample_id)
    if latest and (latest["stop_reason"] in TERMINAL or (latest["status"] in FAILED and not retry_failed)):
        return None
    if latest and latest["stop_reason"] == "quality_retry":
        previous, feedback = candidate_and_quality(latest)
        return latest["attempt"] + 1, previous, feedback
    attempt = latest["attempt"] if latest else 1
    previous_row = latest_results(rows).get((sample_id, attempt - 1))
    previous, feedback = candidate_and_quality(previous_row) if previous_row else (None, None)
    return attempt, previous, feedback


def cached_stages(requests, sample_id, attempt):
    return {row["stage"]: row for row in requests
            if row["sample_id"] == sample_id and row["attempt"] == attempt and row["status"] == "ok"}


RESULT_USAGE_EXTRAS = {
    "token_usage_details", "time_cost_details", "token_usage_scope", "request_count",
    "token_usage_complete", "time_cost_complete", "reused_request_ids", "new_request_count",
    "new_token_usage", "new_time_cost", "unknown_request_count", "cumulative_usage",
}


def account_execution(record, history):
    """Keep only selected request totals; journals and summary retain cost history."""
    for key in RESULT_USAGE_EXTRAS:
        record.pop(key, None)
    selected_ids = set(record.get("request_ids", {}).values())
    if not selected_ids:
        return
    selected = [row for row in history if row["request_id"] in selected_ids]
    if len(selected) != len(selected_ids):
        raise ValueError("Result references a request missing from requests.jsonl")
    record["token_usage"] = sum_usage(selected)
    record["time_cost"] = sum(row["time_cost"] for row in selected)


def summarize(samples, results, requests, started, dry_run):
    latest = latest_samples(results)
    completed = [row for row in latest.values() if row["stop_reason"] in TERMINAL or row["status"] in FAILED]
    calls = [row for row in requests if row["status"] != "planned"]
    known = [row for row in calls if row["token_usage"]["total_tokens"] is not None]
    completed_ids = {row.get("request_id") for row in calls}
    unknown = [row for row in started if row["request_id"] not in completed_ids]
    summary = {"dry_run": dry_run, "sample_count": len(samples), "completed_samples": len(completed),
               "pending_samples": len(samples) - len(completed), "attempt_records": len(results),
               "logical_attempts": len(latest_results(results)),
               "quality_passed_samples": sum(row["stop_reason"] == "quality_passed" for row in completed),
               "max_attempts_reached_samples": sum(row["stop_reason"] == "max_attempts_reached" for row in completed),
               "failed_samples": sum(row["status"] in FAILED for row in completed),
               "request_count": len(calls), "usage_reported_request_count": len(known),
               "reported_total_tokens": sum(row["token_usage"]["total_tokens"] for row in known),
               "total_time_cost": sum(row["time_cost"] for row in calls),
               "unknown_request_count": len(unknown), "unknown_requests": unknown,
               "completed": len(completed) == len(samples),
               "time_cost_complete": not unknown,
               "token_usage_complete": len(known) == len(calls) and not unknown}
    for status in ("ok", "api_error", "response_error", "quality_check_error", "planned"):
        summary[status] = sum(row["status"] == status for row in results)
    return summary
