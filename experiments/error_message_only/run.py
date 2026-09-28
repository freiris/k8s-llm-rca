#!/usr/bin/env python3
"""Generate and check message-only reports, revising at most three times."""

import argparse
import fcntl
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone
from uuid import uuid4

from checker import MessageOnlyReportQualityChecker
from client import build_messages, create_client, normalize_usage, parse_report, request_json, sum_usage
from dataset import load_input_config, load_samples
from pretty_json import export_jsonl
from progress import (account_execution, append_json, atomic_json, cached_stages, plan_sample, read_journal, summarize)

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
METADATA = ("sample_id", "dataset", "reason", "type", "error_message", "namespace", "timestamp", "uuid")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, data):
    atomic_json(path, data)


def git_value(*args):
    try:
        return subprocess.check_output(["git", "-C", str(REPO), *args], stderr=subprocess.DEVNULL).decode().strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def validate_parameters(params):
    if not isinstance(params, dict) or set(params) - {"temperature", "max_tokens", "max_completion_tokens", "seed"}:
        raise ValueError("Unsupported request parameters; see README")
    if "max_tokens" in params and "max_completion_tokens" in params:
        raise ValueError("Choose max_tokens or max_completion_tokens, not both")
    for key in ("max_tokens", "max_completion_tokens", "seed"):
        if key in params and (type(params[key]) is not int or (key != "seed" and params[key] < 1)):
            raise ValueError(f"Invalid {key}")
    if "temperature" in params:
        value = params["temperature"]
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 2:
            raise ValueError("temperature must be a number between 0 and 2")


def load_config(path, model=None, checker_model=None, max_attempts=None):
    config = json.loads(path.read_text(encoding="utf-8"))
    allowed = {"model", "prompt_file", "checker_model", "checker_prompt_file", "request_parameters",
               "checker_request_parameters", "timeout_seconds", "max_attempts"}
    if not isinstance(config, dict) or not allowed <= set(config) or set(config) - allowed - {"input_config", "output_dir"}:
        raise ValueError(f"Config needs: {', '.join(sorted(allowed))}; optional input_config, output_dir")
    for field in ("input_config", "output_dir"):
        if field in config and (not isinstance(config[field], str) or not config[field].strip()):
            raise ValueError(f"{field} must be a nonempty string")
    for key, override in (("model", model), ("checker_model", checker_model), ("max_attempts", max_attempts)):
        if override is not None:
            config[key] = override
    for key in ("model", "prompt_file", "checker_prompt_file"):
        if not isinstance(config[key], str) or not config[key].strip():
            raise ValueError(f"{key} must be a nonempty string")
    if config["checker_model"] is not None and (not isinstance(config["checker_model"], str) or not config["checker_model"].strip()):
        raise ValueError("checker_model must be a nonempty string or null")
    if type(config["max_attempts"]) is not int or not 1 <= config["max_attempts"] <= 3:
        raise ValueError("max_attempts must be an integer from 1 to 3")
    timeout = config["timeout_seconds"]
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout_seconds must be a positive finite number")
    validate_parameters(config["request_parameters"])
    validate_parameters(config["checker_request_parameters"])
    prompts = {}
    for name, key in (("generation", "prompt_file"), ("quality_check", "checker_prompt_file")):
        prompt = (path.parent / config[key]).resolve().read_text(encoding="utf-8")
        if not prompt.strip():
            raise ValueError(f"{key} is empty")
        prompts[name] = prompt
    return config, prompts


def run_attempt(sample, attempt, config, prompts, client, previous, feedback, on_request, dry_run=False,
                cache=None, on_start=None):
    record = {key: sample.get(key) for key in METADATA}
    record.update(schema_version="1.0", method="error_message_only", attempt=attempt,
                  destkind=None, analysis=[], token_usage=normalize_usage(None))
    messages = build_messages(prompts["generation"], sample["error_message"], previous, feedback)
    if dry_run:
        on_request("report_generation", {"model": config["model"], "messages": messages,
                   "status": "planned", "time_cost": 0.0, "token_usage": normalize_usage(None)})
        record.update(status="planned", stop_reason="dry_run", time_cost=0.0)
        return record, None, None
    cache = cache or {}
    def call_stage(stage, call):
        if stage in cache:
            return cache[stage]
        request_id = on_start(stage) if on_start else uuid4().hex
        result = call()
        result["request_id"] = request_id
        on_request(stage, result)
        return result
    stages = []
    generated = call_stage("report_generation", lambda: request_json(
        client, config["model"], config["request_parameters"], messages, parse_report))
    stages.append(generated)
    candidate = quality = checked = None
    if generated["status"] != "ok":
        record.update(status=generated["status"], stop_reason="generator_error", error=generated["error"])
    else:
        candidate = generated["data"]
        report = dict(candidate["report"], overall_score=None, further_investigation=None, quality_feedback=None)
        record.update(destkind=candidate["destkind"], analysis=[{"report": report}])
        checked = call_stage("quality_check", lambda: MessageOnlyReportQualityChecker(
            client, config, prompts["quality_check"]).check(sample["error_message"], candidate))
        stages.append(checked)
        if checked["status"] != "ok":
            record.update(status="quality_check_error", stop_reason="checker_error", error=checked["error"],
                          checker_error_status=checked["status"])
        else:
            quality = checked["data"]
            report.update(quality)
            stop_reason = "quality_passed" if not quality["further_investigation"] else (
                "max_attempts_reached" if attempt == config["max_attempts"] else "quality_retry")
            record.update(status="ok", stop_reason=stop_reason)
    record["token_usage"] = sum_usage(stages)
    record["time_cost"] = sum(stage["time_cost"] for stage in stages)
    record["request_ids"] = {stage: details["request_id"] for stage, details in
                             (("report_generation", generated), ("quality_check", checked))
                             if details is not None}
    return record, candidate, quality


def prepare_inputs(args):
    config, prompts = load_config(args.config.resolve(), args.model, args.checker_model, args.max_attempts)
    selection = {"path": None, "dataset": args.dataset, "limit": None, "limit_per_file": None}
    input_config = args.input_config
    if input_config is None and args.input is None and args.message is None and "input_config" in config:
        input_config = args.config.resolve().parent / config["input_config"]
    if input_config is not None:
        selection = load_input_config(input_config.resolve())
    if args.input is not None:
        selection["path"] = str(args.input.resolve())
    for field in ("dataset", "limit", "limit_per_file"):
        override = getattr(args, field)
        if override is not None:
            selection[field] = override
    for field in ("limit", "limit_per_file"):
        if selection[field] is not None and selection[field] < 1:
            raise ValueError(f"{field} must be positive")
    if args.message is not None:
        if not args.message.strip():
            raise ValueError("Message must not be empty")
        samples = [{"sample_id": "single", "error_message": args.message, "dataset": args.dataset}]
    else:
        if selection["path"] is None:
            raise ValueError("Choose --input, --input-config, --message or input_config in model config")
        samples = load_samples(selection["path"], selection["dataset"], selection["limit_per_file"])
    samples = [{key: row.get(key) for key in METADATA} for row in samples[:selection["limit"]]]
    source_path = Path(selection["path"]) if selection["path"] else None
    source_files = sorted(source_path.glob("*.csv")) if source_path and source_path.is_dir() else ([source_path] if source_path else [])
    source_hashes = {str(file): digest(file.read_bytes()) for file in source_files}
    return config, prompts, samples, selection, input_config, source_path, source_hashes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=HERE / "configs/gpt4o.json")
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--input", type=Path, help="CSV, JSONL or a directory of CSV files")
    inputs.add_argument("--input-config", type=Path, help="Input path and sample selection config")
    inputs.add_argument("--message", help="One error message")
    parser.add_argument("--dataset", help="Dataset label for CSVs or unlabeled JSONL")
    parser.add_argument("--model", help="Override report model; also changes checker if checker_model is null")
    parser.add_argument("--checker-model", help="Override checker model")
    parser.add_argument("--max-attempts", type=int, help="Report/check rounds, from 1 to 3")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--limit-per-file", type=int)
    parser.add_argument("--output-dir", type=Path, help="Override configured output directory")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", type=Path, help="Continue an existing run directory using its saved inputs and settings")
    parser.add_argument("--retry-failed", action="store_true", help="With --resume, retry recorded technical failures")
    args = parser.parse_args(argv)
    if args.retry_failed and args.resume is None:
        parser.error("--retry-failed requires --resume")
    if args.resume is not None:
        forbidden = (args.input, args.input_config, args.message, args.dataset, args.model,
                     args.checker_model, args.max_attempts, args.limit, args.limit_per_file, args.output_dir)
        if any(value is not None for value in forbidden) or args.config.resolve() != (HERE / "configs/gpt4o.json").resolve():
            parser.error("--resume uses saved inputs/settings; do not pass input, model, config or output overrides")
    try:
        if args.resume is None:
            config, prompts, samples, selection, input_config, source_path, source_hashes = prepare_inputs(args)
            output_dir = args.output_dir or (args.config.resolve().parent / config["output_dir"] if "output_dir" in config else HERE / "runs")
            run_dir = output_dir.resolve() / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8])
            run_dir.mkdir(parents=True)
        else:
            run_dir = args.resume.resolve()
            if not run_dir.is_dir():
                raise ValueError(f"Run directory does not exist: {run_dir}")
        with (run_dir / ".run.lock").open("a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError("This run is already active in another process")
            if args.resume is None:
                manifest = new_manifest(args, config, prompts, samples, selection, input_config,
                                        source_path, source_hashes, output_dir)
                write_json(run_dir / "manifest.json", manifest)
                write_json(run_dir / "config.json", config)
                (run_dir / "prompt.txt").write_text(prompts["generation"], encoding="utf-8")
                (run_dir / "checker_prompt.txt").write_text(prompts["quality_check"], encoding="utf-8")
                with (run_dir / "samples.jsonl").open("w", encoding="utf-8") as snapshot:
                    snapshot.write("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in samples))
                    snapshot.flush()
                    os.fsync(snapshot.fileno())
            else:
                manifest, config, prompts, samples = load_saved_run(run_dir)
            return execute_run(run_dir, manifest, config, prompts, samples, args)
    except (ValueError, OSError, ImportError) as exc:
        parser.error(str(exc))


def code_hashes():
    return {name: digest((HERE / name).read_bytes()) for name in
            ("run.py", "client.py", "checker.py", "dataset.py", "cases.py", "pretty_json.py", "progress.py")}


def new_manifest(args, config, prompts, samples, selection, input_config, source_path, source_hashes, output_dir):
    try:
        sdk_version = importlib.metadata.version("openai")
    except importlib.metadata.PackageNotFoundError:
        sdk_version = None
    return {
        "schema_version": "1.0", "resume_protocol_version": 1, "method": "error_message_only",
        "attempt_policy": "checker_feedback_revision", "started_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": args.dry_run, "config": config,
        "checker_model_resolved": config["checker_model"] or config["model"], "sdk_max_retries": 0,
        "base_url": os.environ.get("OPENAI_BASE_URL"), "openai_version": sdk_version, "python_version": sys.version,
        "git_commit": git_value("rev-parse", "HEAD"), "git_branch": git_value("symbolic-ref", "--short", "HEAD"),
        "tracked_changes": git_value("status", "--porcelain", "--untracked-files=no"),
        "sample_count": len(samples), "max_model_requests": len(samples) * config["max_attempts"] * 2,
        "selected_input_sha256": digest(json.dumps(samples, ensure_ascii=False, sort_keys=True).encode()),
        "input_selection": selection, "input_config_file": str(input_config.resolve()) if input_config else None,
        "output_dir_resolved": str(output_dir.resolve()), "input_source_sha256": source_hashes,
        "input_file_sha256": source_hashes.get(str(source_path)) if source_path and source_path.is_file() else None,
        "prompt_sha256": {name: digest(prompt.encode()) for name, prompt in prompts.items()},
        "code_sha256": code_hashes(),
    }


def load_saved_run(run_dir):
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("resume_protocol_version") != 1:
        raise ValueError("This old run has no resumable input snapshot; start a new run with the current runner")
    if manifest.get("dry_run"):
        raise ValueError("A planned dry-run cannot be resumed as a live experiment; start a new run")
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    if config != manifest["config"]:
        raise ValueError("Saved config differs from the original manifest")
    prompts = {"generation": (run_dir / "prompt.txt").read_text(encoding="utf-8"),
               "quality_check": (run_dir / "checker_prompt.txt").read_text(encoding="utf-8")}
    if {key: digest(value.encode()) for key, value in prompts.items()} != manifest["prompt_sha256"]:
        raise ValueError("Saved prompts differ from the original manifest")
    samples = load_samples(run_dir / "samples.jsonl")
    if digest(json.dumps(samples, ensure_ascii=False, sort_keys=True).encode()) != manifest["selected_input_sha256"]:
        raise ValueError("Saved input snapshot differs from the original manifest")
    return manifest, config, prompts, samples


def execute_run(run_dir, manifest, config, prompts, samples, args):
    repair = not (args.resume and args.dry_run)
    results = read_journal(run_dir / "results.jsonl", repair)
    requests = read_journal(run_dir / "requests.jsonl", repair)
    started = read_journal(run_dir / "request_starts.jsonl", repair)
    if args.resume and args.dry_run:
        summary = summarize(samples, results, requests, started, manifest["dry_run"])
        summary["samples_to_run"] = sum(plan_sample(sample["sample_id"], results, args.retry_failed) is not None for sample in samples)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    remaining = [sample for sample in samples if plan_sample(sample["sample_id"], results, args.retry_failed) is not None]
    if args.resume and remaining and os.environ.get("OPENAI_BASE_URL") != manifest["base_url"]:
        raise ValueError("OPENAI_BASE_URL differs from the original run; resume must use the same service")
    needs_client = False
    for sample in remaining:
        attempt, _, _ = plan_sample(sample["sample_id"], results, args.retry_failed)
        cache = cached_stages(requests, sample["sample_id"], attempt)
        if not ("report_generation" in cache and "quality_check" in cache and
                (not cache["quality_check"]["data"]["further_investigation"] or attempt == config["max_attempts"])):
            needs_client = True
    client = create_client(config["timeout_seconds"]) if needs_client and not manifest["dry_run"] else None
    cursor = None
    interrupted = False
    for name in ("results.jsonl", "requests.jsonl", "request_starts.jsonl"):
        (run_dir / name).touch(exist_ok=True)
    def checkpoint():
        summary = summarize(samples, results, requests, started, manifest["dry_run"])
        summary["updated_at"] = datetime.now(timezone.utc).isoformat()
        write_json(run_dir / "progress.json", dict(summary, current=cursor))
        write_json(run_dir / "summary.json", summary)
        return summary
    try:
        with (run_dir / "sessions.jsonl").open("a", encoding="utf-8") as sessions:
            append_json(sessions, {"started_at": datetime.now(timezone.utc).isoformat(),
                                   "resume": bool(args.resume), "retry_failed": args.retry_failed,
                                   "samples_to_run": len(remaining), "code_sha256": code_hashes()})
        summary = checkpoint()
        if summary["unknown_request_count"]:
            print(f"Recovery: {summary['unknown_request_count']} started requests have no saved response; their usage is unknown.")
        print(f"Run directory: {run_dir}", flush=True)
        print(f"Samples to run: {len(remaining)} / {len(samples)}", flush=True)
        with (run_dir / "results.jsonl").open("a", encoding="utf-8") as output, (run_dir / "requests.jsonl").open("a", encoding="utf-8") as request_log, (run_dir / "request_starts.jsonl").open("a", encoding="utf-8") as starts:
            for sample in remaining:
                attempt, previous, feedback = plan_sample(sample["sample_id"], results, args.retry_failed)
                while attempt <= config["max_attempts"]:
                    def on_start(stage):
                        nonlocal cursor
                        entry = {"sample_id": sample["sample_id"], "attempt": attempt, "stage": stage,
                                 "request_id": uuid4().hex, "started_at": datetime.now(timezone.utc).isoformat()}
                        append_json(starts, entry)
                        started.append(entry)
                        cursor = entry
                        checkpoint()
                        return entry["request_id"]
                    def on_request(stage, details):
                        nonlocal cursor
                        entry = {"sample_id": sample["sample_id"], "attempt": attempt, "stage": stage, **details}
                        append_json(request_log, entry)
                        requests.append(entry)
                        cursor = None
                        checkpoint()
                    cache = cached_stages(requests, sample["sample_id"], attempt)
                    record, previous, feedback = run_attempt(
                        sample, attempt, config, prompts, client, previous, feedback, on_request,
                        manifest["dry_run"], cache, on_start)
                    history = [row for row in requests if row["sample_id"] == sample["sample_id"] and
                               row["attempt"] == attempt and row["status"] != "planned"]
                    account_execution(record, history)
                    record["execution"] = 1 + sum(row["sample_id"] == sample["sample_id"] and row["attempt"] == attempt for row in results)
                    append_json(output, record)
                    results.append(record)
                    cursor = None
                    checkpoint()
                    print(f"{sample['sample_id']} attempt={attempt}: {record['status']} ({record['stop_reason']})", flush=True)
                    if record["stop_reason"] != "quality_retry":
                        break
                    attempt += 1
    except KeyboardInterrupt:
        interrupted = True
        print("Interrupted; saved progress can be continued with --resume.")
    finally:
        summary = checkpoint()
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        summary["interrupted"] = interrupted
        write_json(run_dir / "summary.json", summary)
        if client is not None:
            client.close()
        export_jsonl(run_dir / "results.jsonl")
        export_jsonl(run_dir / "requests.jsonl")
    print(f"Results: {run_dir}")
    return 130 if interrupted else (1 if summary["failed_samples"] else 0)


if __name__ == "__main__":
    sys.exit(main())
