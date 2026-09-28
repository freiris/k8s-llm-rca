#!/usr/bin/env python3
"""Run each CSV in a directory as a separate message-only experiment."""

import argparse
import fcntl
import json
from pathlib import Path
import shlex
import signal
import subprocess
import sys

from dataset import load_samples
from run import HERE, load_config, load_saved_run


def find_resume_run(output_dir, input_file, dataset):
    """Choose the latest live run for this input, never a dry-run directory."""
    candidates = []
    for directory in sorted(output_dir.iterdir()) if output_dir.is_dir() else []:
        if not directory.is_dir() or not (directory / "manifest.json").exists():
            continue
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("dry_run"):
            continue
        selection = manifest.get("input_selection", {})
        if selection.get("path") != str(input_file):
            raise ValueError(f"Existing run {directory} uses another input path; choose a separate output directory")
        if dataset is not None and selection.get("dataset") != dataset:
            raise ValueError(f"Dataset label differs from saved run {directory}")
        candidates.append((manifest["started_at"], directory.name, directory))
    if not candidates:
        return None
    directory = max(candidates)[2]
    load_saved_run(directory)  # Validate the snapshots before starting any child.
    return directory


def build_jobs(args):
    input_dir = args.input_dir.resolve()
    if not input_dir.is_dir():
        raise ValueError(f"Input directory does not exist: {input_dir}")
    files = sorted(path for path in input_dir.iterdir() if path.is_file() and path.suffix.lower() == ".csv")
    if not files:
        raise ValueError(f"No CSV files directly inside {input_dir}")
    names = [path.stem.casefold() for path in files]
    if len(set(names)) != len(names):
        raise ValueError("CSV filenames would share an output directory; rename the colliding files")
    jobs = []
    for path in files:
        destination = args.output_dir.resolve() / path.stem
        resumed = find_resume_run(destination, path.resolve(), args.dataset) if args.resume else None
        command = [sys.executable, "-u", str(HERE / "run.py")]
        if resumed is not None:
            command.extend(["--resume", str(resumed)])
            if args.retry_failed:
                command.append("--retry-failed")
            count = None
        else:
            # Validate every new file before issuing any model request.
            samples = load_samples(path, args.dataset, args.limit_per_file)
            count = len(samples)
            command.extend(["--input", str(path.resolve()), "--output-dir", str(destination)])
            if args.dataset is not None:
                command.extend(["--dataset", args.dataset])
            if args.config is not None:
                command.extend(["--config", str(args.config.resolve())])
            if args.limit_per_file is not None:
                command.extend(["--limit", str(args.limit_per_file)])
        jobs.append({"file": path.name, "output_dir": destination, "resume_dir": resumed,
                     "sample_count": count, "command": command})
    if any(job["resume_dir"] is None for job in jobs):
        load_config((args.config or HERE / "configs/gpt4o.json").resolve())
    return jobs


def run_command(command):
    # Argument lists preserve paths with spaces and avoid shell expansion.
    # Forward Ctrl-C once, so the child's finally block can save progress safely.
    child = subprocess.Popen(command, start_new_session=True)
    try:
        return child.wait()
    except KeyboardInterrupt:
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
        try:
            child.wait(timeout=20)
        except subprocess.TimeoutExpired:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        raise


def execute_jobs(jobs):
    failed = []
    for index, job in enumerate(jobs, 1):
        print(f"\n[{index}/{len(jobs)}] Running {job['file']}", flush=True)
        code = run_command(job["command"])
        if code in (130, -signal.SIGINT):
            print("Interrupted; remaining files were not started. Use --resume to continue.", flush=True)
            return 130
        if code != 0:
            failed.append(job["file"])
            print(f"File returned exit code {code}; continuing with the next file.", flush=True)
    print(f"\nFiles processed: {len(jobs)}; files with errors: {len(failed)}", flush=True)
    if failed:
        print("Files with errors: " + ", ".join(failed), flush=True)
    return 1 if failed else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dataset", help="Optional label, for example dataset-1")
    parser.add_argument("--config", type=Path, help="Override the default model config for new runs; resumed runs use saved settings")
    parser.add_argument("--limit-per-file", type=int, help="Use only the first N samples of each new CSV run; resumed runs use saved inputs")
    parser.add_argument("--dry-run", action="store_true", help="List files and commands without creating output or calling models")
    parser.add_argument("--resume", action="store_true", help="Resume the latest live run per file; start files with no previous live run")
    parser.add_argument("--retry-failed", action="store_true", help="With --resume, also retry saved technical failures")
    args = parser.parse_args(argv)
    if args.limit_per_file is not None and args.limit_per_file < 1:
        parser.error("--limit-per-file must be positive")
    if args.retry_failed and not args.resume:
        parser.error("--retry-failed requires --resume")
    try:
        if args.dry_run:
            jobs = build_jobs(args)
            print_jobs(jobs)
            print("Preview only; no output directories created and no model requests sent.")
            return 0
        root = args.output_dir.resolve()
        root.mkdir(parents=True, exist_ok=True)
        with (root / ".directory.lock").open("a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError("Another directory runner is already using this output directory")
            jobs = build_jobs(args)
            print_jobs(jobs)
            return execute_jobs(jobs)
    except KeyboardInterrupt:
        print("\nInterrupted; use --resume to continue the latest runs and start remaining files.")
        return 130
    except (OSError, ValueError, ImportError) as exc:
        parser.error(str(exc))


def print_jobs(jobs):
    print(f"CSV files: {len(jobs)}", flush=True)
    for index, job in enumerate(jobs, 1):
        mode = "resume" if job["resume_dir"] is not None else f"new run, {job['sample_count']} samples"
        print(f"[{index}/{len(jobs)}] {job['file']} ({mode})", flush=True)
        print("  Output: " + str(job["resume_dir"] or job["output_dir"]), flush=True)
        print("  " + " ".join(shlex.quote(part) for part in job["command"]), flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
