#!/usr/bin/env python3
"""Copy each file's latest live results.json into a single export directory."""

import argparse
import fcntl
import json
from pathlib import Path
import sys
from uuid import uuid4


def latest_run(parent):
    candidates = []
    for directory in sorted(parent.iterdir()):
        if not directory.is_dir():
            continue
        manifest_path = directory / "manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("dry_run"):
                continue
            started = manifest.get("started_at", directory.name)
        elif (directory / "results.json").exists():
            started = directory.name
        else:
            continue
        candidates.append((started, directory.name, directory))
    return max(candidates)[2] if candidates else None


def copy_result(run_dir, destination, overwrite=False):
    # A shared lock prevents collecting a stale JSON mirror during resume.
    lock_path = run_dir / ".run.lock"
    lock = lock_path.open("r") if lock_path.exists() else None
    try:
        if lock is not None:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError(f"Run is active: {run_dir}")
        source = run_dir / "results.json"
        data = source.read_bytes()
        if not isinstance(json.loads(data), list):
            raise ValueError(f"Expected a JSON result array: {source}")
        if source.resolve() == destination.resolve():
            raise ValueError("Export destination must differ from the source")
        if destination.exists():
            if destination.read_bytes() == data:
                return "unchanged"
            if not overwrite:
                raise ValueError(f"Export already exists with different content: {destination}; use --overwrite to refresh")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".tmp-" + uuid4().hex)
        try:
            temporary.write_bytes(data)
            temporary.replace(destination)
        finally:
            if temporary.exists():
                temporary.unlink()
        return "copied"
    finally:
        if lock is not None:
            lock.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True, help="Batch output root containing per-CSV folders")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for <per-CSV-folder>.json copies")
    parser.add_argument("--dry-run", action="store_true", help="Show sources and destinations without copying")
    parser.add_argument("--overwrite", action="store_true", help="Refresh existing exported files whose content has changed")
    args = parser.parse_args(argv)
    root, output = args.input_dir.resolve(), args.output_dir.resolve()
    if not root.is_dir():
        parser.error(f"Input directory does not exist: {root}")
    if root == output:
        parser.error("Choose a separate output directory")
    try:
        parents = [path for path in sorted(root.iterdir()) if path.is_dir() and path.resolve() != output]
        names = [path.name.casefold() for path in parents]
        if len(names) != len(set(names)):
            raise ValueError("Input folder names would produce colliding export filenames")
        copied = unchanged = skipped = found = 0
        for parent in parents:
            run_dir = latest_run(parent)
            if run_dir is None:
                continue
            found += 1
            destination = output / (parent.name + ".json")
            source = run_dir / "results.json"
            if not source.is_file():
                print(f"Skipped {parent.name}: latest run has no results.json ({run_dir.name})", file=sys.stderr)
                skipped += 1
                continue
            if args.dry_run:
                print(f"{source} -> {destination}")
                continue
            try:
                status = copy_result(run_dir, destination, args.overwrite)
            except (OSError, ValueError) as exc:
                print(f"Skipped {parent.name}: {exc}", file=sys.stderr)
                skipped += 1
                continue
            copied += status == "copied"
            unchanged += status == "unchanged"
            print(f"{parent.name}.json: {status} (from {run_dir.name})")
        if not found:
            raise ValueError("No per-file run directories found; use the batch output root as --input-dir")
        if args.dry_run:
            print(f"Previewed {found - skipped} files; skipped {skipped}. No files copied.")
        else:
            print(f"Copied: {copied}; unchanged: {unchanged}; skipped: {skipped}\nOutput: {output}")
        return 1 if skipped else 0
    except (OSError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
