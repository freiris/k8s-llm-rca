"""Inspect a hosted service's model claims with one small completion request."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
import uuid

EXPERIMENT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(EXPERIMENT))
from client import create_client

SAFE_HEADERS = {
    "x-request-id", "request-id", "x-ms-request-id", "apim-request-id",
    "x-litellm-call-id", "x-litellm-model-id", "x-litellm-model-group",
    "x-litellm-model-name", "x-litellm-attempted-fallbacks", "x-litellm-attempted-retries",
    "llm_provider-x-request-id", "llm_provider-x-client-request-id",
    "x-model", "x-model-version", "x-deployment-name",
}


def inspect_response(response, requested_model):
    # Read HTTP JSON directly to retain gateway fields omitted by typed SDK models.
    body = response.json()
    choices = body.get("choices") or []
    return {
        "status": "ok",
        "http_status": response.status_code,
        "requested_model": requested_model,
        "returned_model": body.get("model"),
        "model_name_matches": body.get("model") == requested_model,
        "backend_identity_verified": False,
        "identity_note": "Response metadata is the service's claim. Confirm the alias routing, "
                         "upstream model/version and any fallback with the service administrator.",
        "response_id": body.get("id"),
        "system_fingerprint": body.get("system_fingerprint"),
        "response_headers": {k.lower(): v for k, v in response.headers.items()
                             if k.lower() in SAFE_HEADERS},
        "response_header_names": sorted(response.headers.keys()),
        "usage": body.get("usage"),
        "finish_reason": choices[0].get("finish_reason") if choices else None,
        "raw_response": body,
    }


def probe(client, model, max_tokens):
    started = time.perf_counter()
    try:
        response = client.chat.completions.with_raw_response.create(
            model=model, messages=[{"role": "user", "content": "Reply with the single word OK."}],
            max_tokens=max_tokens,
        )
        result = inspect_response(response.http_response, model)
    except Exception as exc:
        # Do not serialize exception text: gateways may echo credentials or URLs.
        result = {"status": "api_error", "requested_model": model,
                  "backend_identity_verified": False, "error_type": type(exc).__name__}
        response = getattr(exc, "response", None)
        if response is not None:
            result["http_status"] = response.status_code
            result["response_headers"] = {k.lower(): v for k, v in response.headers.items()
                                          if k.lower() in SAFE_HEADERS}
    result["time_cost"] = time.perf_counter() - started
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=EXPERIMENT / "configs/gpt4o.json")
    parser.add_argument("--model", help="Override the model name in the config")
    parser.add_argument("--output-dir", type=Path, default=EXPERIMENT / "runs/model-probe")
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without calling the API")
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.max_tokens <= 0:
        parser.error("--timeout and --max-tokens must be positive")
    model = args.model or json.loads(args.config.read_text(encoding="utf-8"))["model"]
    if args.dry_run:
        print(json.dumps({"requested_model": model, "max_tokens": args.max_tokens,
                          "timeout_seconds": args.timeout, "completion_requests": 1,
                          "output_dir": str(args.output_dir.resolve())}, indent=2))
        return 0
    with create_client(args.timeout) as client:
        result = probe(client, model, args.max_tokens)
    name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    run_dir = args.output_dir.resolve() / name
    run_dir.mkdir(parents=True, exist_ok=False)
    output = run_dir / "model_probe.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items()
                      if k not in {"raw_response", "response_header_names"}},
                     ensure_ascii=False, indent=2))
    print("Saved:", output)
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
