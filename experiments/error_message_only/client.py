"""Model calls and report validation for the message-only workflow."""

import json
import os
import time

TOKEN_FIELDS = ("prompt_tokens", "completion_tokens", "total_tokens")


def build_messages(prompt, error_message, previous=None, feedback=None):
    messages = [{"role": "system", "content": prompt},
                {"role": "user", "content": error_message}]
    if previous is not None:
        messages.extend([
            {"role": "assistant", "content": json.dumps(previous, ensure_ascii=False)},
            {"role": "user", "content": "Revise this report using the original error message and the quality feedback below. "
             "No new cluster evidence is available. Address the specific deficiencies without inventing facts.\n"
             + json.dumps(feedback, ensure_ascii=False)},
        ])
    return messages


def create_client(timeout_seconds):
    for name in ("OPENAI_API_KEY", "OPENAI_BASE_URL"):
        if not os.environ.get(name):
            raise ValueError(f"Set {name} before making live requests")
    from openai import OpenAI
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"], base_url=os.environ["OPENAI_BASE_URL"],
                  timeout=timeout_seconds, max_retries=0)


def parse_json(text):
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Response has no textual content")
    text = text.strip()
    if text.startswith("```json\n") and text.endswith("```"):
        text = text[8:-3].strip()
    elif text.startswith("```\n") and text.endswith("```"):
        text = text[4:-3].strip()
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("Response must be a JSON object")
    return value


def nonempty_string(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")


def parse_report(text):
    value = parse_json(text)
    if set(value) != {"destkind", "report"}:
        raise ValueError("Generated response must contain destkind and report only")
    if value["destkind"] is not None:
        nonempty_string(value["destkind"], "destkind")
    report = value["report"]
    if not isinstance(report, dict) or set(report) != {"summary", "conclusion", "resolution", "missing_information"}:
        raise ValueError("Generated report needs summary, conclusion, resolution and missing_information only")
    for field in ("conclusion", "resolution"):
        nonempty_string(report[field], field)
    if not isinstance(report["summary"], list):
        raise ValueError("summary must be a list")
    for item in report["summary"]:
        if not isinstance(item, dict) or set(item) != {"kind", "explanation"}:
            raise ValueError("Each summary entry needs kind and explanation")
        if item["kind"] is not None:
            nonempty_string(item["kind"], "summary.kind")
        nonempty_string(item["explanation"], "summary.explanation")
    values = report["missing_information"]
    if not isinstance(values, list) or not all(isinstance(v, str) and v.strip() for v in values):
        raise ValueError("missing_information must be a list of nonempty strings")
    return value


def normalize_usage(usage):
    usage = usage or {}
    return {key: usage.get(key) if type(usage.get(key)) is int and usage[key] >= 0 else None
            for key in TOKEN_FIELDS}


def sum_usage(stages):
    values = [stage["token_usage"] for stage in stages]
    return {key: sum(v[key] for v in values) if all(v[key] is not None for v in values) else None
            for key in TOKEN_FIELDS}


def request_json(client, model, parameters, messages, validator):
    result = {"model": model, "messages": messages, "status": "api_error",
              "token_usage": normalize_usage(None)}
    started = time.perf_counter()
    try:
        response = client.chat.completions.create(model=model, messages=messages, **parameters)
        raw = json.loads(response.model_dump_json())
        result.update(raw_response=raw, token_usage=normalize_usage(raw.get("usage")), status="response_error")
        if not response.choices:
            raise ValueError("Response contains no choices")
        choice = response.choices[0]
        result.update(raw_text=choice.message.content, finish_reason=choice.finish_reason)
        if choice.finish_reason != "stop":
            raise ValueError(f"Unexpected finish_reason: {choice.finish_reason}")
        result["data"] = validator(choice.message.content)
        result["status"] = "ok"
    except Exception as exc:
        result["error"] = {"type": type(exc).__name__, "message": str(exc)}
    finally:
        result["time_cost"] = time.perf_counter() - started
    return result
