"""Offline workflow checks through the real SDK and a mock HTTP transport."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import httpx
from openai import OpenAI

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import run
from checker import parse_quality
from client import parse_report
from dataset import load_samples

CANDIDATE = {"destkind": "Node", "report": {
    "summary": [{"kind": "Node", "explanation": "The scheduler reports insufficient memory."}],
    "conclusion": "Requested memory may exceed available scheduling capacity.",
    "resolution": "Inspect pod requests and node allocatable memory before adjusting capacity.",
    "missing_information": ["Pod requests and node allocations."]}}

def quality(retry=False):
    return {"overall_score": 6 if retry else 9, "further_investigation": retry,
            "quality_feedback": "Explain how requests affect scheduling." if retry else "Adequate explanation."}

def completion(content, finish_reason="stop", usage=True):
    result = {"id": "mock", "object": "chat.completion", "created": 1, "model": "mock-model",
              "choices": [{"index": 0, "finish_reason": finish_reason,
                           "message": {"role": "assistant", "content": content}}]}
    if usage:
        result["usage"] = {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
    return result

def is_checker(body):
    return body["messages"][0]["content"].startswith("You are MessageOnlyReportQualityChecker")

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.input = self.root / "samples.jsonl"
        rows = [{"sample_id": "one", "error_message": "First error", "label": "SECRET-LABEL",
                 "namespace": "PRIVATE-NAMESPACE", "timestamp": "original-time", "uuid": "original-uuid",
                 "dataset": "dataset-1", "reason": "Evicted", "type": "LowOnResource"},
                {"sample_id": "two", "error_message": "Second error"}]
        self.input.write_text("".join(json.dumps(row) + "\n" for row in rows))

    def execute(self, handler=None, extra=()):
        args = ["--input", str(self.input), "--output-dir", str(self.root / "runs"), *extra]
        if handler is None:
            with patch.object(run, "create_client") as factory, contextlib.redirect_stdout(io.StringIO()):
                code = run.main([*args, "--dry-run"])
                factory.assert_not_called()
        else:
            client = OpenAI(api_key="test-key", base_url="https://mock.invalid/v1", max_retries=0,
                            http_client=httpx.Client(transport=httpx.MockTransport(handler)))
            with patch.object(run, "create_client", return_value=client), contextlib.redirect_stdout(io.StringIO()):
                code = run.main(args)
        directory = next((self.root / "runs").iterdir())
        read = lambda name: [json.loads(line) for line in (directory / name).read_text().splitlines()]
        return code, read("results.jsonl"), json.loads((directory / "summary.json").read_text()), read("requests.jsonl"), directory

    def test_dry_run_preserves_metadata_without_sending_it(self):
        with patch.dict("os.environ", {}, clear=True):
            code, rows, summary, requests, directory = self.execute()
        self.assertEqual(code, 0)
        self.assertEqual(summary["planned"], 2)
        self.assertEqual(summary["request_count"], 0)
        self.assertEqual(rows[0]["namespace"], "PRIVATE-NAMESPACE")
        self.assertEqual(rows[0]["uuid"], "original-uuid")
        self.assertIsNone(rows[1]["namespace"])
        self.assertNotIn("SECRET-LABEL", json.dumps(rows))
        for request, row in zip(requests, rows):
            self.assertEqual(request["messages"][1]["content"], row["error_message"])
            self.assertNotIn("PRIVATE-NAMESPACE", json.dumps(request["messages"]))
        manifest = json.loads((directory / "manifest.json").read_text())
        self.assertEqual(manifest["max_model_requests"], 12)
        self.assertEqual(set(manifest["code_sha256"]), {"run.py", "client.py", "checker.py", "dataset.py", "cases.py", "pretty_json.py", "progress.py"})

    def test_first_pass_has_two_calls_and_independent_samples(self):
        calls = []
        def handler(request):
            self.assertEqual(request.url.path, "/v1/chat/completions")
            body = json.loads(request.content)
            calls.append(body)
            return httpx.Response(200, json=completion(json.dumps(quality() if is_checker(body) else CANDIDATE)))
        code, rows, summary, requests, _ = self.execute(handler)
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 4)
        self.assertEqual(summary["quality_passed_samples"], 2)
        self.assertEqual(summary["reported_total_tokens"], 120)
        self.assertTrue(summary["token_usage_complete"])
        for row in rows:
            self.assertEqual(row["attempt"], 1)
            self.assertEqual(row["stop_reason"], "quality_passed")
            self.assertEqual(row["token_usage"]["total_tokens"], 60)
            cost_fields = {key for key in row if "token" in key or "time_cost" in key}
            self.assertEqual(cost_fields, {"token_usage", "time_cost"})
            self.assertEqual(row["analysis"][0]["report"]["overall_score"], 9)
        for body in calls:
            self.assertEqual(len(body["messages"]), 2)
            self.assertNotIn("PRIVATE-NAMESPACE", json.dumps(body))
            self.assertNotIn("SECRET-LABEL", json.dumps(body))
        self.assertEqual(calls[2]["messages"][1]["content"], "Second error")
        self.assertEqual([r["stage"] for r in requests], ["report_generation", "quality_check"] * 2)

    def test_feedback_revises_then_stops(self):
        calls, checks = [], []
        def handler(request):
            body = json.loads(request.content)
            calls.append(body)
            if is_checker(body):
                checks.append(body)
                output = quality(retry=len(checks) == 1)
            else:
                output = CANDIDATE
            return httpx.Response(200, json=completion(json.dumps(output)))
        code, rows, summary, _, _ = self.execute(handler, ["--limit", "1"])
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 4)
        self.assertEqual([r["attempt"] for r in rows], [1, 2])
        self.assertEqual([r["stop_reason"] for r in rows], ["quality_retry", "quality_passed"])
        revised = calls[2]["messages"]
        self.assertEqual(len(revised), 4)
        self.assertEqual(revised[1]["content"], "First error")
        self.assertEqual(json.loads(revised[2]["content"]), CANDIDATE)
        self.assertIn(quality(True)["quality_feedback"], revised[3]["content"])
        self.assertEqual(summary["reported_total_tokens"], 120)

    def test_three_round_limit_preserves_negative_verdict(self):
        calls = []
        def handler(request):
            body = json.loads(request.content)
            calls.append(body)
            return httpx.Response(200, json=completion(json.dumps(quality(True) if is_checker(body) else CANDIDATE)))
        code, rows, summary, _, _ = self.execute(handler, ["--limit", "1"])
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 6)
        self.assertEqual([r["attempt"] for r in rows], [1, 2, 3])
        self.assertEqual(rows[-1]["stop_reason"], "max_attempts_reached")
        self.assertTrue(rows[-1]["analysis"][0]["report"]["further_investigation"])
        self.assertEqual(summary["max_attempts_reached_samples"], 1)
        self.assertEqual(summary["quality_passed_samples"], 0)

    def test_checker_schema_error_keeps_report_and_stops(self):
        calls = []
        def handler(request):
            body = json.loads(request.content)
            calls.append(body)
            output = dict(quality(), further_investigation="false") if is_checker(body) else CANDIDATE
            return httpx.Response(200, json=completion(json.dumps(output)))
        code, rows, summary, _, _ = self.execute(handler, ["--limit", "1"])
        self.assertEqual(code, 1)
        self.assertEqual(len(calls), 2)
        self.assertEqual(rows[0]["status"], "quality_check_error")
        self.assertEqual(rows[0]["checker_error_status"], "response_error")
        self.assertEqual(rows[0]["analysis"][0]["report"]["conclusion"], CANDIDATE["report"]["conclusion"])
        self.assertIsNone(rows[0]["analysis"][0]["report"]["overall_score"])
        self.assertEqual(summary["failed_samples"], 1)

    def test_checker_api_failure_has_no_transport_retry(self):
        calls = []
        def handler(request):
            body = json.loads(request.content)
            calls.append(body)
            if is_checker(body):
                return httpx.Response(500, json={"error": {"message": "mock failure", "type": "server_error"}})
            return httpx.Response(200, json=completion(json.dumps(CANDIDATE)))
        code, rows, summary, _, _ = self.execute(handler, ["--limit", "1"])
        self.assertEqual(code, 1)
        self.assertEqual(len(calls), 2)
        self.assertEqual(rows[0]["checker_error_status"], "api_error")
        self.assertIsNone(rows[0]["token_usage"]["total_tokens"])
        self.assertEqual(summary["reported_total_tokens"], 30)

    def test_generator_errors_do_not_reach_checker(self):
        calls = []
        def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(200, json=completion("not JSON"))
            return httpx.Response(500, json={"error": {"message": "mock failure", "type": "server_error"}})
        code, rows, summary, requests, _ = self.execute(handler)
        self.assertEqual(code, 1)
        self.assertEqual(len(calls), 2)
        self.assertEqual([r["status"] for r in rows], ["response_error", "api_error"])
        self.assertEqual(rows[0]["analysis"], [])
        self.assertEqual(requests[0]["raw_text"], "not JSON")
        self.assertEqual(summary["failed_samples"], 2)

    def test_truncated_response_is_not_checked(self):
        def handler(request):
            return httpx.Response(200, json=completion(json.dumps(CANDIDATE), "length"))
        code, rows, summary, requests, _ = self.execute(handler, ["--limit", "1"])
        self.assertEqual(code, 1)
        self.assertEqual(summary["request_count"], 1)
        self.assertEqual(rows[0]["stop_reason"], "generator_error")
        self.assertEqual(requests[0]["finish_reason"], "length")

    def test_missing_usage_does_not_create_false_total(self):
        def handler(request):
            body = json.loads(request.content)
            check = is_checker(body)
            if check:
                self.assertEqual(body["model"], "other-checker")
            return httpx.Response(200, json=completion(json.dumps(quality() if check else CANDIDATE), usage=not check))
        code, rows, summary, requests, _ = self.execute(handler, ["--limit", "1", "--checker-model", "other-checker"])
        self.assertEqual(code, 0)
        self.assertIsNone(rows[0]["token_usage"]["total_tokens"])
        self.assertEqual(requests[0]["token_usage"]["total_tokens"], 30)
        self.assertEqual(summary["usage_reported_request_count"], 1)
        self.assertEqual(summary["reported_total_tokens"], 30)
        self.assertFalse(summary["token_usage_complete"])

    def test_one_round_override_still_checks(self):
        def handler(request):
            body = json.loads(request.content)
            return httpx.Response(200, json=completion(json.dumps(quality(True) if is_checker(body) else CANDIDATE)))
        code, rows, summary, _, _ = self.execute(handler, ["--limit", "1", "--max-attempts", "1"])
        self.assertEqual(code, 0)
        self.assertEqual(summary["request_count"], 2)
        self.assertEqual(rows[0]["stop_reason"], "max_attempts_reached")

    def test_invalid_checker_types_and_generator_scoring_rejected(self):
        for value in (True, -1, 11, float("nan"), "9"):
            with self.subTest(score=value), self.assertRaises(ValueError):
                parse_quality(json.dumps(dict(quality(), overall_score=value)))
        bad = json.loads(json.dumps(CANDIDATE))
        bad["report"]["overall_score"] = 10
        with self.assertRaises(ValueError):
            parse_report(json.dumps(bad))

    def test_invalid_budget_rejected_before_creating_client(self):
        with patch.object(run, "create_client") as factory, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                run.main(["--input", str(self.input), "--max-attempts", "4"])
            self.assertEqual(error.exception.code, 2)
            factory.assert_not_called()

    def test_duplicate_ids_rejected(self):
        self.input.write_text('{"sample_id":"one","error_message":"A"}\n'
                              '{"sample_id":"one","error_message":"B"}\n')
        with self.assertRaisesRegex(ValueError, "Duplicate sample_id"):
            load_samples(self.input)
        self.assertFalse((self.root / "runs").exists())

if __name__ == "__main__":
    unittest.main()
