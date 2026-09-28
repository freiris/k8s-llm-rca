"""Offline checks for model claims and safe response-header capture."""

import json
from pathlib import Path
import sys
import unittest

import httpx
from openai import OpenAI

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from probe_model import probe


class ModelProbeTests(unittest.TestCase):
    def call_probe(self, returned_model, status=200):
        requests = []

        def respond(request):
            requests.append(json.loads(request.content))
            return httpx.Response(status, json={
                "id": "chatcmpl-test", "model": returned_model,
                "choices": [{"finish_reason": "stop", "message": {"content": "OK"}}],
                "gateway_metadata": {"upstream_model": "gpt-5.2"},
            }, headers={"x-request-id": "trace-test", "x-litellm-model-name": "gpt-5.2",
                        "llm_provider-x-request-id": "upstream-test", "x-api-key": "secret",
                        "set-cookie": "secret-cookie"})

        with OpenAI(api_key="test", base_url="https://mock.invalid/v1", max_retries=0,
                    http_client=httpx.Client(transport=httpx.MockTransport(respond))) as client:
            result = probe(client, "jwg/gpt-4o", 128)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["model"], "jwg/gpt-4o")
        self.assertNotIn("secret", json.dumps(result))
        return result

    def test_alias_match_does_not_verify_backend_and_retains_gateway_fields(self):
        result = self.call_probe("jwg/gpt-4o")
        self.assertTrue(result["model_name_matches"])
        self.assertFalse(result["backend_identity_verified"])
        self.assertEqual(result["response_headers"], {
            "x-request-id": "trace-test", "x-litellm-model-name": "gpt-5.2",
            "llm_provider-x-request-id": "upstream-test"})
        self.assertEqual(result["raw_response"]["gateway_metadata"]["upstream_model"], "gpt-5.2")

    def test_returned_model_can_differ_from_requested_alias(self):
        result = self.call_probe("gpt-5.2")
        self.assertFalse(result["model_name_matches"])
        self.assertEqual(result["returned_model"], "gpt-5.2")

    def test_http_error_keeps_trace_id_without_retrying(self):
        result = self.call_probe("unknown", status=403)
        self.assertEqual(result["status"], "api_error")
        self.assertEqual(result["http_status"], 403)
        self.assertEqual(result["response_headers"]["x-request-id"], "trace-test")


if __name__ == "__main__":
    unittest.main()
