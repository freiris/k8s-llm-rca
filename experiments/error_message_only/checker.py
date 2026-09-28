"""Check explanation quality using only the message and candidate report."""

import json
import math

from client import nonempty_string, parse_json, request_json


def parse_quality(text):
    value = parse_json(text)
    if set(value) != {"overall_score", "further_investigation", "quality_feedback"}:
        raise ValueError("Checker needs overall_score, further_investigation and quality_feedback only")
    score = value["overall_score"]
    if type(score) not in (int, float) or not math.isfinite(score) or not 0 <= score <= 10:
        raise ValueError("overall_score must be a finite number from 0 to 10")
    if type(value["further_investigation"]) is not bool:
        raise ValueError("further_investigation must be a JSON boolean")
    nonempty_string(value["quality_feedback"], "quality_feedback")
    return value


class MessageOnlyReportQualityChecker:
    def __init__(self, client, config, prompt):
        self.client = client
        self.model = config["checker_model"] or config["model"]
        self.parameters = config["checker_request_parameters"]
        self.prompt = prompt

    def check(self, error_message, candidate):
        messages = [
            {"role": "system", "content": self.prompt},
            {"role": "user", "content": json.dumps(
                {"error_message": error_message, "candidate": candidate}, ensure_ascii=False)},
        ]
        return request_json(self.client, self.model, self.parameters, messages, parse_quality)
