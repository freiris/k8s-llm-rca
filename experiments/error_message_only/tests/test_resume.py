"""Exercise recovery at model-call and output-write interruption boundaries."""
import contextlib
import fcntl
import io
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch
import httpx
from openai import OpenAI

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import run
from progress import append_json, read_journal
from test_pipeline import CANDIDATE, completion, is_checker, quality
sys.path.insert(0, str(HERE / 'tools'))
from repair_usage import repair_usage


class ResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.input = self.root / 'input.jsonl'
        self.input.write_text(json.dumps({'sample_id': 'one', 'error_message': 'First error'}) + '\n')
        self.output = self.root / 'runs'

    def call(self, handler=None, resume=False, extra=()):
        args = ['--resume', str(self.directory)] if resume else ['--input', str(self.input), '--output-dir', str(self.output)]
        if handler is None:
            with patch.object(run, 'create_client') as factory, contextlib.redirect_stdout(io.StringIO()):
                code = run.main([*args, *extra])
                factory.assert_not_called()
        else:
            client = OpenAI(api_key='test-key', base_url='https://mock.invalid/v1', max_retries=0,
                            http_client=httpx.Client(transport=httpx.MockTransport(handler)))
            with patch.object(run, 'create_client', return_value=client), contextlib.redirect_stdout(io.StringIO()):
                code = run.main([*args, *extra])
        if not resume:
            self.directory = next(self.output.iterdir())
        return code

    def read(self, name):
        path = self.directory / name
        return json.loads(path.read_text()) if path.suffix == '.json' else read_journal(path)

    def good(self, request):
        body = json.loads(request.content)
        return httpx.Response(200, json=completion(json.dumps(quality() if is_checker(body) else CANDIDATE)))

    def interrupt_checker(self, request):
        if is_checker(json.loads(request.content)):
            raise KeyboardInterrupt()
        return self.good(request)

    def test_skips_completed_sample_and_uses_snapshot_after_source_removed(self):
        self.input.write_text(''.join(json.dumps({'sample_id': name, 'error_message': message})+'\n'
                                      for name, message in [('one','First error'),('two','Second error')]))
        def initial(request):
            body = json.loads(request.content)
            if not is_checker(body) and body['messages'][1]['content'] == 'Second error':
                raise KeyboardInterrupt()
            return self.good(request)
        self.assertEqual(self.call(initial), 130)
        self.assertEqual(self.read('summary.json')['completed_samples'], 1)
        self.input.unlink()
        calls = []
        def resumed(request):
            calls.append(json.loads(request.content))
            return self.good(request)
        self.assertEqual(self.call(resumed, resume=True), 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]['messages'][1]['content'], 'Second error')
        self.assertEqual(self.read('summary.json')['quality_passed_samples'], 2)
        self.assertEqual(self.read('summary.json')['reported_total_tokens'], 120)
        self.assertEqual(self.read('summary.json')['unknown_request_count'], 1)
        self.assertFalse(self.read('summary.json')['token_usage_complete'])

    def test_reuses_generator_when_checker_interrupted(self):
        self.assertEqual(self.call(self.interrupt_checker), 130)
        calls = []
        def resumed(request):
            body = json.loads(request.content)
            calls.append(body)
            self.assertTrue(is_checker(body))
            self.assertEqual(json.loads(body['messages'][1]['content'])['candidate'], CANDIDATE)
            return self.good(request)
        self.assertEqual(self.call(resumed, resume=True), 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.read('summary.json')['request_count'], 2)
        result = self.read('results.jsonl')[0]
        self.assertEqual(result['token_usage']['total_tokens'],60)
        requests = self.read('requests.jsonl')
        self.assertEqual(result['request_ids']['report_generation'],requests[0]['request_id'])
        self.assertEqual(requests[0]['token_usage']['total_tokens'],30)
        summary = self.read('summary.json')
        self.assertEqual(summary['unknown_request_count'],1)
        self.assertFalse(summary['time_cost_complete'])
        self.assertFalse(summary['token_usage_complete'])
        self.assertEqual(summary['reported_total_tokens'],60)

    def test_both_responses_saved_but_result_missing_needs_no_model_call(self):
        def interrupt_result(stream, value):
            if Path(stream.name).name == 'results.jsonl':
                raise KeyboardInterrupt()
            return append_json(stream, value)
        with patch.object(run, 'append_json', side_effect=interrupt_result):
            self.assertEqual(self.call(self.good), 130)
        self.assertEqual(len(self.read('requests.jsonl')), 2)
        self.assertEqual(len(self.read('results.jsonl')), 0)
        self.assertEqual(self.call(resume=True), 0)
        self.assertEqual(len(self.read('requests.jsonl')), 2)
        self.assertEqual(len(self.read('results.jsonl')), 1)

    def test_reconstructs_revision_context_and_attempt_budget(self):
        checks = []
        def initial(request):
            body = json.loads(request.content)
            if not is_checker(body) and len(body['messages']) == 4:
                raise KeyboardInterrupt()
            if is_checker(body):
                checks.append(body)
                return httpx.Response(200, json=completion(json.dumps(quality(True))))
            return self.good(request)
        self.assertEqual(self.call(initial), 130)
        self.assertEqual(self.read('results.jsonl')[0]['stop_reason'], 'quality_retry')
        calls = []
        def resumed(request):
            body = json.loads(request.content)
            calls.append(body)
            if not is_checker(body):
                self.assertEqual(len(body['messages']), 4)
                self.assertEqual(json.loads(body['messages'][2]['content']), CANDIDATE)
                self.assertIn(quality(True)['quality_feedback'], body['messages'][3]['content'])
            return self.good(request)
        self.assertEqual(self.call(resumed, resume=True), 0)
        self.assertEqual([row['attempt'] for row in self.read('results.jsonl')], [1, 2])
        self.assertEqual(len(calls), 2)

    def test_retry_failed_checker_keeps_generator_and_cost_history(self):
        def initial(request):
            if is_checker(json.loads(request.content)):
                return httpx.Response(500, json={'error': {'message': 'mock failure', 'type': 'server_error'}})
            return self.good(request)
        self.assertEqual(self.call(initial), 1)
        self.assertEqual(self.call(resume=True), 1)  # Default leaves recorded failures untouched.
        calls = []
        def resumed(request):
            body=json.loads(request.content)
            calls.append(body)
            self.assertTrue(is_checker(body))
            return self.good(request)
        self.assertEqual(self.call(resumed, resume=True, extra=['--retry-failed']), 0)
        rows = self.read('results.jsonl')
        self.assertEqual([(r['attempt'],r['execution']) for r in rows], [(1,1),(1,2)])
        self.assertEqual(rows[0]['status'], 'quality_check_error')
        self.assertEqual(len(self.read('requests.jsonl')),3)
        self.assertEqual(rows[1]['request_ids']['report_generation'], rows[0]['request_ids']['report_generation'])
        self.assertNotEqual(rows[1]['request_ids']['quality_check'], rows[0]['request_ids']['quality_check'])
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.read('summary.json')['failed_samples'], 0)
        self.assertEqual(self.read('summary.json')['logical_attempts'], 1)
        self.assertEqual(self.read('summary.json')['reported_total_tokens'], 60)
        self.assertEqual(rows[1]['token_usage']['total_tokens'],60)
        self.assertFalse(self.read('summary.json')['token_usage_complete'])

    def test_generator_failure_retried_only_with_flag(self):
        def initial(request):
            return httpx.Response(200,json=completion('not JSON'))
        self.assertEqual(self.call(initial), 1)
        self.assertEqual(self.call(resume=True), 1)
        self.assertEqual(self.call(self.good, resume=True, extra=['--retry-failed']), 0)
        self.assertEqual(self.read('summary.json')['request_count'], 3)
        self.assertEqual(self.read('summary.json')['reported_total_tokens'], 90)
        self.assertEqual(self.read('results.jsonl')[-1]['token_usage']['total_tokens'], 60)
        self.assertEqual(self.read('summary.json')['reported_total_tokens'], 90)

    def test_repair_usage_restores_success_values_and_preserves_report_and_requests(self):
        def initial(request):
            return httpx.Response(200,json=completion('not JSON'))
        self.assertEqual(self.call(initial),1)
        self.assertEqual(self.call(self.good,resume=True,extra=['--retry-failed']),0)
        rows=self.read('results.jsonl')
        report=rows[-1]['analysis']
        rows[-1]['token_usage']={key:None for key in ('prompt_tokens','completion_tokens','total_tokens')}
        rows[-1].update(token_usage_details={'report_generation': {'total_tokens': 30}},
                        time_cost_details={'report_generation': 1}, new_token_usage={'total_tokens': 60},
                        new_time_cost=1, cumulative_usage={'token_usage': {'total_tokens': 90}},
                        reused_request_ids={}, request_count=2, token_usage_complete=True)
        source=self.directory/'results.jsonl'
        source.write_text(''.join(json.dumps(row)+'\n' for row in rows))
        old=source.read_bytes()
        requests_before=(self.directory/'requests.jsonl').read_bytes()
        with patch.object(run,'create_client') as factory:
            backup=repair_usage(self.directory)
            factory.assert_not_called()
        fixed=self.read('results.jsonl')
        self.assertEqual(backup.read_bytes(),old)
        self.assertEqual((self.directory/'requests.jsonl').read_bytes(),requests_before)
        self.assertEqual(fixed[-1]['analysis'],report)
        self.assertEqual(fixed[-1]['token_usage']['total_tokens'],60)
        cost_fields = {key for key in fixed[-1] if 'token' in key or 'time_cost' in key}
        self.assertEqual(cost_fields, {'token_usage', 'time_cost'})
        self.assertNotIn('cumulative_usage', fixed[-1])
        self.assertNotIn('reused_request_ids', fixed[-1])
        self.assertEqual(fixed[-1]['request_ids'],rows[-1]['request_ids'])
        self.assertEqual(self.read('summary.json')['reported_total_tokens'],90)
        self.assertEqual(self.read('results.json'),fixed)

    def test_completed_and_exhausted_samples_are_never_repeated(self):
        def initial(request):
            body=json.loads(request.content)
            return httpx.Response(200,json=completion(json.dumps(quality(True) if is_checker(body) else CANDIDATE)))
        self.assertEqual(self.call(initial), 0)
        self.assertEqual(self.read('summary.json')['request_count'], 6)
        self.assertEqual(self.call(resume=True,extra=['--retry-failed']), 0)
        self.assertEqual(self.read('summary.json')['request_count'], 6)
        self.assertEqual(self.read('results.jsonl')[-1]['stop_reason'], 'max_attempts_reached')

    def test_progress_inspection_makes_no_calls_or_journal_changes(self):
        self.assertEqual(self.call(self.interrupt_checker), 130)
        before={p.name:p.read_bytes() for p in self.directory.iterdir() if p.is_file()}
        self.assertEqual(self.call(resume=True,extra=['--dry-run']), 0)
        after={p.name:p.read_bytes() for p in self.directory.iterdir() if p.is_file()}
        self.assertEqual(before,after)

    def test_torn_last_lines_repaired_and_backed_up(self):
        self.assertEqual(self.call(self.interrupt_checker), 130)
        with (self.directory/'requests.jsonl').open('ab') as stream:
            stream.write(b'{"sample_id":')
        self.assertEqual(self.call(self.good,resume=True), 0)
        backups=list(self.directory.glob('requests.jsonl.partial-*'))
        self.assertEqual(len(backups),1)
        self.assertEqual(backups[0].read_bytes(),b'{"sample_id":')
        self.assertEqual(len(self.read('requests.jsonl')),2)
        self.assertEqual(json.loads((self.directory/'requests.json').read_text()),self.read('requests.jsonl'))

    def test_prompt_and_input_snapshot_changes_rejected(self):
        self.assertEqual(self.call(self.interrupt_checker), 130)
        prompt=self.directory/'prompt.txt'
        original=prompt.read_text()
        prompt.write_text(original+'changed')
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.call(resume=True)
        self.assertEqual(error.exception.code,2)
        prompt.write_text(original)
        snapshot=self.directory/'samples.jsonl'
        rows=self.read('samples.jsonl')
        rows[0]['error_message']='changed'
        snapshot.write_text(json.dumps(rows[0])+'\n')
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.call(resume=True)
        self.assertEqual(error.exception.code,2)

    def test_service_change_rejected_before_requests(self):
        self.assertEqual(self.call(self.interrupt_checker),130)
        with patch.dict('os.environ',{'OPENAI_BASE_URL':'https://different.invalid/v1'}), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.call(resume=True)
        self.assertEqual(error.exception.code,2)

    def test_concurrent_resume_rejected(self):
        self.assertEqual(self.call(self.good),0)
        with (self.directory/'.run.lock').open('a') as lock:
            fcntl.flock(lock.fileno(),fcntl.LOCK_EX | fcntl.LOCK_NB)
            with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit) as error:
                self.call(resume=True)
            self.assertEqual(error.exception.code,2)

    def test_corrupt_middle_line_is_not_discarded(self):
        file=self.root/'broken.jsonl'
        original=b'{"ok":true}\ninvalid\n{"ok":true}\n'
        file.write_bytes(original)
        with self.assertRaisesRegex(ValueError,'Corrupt journal'):
            read_journal(file,repair=True)
        self.assertEqual(file.read_bytes(),original)

    def test_sigkill_after_generator_recovers_without_finally_block(self):
        ready = self.root / 'checker-started'
        script = '''
import json, sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import run
import checker
candidate = json.loads(sys.argv[5])
run.create_client = lambda timeout: type('FakeClient', (), {'close': lambda self: None})()
def request_json(client, model, parameters, messages, validator):
    if messages[0]['content'].startswith('You are MessageOnlyReportQualityChecker'):
        Path(sys.argv[4]).write_text('ready')
        time.sleep(60)
    return {'model': model, 'messages': messages, 'status': 'ok', 'data': candidate,
            'time_cost': 0.01, 'token_usage': {'prompt_tokens': 10, 'completion_tokens': 20, 'total_tokens': 30}}
run.request_json = request_json
checker.request_json = request_json
run.main(['--input', sys.argv[2], '--output-dir', sys.argv[3]])
'''
        process = subprocess.Popen([sys.executable, '-c', script, str(HERE), str(self.input),
                                    str(self.output), str(ready), json.dumps(CANDIDATE)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            if not ready.exists() and process.poll() is not None:
                _, errors = process.communicate(timeout=5)
                self.fail('Child did not reach the checker stage: ' + errors.decode())
            self.assertTrue(ready.exists(), 'Child did not reach the checker stage')
            process.kill()
            process.communicate(timeout=5)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)
        self.directory = next(self.output.iterdir())
        self.assertEqual(len(self.read('requests.jsonl')), 1)
        self.assertEqual(len(self.read('results.jsonl')), 0)
        self.assertFalse((self.directory / 'requests.json').exists())
        calls = []
        def resumed(request):
            body = json.loads(request.content)
            calls.append(body)
            self.assertTrue(is_checker(body))
            return self.good(request)
        self.assertEqual(self.call(resumed, resume=True), 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.read('summary.json')['quality_passed_samples'], 1)


if __name__=='__main__':
    unittest.main()
