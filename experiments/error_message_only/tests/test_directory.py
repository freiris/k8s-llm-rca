"""Separate file outputs, preview without writes, and directory-level recovery."""

import contextlib
import csv
import io
import json
from pathlib import Path
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

import httpx
from openai import OpenAI

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import run
import run_directory
from test_pipeline import CANDIDATE, completion, is_checker, quality


class DirectoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.input = self.root / 'input with spaces'
        self.input.mkdir()
        self.output = self.root / 'output with spaces'
        for name, message in [('FailedMount-StaleNFS-repeat.csv', 'Second error'),
                              ('Evicted-LowOnResource-repeat.csv', 'First error')]:
            self.write_csv(self.input / name, message)
        (self.input / 'notes.txt').write_text('Not an input')
        nested = self.input / 'nested'
        nested.mkdir()
        self.write_csv(nested / 'ignored.csv', 'Ignored error')

    def write_csv(self, path, message):
        with path.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['message', 'namespace2', 'timestamp', 'uuid'])
            writer.writerow([message, 'namespace', 'timestamp', 'uuid'])

    def args(self, *extra):
        return ['--input-dir', str(self.input), '--output-dir', str(self.output),
                '--dataset', 'dataset-1', *extra]

    def good(self, request):
        body = json.loads(request.content)
        return httpx.Response(200, json=completion(json.dumps(quality() if is_checker(body) else CANDIDATE)))

    def execute_offline(self, handler, *extra):
        commands = []

        def execute(command):
            commands.append(command)
            # Exercise the actual per-file runner with mock HTTP, no subprocess/API.
            return run.main(command[3:])

        def factory(_timeout):
            return OpenAI(api_key='test', base_url='https://mock.invalid/v1', max_retries=0,
                          http_client=httpx.Client(transport=httpx.MockTransport(handler)))

        with patch.object(run_directory, 'run_command', side_effect=execute), \
                patch.object(run, 'create_client', side_effect=factory), \
                patch.dict('os.environ', {'OPENAI_BASE_URL': 'https://mock.invalid/v1'}), \
                contextlib.redirect_stdout(io.StringIO()):
            code = run_directory.main(self.args(*extra))
        return code, commands

    def result_dirs(self):
        return sorted(self.output.glob('*/*/manifest.json'))

    def test_preview_lists_csvs_in_order_and_writes_nothing(self):
        stream = io.StringIO()
        with patch.object(run_directory, 'run_command') as child, contextlib.redirect_stdout(stream):
            self.assertEqual(run_directory.main(self.args('--dry-run', '--limit-per-file', '1')), 0)
            child.assert_not_called()
        output = stream.getvalue()
        self.assertIn('CSV files: 2', output)
        self.assertLess(output.index('Evicted-LowOnResource-repeat.csv'), output.index('FailedMount-StaleNFS-repeat.csv'))
        self.assertNotIn('ignored.csv', output)
        self.assertFalse(self.output.exists())

    def test_real_runner_writes_independent_results_and_preserves_paths(self):
        code, commands = self.execute_offline(self.good, '--limit-per-file', '1')
        self.assertEqual(code, 0)
        self.assertEqual(len(commands), 2)
        manifests = self.result_dirs()
        self.assertEqual(len(manifests), 2)
        self.assertEqual({p.parent.parent.name for p in manifests},
                         {'Evicted-LowOnResource-repeat', 'FailedMount-StaleNFS-repeat'})
        for manifest_path in manifests:
            manifest = json.loads(manifest_path.read_text())
            rows = json.loads((manifest_path.parent / 'results.json').read_text())
            self.assertEqual(len(rows), 1)
            self.assertEqual(manifest['sample_count'], 1)
            self.assertEqual(rows[0]['dataset'], 'dataset-1')
            self.assertIn(manifest_path.parent.parent.name + '.csv', rows[0]['sample_id'])

    def test_resume_retries_failed_file_uses_snapshot_and_skips_completed_calls(self):
        def fail_first(request):
            body = json.loads(request.content)
            if not is_checker(body) and body['messages'][1]['content'] == 'First error':
                return httpx.Response(500, json={'error': {'message': 'mock failure'}})
            return self.good(request)

        code, _ = self.execute_offline(fail_first)
        self.assertEqual(code, 1)
        before = {p.parent: (p.parent / 'requests.jsonl').read_bytes() for p in self.result_dirs()}
        # The resumed report must still use the original message snapshot.
        self.write_csv(self.input / 'Evicted-LowOnResource-repeat.csv', 'Changed error')
        calls = []

        def resumed(request):
            body = json.loads(request.content)
            calls.append(body)
            if not is_checker(body):
                self.assertEqual(body['messages'][1]['content'], 'First error')
            return self.good(request)

        code, commands = self.execute_offline(resumed, '--resume', '--retry-failed')
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all('--resume' in command for command in commands))
        self.assertEqual(len(self.result_dirs()), 2)
        for directory, old_requests in before.items():
            if directory.parent.name.startswith('FailedMount'):
                self.assertEqual((directory / 'requests.jsonl').read_bytes(), old_requests)

    def test_resume_starts_new_files_and_ignores_dry_run_directories(self):
        code, _ = self.execute_offline(self.good)
        self.assertEqual(code, 0)
        self.write_csv(self.input / 'Failed-AccessDenied.csv', 'New error')
        parent = self.output / 'Evicted-LowOnResource-repeat'
        dry = parent / 'future-dry-run'
        dry.mkdir()
        (dry / 'manifest.json').write_text(json.dumps({'dry_run': True, 'started_at': '9999'}))
        code, commands = self.execute_offline(self.good, '--resume', '--limit-per-file', '1')
        self.assertEqual(code, 0)
        self.assertEqual(sum('--resume' in command for command in commands), 2)
        self.assertEqual(sum('--input' in command for command in commands), 1)

    def test_invalid_input_detected_before_any_file_runs(self):
        (self.input / 'Invalid.csv').write_text('invalid_header\nvalue\n')
        with patch.object(run_directory, 'run_command') as child, \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
            run_directory.main(self.args())
        self.assertEqual(exc.exception.code, 2)
        child.assert_not_called()

    def test_interrupted_child_stops_before_next_file(self):
        with patch.object(run_directory, 'run_command', return_value=130) as child, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(run_directory.main(self.args()), 130)
        self.assertEqual(child.call_count, 1)

    def test_ctrl_c_is_forwarded_and_child_waited(self):
        with patch.object(run_directory.subprocess, 'Popen') as start:
            child = start.return_value
            child.wait.side_effect = [KeyboardInterrupt(), 130]
            child.poll.return_value = None
            with self.assertRaises(KeyboardInterrupt):
                run_directory.run_command(['python3', 'run.py'])
            child.send_signal.assert_called_once_with(signal.SIGINT)
            self.assertEqual(child.wait.call_count, 2)


if __name__ == '__main__':
    unittest.main()
