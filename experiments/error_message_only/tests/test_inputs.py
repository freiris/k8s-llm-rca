"""Input formats, occurrence preservation, and configuration path precedence."""
import contextlib
import csv
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import run
from dataset import load_input_config, load_samples


class InputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.data = self.root / 'input'
        self.data.mkdir()
        self.file = self.data / 'FailedMount-StaleNFS-repeat.csv'
        self.other = self.data / 'Evicted-LowOnResource-repeat.csv'
        for file in (self.file, self.other):
            with file.open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(['namespace2', 'message', 'timestamp', 'uuid', 'name2'])
                # Exact repeated rows and a quoted multi-line message are intentional.
                for _ in range(3):
                    writer.writerow(['hidden-ns', 'error, "quoted"\nnext line', 'time', 'same-uuid', 'unused-name'])

    def configure(self):
        directory = self.root / 'settings'
        inputs = directory / 'inputs'
        inputs.mkdir(parents=True)
        spec = {'path': '../../input', 'dataset': 'dataset-1', 'limit': None, 'limit_per_file': 1}
        input_config = inputs / 'small.json'
        input_config.write_text(json.dumps(spec))
        config = json.loads((HERE / 'configs/gpt4o.json').read_text())
        config.update(input_config='inputs/small.json', output_dir='../configured-output',
                      prompt_file=str(HERE / 'prompts/message_only_v1.txt'),
                      checker_prompt_file=str(HERE / 'prompts/report_quality_v1.txt'))
        model_config = directory / 'model.json'
        model_config.write_text(json.dumps(config))
        return model_config, input_config

    def invoke(self, config, extra=()):
        with patch.object(run, 'create_client') as factory, contextlib.redirect_stdout(io.StringIO()):
            code = run.main(['--config', str(config), '--dry-run', *extra])
            factory.assert_not_called()
        self.assertEqual(code, 0)

    def test_csv_preserves_repeats_and_quoted_message(self):
        rows = load_samples(self.file, 'dataset-1')
        self.assertEqual(len(rows), 3)
        self.assertEqual(len({row['sample_id'] for row in rows}), 3)
        self.assertEqual({row['uuid'] for row in rows}, {'same-uuid'})
        self.assertEqual(rows[0]['error_message'], 'error, "quoted"\nnext line')
        self.assertEqual(rows[0]['namespace'], 'hidden-ns')
        self.assertEqual((rows[0]['reason'], rows[0]['type']), ('FailedMount', 'StaleNFS'))
        self.assertNotIn('unused-name', json.dumps(rows))

    def test_directory_selection_keeps_ids_and_order(self):
        rows = load_samples(self.data, 'dataset-1', limit_per_file=1)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['reason'], 'Evicted')
        self.assertEqual(rows[1]['sample_id'], load_samples(self.file, 'dataset-1')[0]['sample_id'])
        self.assertEqual(len(load_samples(self.data, 'dataset-1')), 6)

    def test_relative_input_and_output_paths_from_configuration(self):
        config, input_config = self.configure()
        self.assertEqual(load_input_config(input_config)['path'], str(self.data))
        self.invoke(config)
        directory = next((self.root / 'configured-output').iterdir())
        manifest = json.loads((directory / 'manifest.json').read_text())
        self.assertEqual(manifest['sample_count'], 2)
        self.assertEqual(manifest['input_selection']['dataset'], 'dataset-1')
        self.assertEqual(len(manifest['input_source_sha256']), 2)
        self.assertEqual(manifest['output_dir_resolved'], str(self.root / 'configured-output'))

    def test_cli_limits_and_output_override_configuration(self):
        config, _ = self.configure()
        output = self.root / 'override-output'
        self.invoke(config, ['--limit-per-file', '2', '--limit', '3', '--output-dir', str(output)])
        directory = next(output.iterdir())
        manifest = json.loads((directory / 'manifest.json').read_text())
        self.assertEqual(manifest['sample_count'], 3)
        self.assertEqual(manifest['input_selection']['limit_per_file'], 2)
        self.assertFalse((self.root / 'configured-output').exists())

    def test_cli_file_replaces_config_input_and_selection(self):
        config, _ = self.configure()
        self.invoke(config, ['--input', str(self.file), '--dataset', 'dataset-2'])
        directory = next((self.root / 'configured-output').iterdir())
        manifest = json.loads((directory / 'manifest.json').read_text())
        self.assertEqual(manifest['sample_count'], 3)
        self.assertEqual(manifest['input_selection']['dataset'], 'dataset-2')
        self.assertIsNone(manifest['input_config_file'])
        self.assertEqual(len(manifest['input_source_sha256']), 1)

    def test_explicit_input_config_without_cli_input(self):
        config, input_config = self.configure()
        self.invoke(config, ['--input-config', str(input_config), '--limit', '1'])
        directory = next((self.root / 'configured-output').iterdir())
        manifest = json.loads((directory / 'manifest.json').read_text())
        self.assertEqual(manifest['sample_count'], 1)
        self.assertEqual(manifest['input_config_file'], str(input_config))

    def test_invalid_csv_and_empty_directory_rejected(self):
        self.file.write_text('namespace2,message,timestamp,uuid\nns,error,time\n')
        with self.assertRaisesRegex(ValueError, 'columns'):
            load_samples(self.file)
        self.file.write_text('namespace2,other\nns,error\n')
        with self.assertRaisesRegex(ValueError, 'error_message column'):
            load_samples(self.file)
        empty = self.root / 'empty'
        empty.mkdir()
        with self.assertRaisesRegex(ValueError, 'no CSV'):
            load_samples(empty)

    def test_jsonl_dataset_conflict_and_invalid_limit_rejected(self):
        file = self.root / 'samples.jsonl'
        file.write_text(json.dumps({'sample_id': 'one', 'error_message': 'error', 'dataset': 'dataset-1'}) + '\n')
        with self.assertRaisesRegex(ValueError, 'conflicts'):
            load_samples(file, 'dataset-2')
        _, input_config = self.configure()
        value = json.loads(input_config.read_text())
        value['limit'] = True
        input_config.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'positive integer'):
            load_input_config(input_config)


if __name__ == '__main__':
    unittest.main()
