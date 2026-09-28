# Input contract

The runner accepts a single JSONL, a single CSV, or a directory of CSV files. Use `--input PATH --output-dir PATH` directly, or save reusable choices in `configs/inputs/`. `--limit` limits the total selected rows; `--limit-per-file` limits each file before combining them. CSV directories are sorted by filename and are not read recursively. Existing input rows and duplicates are retained.

`smoke.jsonl` contains two synthetic messages for pipeline checks. It is not a public evaluation dataset.

Each JSONL line needs a nonempty string `sample_id` and a nonempty string `error_message`. IDs must be unique. Optional `namespace`, `timestamp`, `uuid`, `dataset`, `reason`, and `type` metadata are copied to results, but never sent to either model. These fields must be strings or null; absent fields become null. Other fields are ignored. Keep ground truth and evaluation labels in a separate file joined by `sample_id`.

When the public input is selected, record its source, version, license, sample selection, preprocessing, and any deduplication. Put local data in `data/local/`, which is ignored by Git. Preserve the original message text.

Do not silently add namespace, timestamps, SourceKind, resource state, or graph-derived information to this baseline's prompt.

Both historical Table 5.10 inputs have now been copied and verified. See [input provenance](../docs/input-provenance.md): `INPUT/data-4-n166` contains 619 samples and is byte-identical to the Mac's corresponding old data-4 files. The current n168 copy is `INPUT/data-5-refine-n168` (previously named `INPUT/data-5-refine`) and contains 843 samples; all messages and full case identities match GPT-4o outputs, and all row occurrences and ordering match final mini outputs after retaining first attempts. Run `python3 experiments/error_message_only/tools/audit_inputs.py` from the repository root to reproduce the audit and generate `data/local/input-audit/dataset-1.jsonl` and `dataset-2.jsonl` directly from original CSVs. These JSONLs or the original CSV directories can be used directly; avoid the older output-recovered dataset-2 candidate. Any new public dataset remains a separate input choice.
