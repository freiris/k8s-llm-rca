# Error-message-only baseline

本实验直接解释 Kubernetes error message，使用独立的 MessageOnlyReportQualityChecker 检查报告，作为与 SynergyRCA 对比的基线。分支为 `exp/error-message-only`，起点为 `c9d592e`。

## 工作流程

1. 生成器依据原始 error message 输出最可能的原因、解释和排查建议。
2. 新 Checker 只读取原始消息和本轮候选报告，输出 `overall_score`、`further_investigation` 和具体 `quality_feedback`。
3. `further_investigation=false` 时停止；为 true 时将原消息、前次候选报告和反馈交给生成器修订，最多三轮。

这是按解释质量决定是否修订的流程。第三轮仍未通过时，保留 true 并记录 `max_attempts_reached`。Checker 不会单因没有外部集群数据而要求重跑，也不能验证真实根因；其评分不是人工正确率。不同样本之间不共享对话。

模型不接收输入中额外的 namespace、timestamp、uuid、reason/type、参考答案或图信息；消息原文中已有的信息保留。namespace 等元数据仅由程序复制到结果。整个流程不依赖 Neo4j、stategraph/metagraph 或旧 Assistants 实现。

`destkind` 指向与问题最直接相关、最可能导致故障的 Kubernetes API 或外部资源类型，不能仅因某资源承受或报告故障就选择它。v2 借鉴 SynergyRCA RootCauseLocator 的通用目标，要求考虑资源交互与依赖、解释推测原因如何导致故障，并保持 destkind 与 conclusion 一致；生成器和 Checker 不添加具体资源类型、故障场景或名称后缀提示。

当前默认 v3 在 v2 基础上明确要求 resolution 提供“针对最可能原因的可操作建议”，Checker 同步此要求。没有增加固定步骤、分支模板或强制命令要求。v1/v2 文件保留用于对照；`--resume` 沿用已有运行保存的 prompt。新版本实验使用独立输出根目录，避免同一目录批次中新旧版本混用。

## 文件

- `run.py`：批量运行、按 Checker 反馈修订、记录所有轮次。
- `run_directory.py`：按 CSV 文件分别调用 run.py，为每个文件建立独立输出目录，支持批量恢复。
- `client.py`：OpenAI SDK + 自定义 base URL 调用、生成报告格式校验。
- `checker.py`：独立 MessageOnlyReportQualityChecker 和检查结果校验。
- `configs/gpt4o.json`：生成器、Checker 和最多轮次数的配置。
- `configs/inputs/`：可选的输入路径和样本选择配置，命令行可覆盖。
- `prompts/message_only_v3.txt`、`prompts/report_quality_v3.txt`：当前两个阶段的英文 prompt；v1/v2 保留用于对照。
- `dataset.py`：JSONL 输入校验和元数据保留。
- `data/smoke.jsonl`：两个合成消息，仅用于流程验证。
- `docs/output-format-proposal.md`：输出字段、停止条件和后处理约定。
- `tests/test_pipeline.py`：真实 SDK + mock HTTP 的离线检查。
- `runs/`、`data/local/`：本地结果和数据，不纳入 Git。

## 离线检查

以下命令从仓库根目录执行。dry-run 不需要密钥，不调用服务：

```bash
python3 experiments/error_message_only/run.py \
  --input experiments/error_message_only/data/smoke.jsonl \
  --dry-run

python3 -m unittest discover \
  -s experiments/error_message_only/tests -v
```

dry-run 为每条样本保存第一轮生成请求的计划。实际 messages 在 `requests.jsonl`，两个阶段的 prompt 副本也会保存。Checker 需要生成报告才能调用，因此 dry-run 不虚构候选报告、检查结果或后续轮次。

## 使用模型服务

建议使用独立虚拟环境：

```bash
python3 -m venv experiments/error_message_only/.venv
source experiments/error_message_only/.venv/bin/activate
python -m pip install -r experiments/error_message_only/requirements.txt
```

依赖固定为已用于 Mac 离线 SDK 验证的 `openai==1.58.1`。设置 `OPENAI_API_KEY` 和 `OPENAI_BASE_URL`；`.env.example` 不会自动加载。base URL 应包含服务要求的 API 路径，例如 `/v1`。沿用此前测试成功的 hosted 模型服务调用方式，正式运行前先检查单条输入：

```bash
python3 experiments/error_message_only/run.py \
  --message '0/3 nodes are available: 3 Insufficient memory.'
```

默认生成和检查均用 `jwg/gpt-4o`，最多三轮，单条输入通常为 2–6 次模型调用。已用三条消息实际验证 hosted 服务的生成和检查流程。

### 单独检查服务返回的模型信息

```bash
python3 experiments/error_message_only/tools/probe_model.py
```

沿用 `OPENAI_API_KEY` 和 `OPENAI_BASE_URL`，默认读取 `configs/gpt4o.json` 的模型名。仅发送一次要求回答 `OK` 的小请求，不调用 Checker，不自动重试，不影响批量实验。支持 `--model`、`--config`、`--output-dir`、`--timeout`、`--max-tokens` 和 `--dry-run`。

结果保存到 `runs/model-probe/<UTC时间>-<随机ID>/model_probe.json`，包括请求/返回的模型名、响应 ID、fingerprint、usage、用于追踪的响应头及原始响应 JSON；终端打印摘要。响应头仅保存选定的追踪和模型字段，不保存认证或 cookie 值。特别检查 `x-litellm-model-name`、`x-litellm-model-group`、fallback/retry 计数和 `llm_provider-x-request-id`；这些字段若存在，可帮助管理员核对实际路由，仍属于服务返回的声明。

`returned_model` 是服务声明的名称。即使它与 `requested_model` 相同，也可能只是 hosted 服务的别名，因此 `backend_identity_verified` 保持 false。`system_fingerprint` 和模型自述也不能单独证明底层模型身份。若需确定是否实际使用 GPT-4o，请让服务管理员根据响应 ID 核对该别名的路由、上游模型/部署版本和 fallback。该探测结果只描述此次请求，不能追溯证明历史实验使用的模型。

支持 `--config`、`--model`、`--checker-model`、`--max-attempts`（1–3）、`--limit`、`--output-dir` 和 `--dry-run`。`checker_model=null` 表示跟随生成器模型，`--model` 也会改变 Checker；需要固定检查模型时使用 `--checker-model`。prompt 路径相对配置文件解析。

## 输入和输出路径

`run.py --input` 可以直接指定单个 CSV、单个 JSONL 或一个 CSV 目录；`--output-dir` 指定输出根目录。这些路径在运行时传入，没有固定为代码中的数据集路径。直接对目录调用 run.py 时，所有文件的结果写入同一个运行目录；希望每个 CSV 分开保存时，使用下方的 run_directory.py。

单个文件先取三条测试：

```bash
python3 experiments/error_message_only/run.py \
  --input INPUT/data-4-n166/FailedMount-StaleNFS-repeat.csv \
  --dataset dataset-1 --limit 3 \
  --output-dir experiments/error_message_only/runs/stale-nfs \
  --dry-run
```

每个 CSV 取一条，覆盖整个数据集的错误类别：

```bash
python3 experiments/error_message_only/run.py \
  --input INPUT/data-5-refine-n168 \
  --dataset dataset-2 --limit-per-file 1 \
  --output-dir experiments/error_message_only/runs/dataset-2-small \
  --dry-run
```

去掉 `--dry-run` 后调用模型；去掉限量参数后运行完整输入。目录只读取直接包含的 `.csv` 文件，不递归，按文件名排序；文件内部保留 CSV 记录顺序及重复行。`--limit-per-file N` 先对每个文件取前 N 条，`--limit N` 再对合并后的样本取前 N 条。单个文件也支持这两个参数。

自定义小输入放到 JSONL 文件中，例如 `data/local/my-test.jsonl`，再用 `--input` 指向它；不需要把消息写入 Python 代码。CSV 支持 `message`/`error_message`、`namespace2`/`namespace` 列名；timestamp、uuid 可选，额外列不进入模型。历史文件名识别 reason/type，仅保留在结果。

反复使用同一设置时可用配置。模型配置中的路径片段如下，其余模型参数照常保留：

```json
{
  "input_config": "inputs/dataset-1-small.json",
  "output_dir": "../runs/dataset-1-small"
}
```

输入配置独立保存路径和选择规则，例如 `configs/inputs/dataset-1-small.json`：

```json
{
  "path": "../../../../INPUT/data-4-n166",
  "dataset": "dataset-1",
  "limit": null,
  "limit_per_file": 1
}
```

input_config 和 output_dir 相对模型配置文件解析；输入 path 相对输入配置文件解析。也可使用绝对路径。命令行路径相对启动目录解析，命令行参数优先。显式 `--input` 或 `--message` 会替换配置中的输入选择，不继承原输入配置的限量或 dataset 设置；用 `--input-config` 切换配置时，`--limit`、`--limit-per-file`、`--dataset` 可单独覆盖选择参数。

预设包括 smoke（两个合成输入）、single-file（StaleNFS 前三条）、dataset-1-small（18 个文件各一条），以及 dataset-1/dataset-2（完整目录）。例如：

```bash
python3 experiments/error_message_only/run.py \
  --input-config experiments/error_message_only/configs/inputs/dataset-1-small.json \
  --dry-run
```

不传输入参数时使用模型配置的 input_config，当前默认 smoke；不传输出参数时使用其 output_dir。每次运行在输出根目录下创建新的时间戳目录，便于保留不同试验。manifest 保存有效选择规则、所有源文件哈希及实际输出路径。

直接 CSV 模式的 sample_id 由 dataset、文件名及原始数据行编号组成，选文件或选目录时保持一致。审计生成的 JSONL 保留原来的 sample_id 命名方式；跨这两种输入格式比较时应使用明确的行对应关系，不能直接假定 ID 相同。

### 逐文件运行整个目录

推荐通过参数指定输入、输出路径，不需要修改脚本变量。Python 包装脚本读取目录直接包含的 CSV（不递归），按文件名排序，逐个调用同一 Python 环境中的 run.py。模型、prompt、Checker 和最多三轮的策略沿用默认配置；`--dataset` 可选，用来保留明确的数据集标签。

先预览 dataset-1 的文件列表、每个文件的样本数、输出位置和实际命令：

```bash
python3 -u experiments/error_message_only/run_directory.py \
  --input-dir INPUT/data-4-n166 \
  --output-dir experiments/error_message_only/runs/dataset-1 \
  --dataset dataset-1 \
  --dry-run
```

这里的 `--dry-run` 只预览，不调用模型，也不创建输出目录。去掉该参数后正式执行，目录结构为：

```text
runs/dataset-1/
  Evicted-LowOnResource-repeat/
    <timestamp-id>/
      results.jsonl
      results.json
      requests.jsonl
      summary.json
      progress.json
      ...
  Failed-NoVolumeToMount-repeat/
    <timestamp-id>/
      ...
```

支持 `--limit-per-file N`，每个 CSV 只选前 N 条；小规模测试使用单独输出根目录，例如 `runs/dataset-1-small`，避免把限量运行当成完整实验恢复。可用 `--config` 选择模型配置，其余参数使用 run.py 的默认值。所有新文件会先检查输入格式，避免运行到后面才发现某个 CSV 格式错误。

一个文件有技术失败时保留结果并继续下一个，最后退出码为 1；Ctrl-C 停止当前运行及后续文件，退出码为 130。恢复同一个输出根目录：

```bash
python3 -u experiments/error_message_only/run_directory.py \
  --input-dir INPUT/data-4-n166 \
  --output-dir experiments/error_message_only/runs/dataset-1 \
  --dataset dataset-1 \
  --resume
```

`--resume` 为每个 CSV 选择该文件输出目录下最新的正式运行（忽略 dry-run），使用其输入和设置快照。完成的样本跳过，中断的样本继续；尚无正式运行的文件新建运行。默认不重试已记录的技术失败，追加 `--retry-failed` 才重试这些失败项。还可同时加 `--dry-run` 预览选中的恢复路径。单独恢复某个文件时仍可用 `run.py --resume <timestamp-id目录>`。

未启动文件使用当前默认配置，或命令行指定的 `--config`/`--limit-per-file`；这两个参数不改变已有运行的快照。如果原批次使用了自定义配置或限量，恢复时保留这些参数，使剩余文件使用同样设置。`--resume` 不会把原本只选 N 条的快照扩展成完整 CSV，完整实验应新建输出根目录。

不加 `--resume` 表示新实验，每个文件都会创建新的时间戳目录。不同模型和不同样本选择建议使用不同输出根目录。输出根目录有批次锁，每个子运行也沿用 run.py 的锁，避免同一目录同时写入。

生成器默认 `temperature=0, max_tokens=4096`；Checker 默认 `temperature=0, max_tokens=1536`。这些是输出上限，实际消耗以服务报告的 usage 为准。两组参数分别配置，支持 temperature、max_tokens、max_completion_tokens、seed；同一组不能同时设置两个 token 上限。服务不支持的参数应在相应配置中移除或调整。截断响应会记录为格式错误，不当作完成的报告。

默认 `timeout_seconds=120`，生成器和 Checker 使用同一个 SDK 客户端的网络超时设置；正常响应返回后立即继续，不固定等待 120 秒。延长客户端等待可能减少较慢请求的超时报错，持续无响应时会等待更久才失败；它不能改变服务自身的超时限制。SDK 自动重试仍关闭。已有运行的 `--resume` 沿用 config.json 快照中的值，修改默认配置不会改变其超时设置。

对旧 GPT-4o 保存报告做了单独长度检查：用 `o200k_base` 对两空格缩进的报告 JSON 重新分词，dataset-1 的 1203 份完整报告平均 234 tokens、P99 为 580、最长 901；dataset-2 的 1292 份完整报告平均 218、P99 为 521、最长 571。这包括保存的中间报告和重试报告，数量不是论文样本数；原始响应空白格式未保留，因此是长度估算。检查没有把整个 workflow 的 completion_tokens 当成单份报告长度。4096 留有较大余量，仍需实际生成确认。可用 `tools/audit_report_tokens.py` 重现（额外需要 tiktoken 及编码表），明细保存到 `data/local/report-token-audit.json`。

## 输出、用量与失败

每次运行创建新的目录：

- `results.jsonl`：每行一轮，包含基础身份字段、attempt、destkind、analysis/report、Checker 判断、stop_reason、耗时及 token。
- `results.json`：运行结束时自动生成的缩进 JSON 数组，字段、内容和轮次顺序与 JSONL 一致，便于阅读或用 json.load 读取。
- `requests.jsonl`：两个阶段实际 messages、原始响应、解析输出和错误细节。
- `requests.json`：运行结束时自动生成的缩进 JSON 数组，便于阅读模型输入与响应；内容和顺序与 requests.jsonl 一致。
- `summary.json`：完成样本数、通过数、达到轮次上限的样本数、失败数、实际请求数和已报告 token 总量。
- `progress.json`：运行中持续更新的进度，包括已完成/待处理/失败样本数、当前样本/轮次/阶段及未知用量调用。
- `samples.jsonl`：本次选中输入的快照，恢复时不重新读取外部 CSV。
- `request_starts.jsonl`：每次实际请求开始前持久保存的 request_id 和阶段，用于识别没有保存响应的调用。
- `sessions.jsonl`：首次启动和各次恢复的时间、恢复选项与代码哈希。
- `manifest.json`：模型、服务地址、有效配置、尝试策略、输入/prompt/脚本哈希、Git 和 SDK/Python 版本。
- `config.json`、`prompt.txt`、`checker_prompt.txt`：本次配置及两个实际 prompt 副本。

JSONL 保持每条记录占一行，不在其中加入多行缩进。可读版本使用 `json.dumps(..., indent=2, ensure_ascii=False)`，保留标准 JSON 的 true/false/null；Python pprint 的输出是 Python 对象表示，不适合作为 JSON 数据文件。已有结果也可单独转换，无需调用模型：

```bash
python3 experiments/error_message_only/pretty_json.py PATH/TO/results.jsonl
```

默认在同目录生成同名 `.json`，也可用 `--output` 指定其他路径。runner 会自动为 results.jsonl 和 requests.jsonl 各生成可读版本；已有 requests.jsonl 也可用该工具转换。

### 把各文件的结果集中到一个目录

用 tools/collect_results.py 复制每个输入文件目录下最新正式运行的 results.json，并以输入文件目录名命名：

```bash
python3 experiments/error_message_only/tools/collect_results.py \
  --input-dir experiments/error_message_only/runs/dataset-1-small-v2 \
  --output-dir experiments/error_message_only/runs/collected/dataset-1-small-v2
```

导出的文件例如 `Evicted-LowOnResource-repeat.json`、`FailedMount-StaleNFS-repeat.json`，都直接位于 output-dir。整个结果数组原样复制，保留失败执行和修订轮次，不合并、不筛选，源文件不变。若一个文件有多次独立运行，按 manifest 的 started_at 选最新正式运行，忽略 dry-run；最新运行尚无 results.json 或仍在运行时跳过并提示，不回退到旧结果。

加 `--dry-run` 可预览来源与目标而不复制。再次执行时，内容相同的副本保持不变；恢复实验导致结果变化后，加 `--overwrite` 刷新对应副本。导出副本不会随源结果自动更新。

每条结果仅保留两项成本字段：`time_cost`（秒）和 `token_usage`（prompt_tokens、completion_tokens、total_tokens）。它们合计该结果 `request_ids` 引用的报告生成与检查请求；任一选中阶段未报告用量时，对应总量为 null，历史失败/中断不会覆盖本次成功响应的已知用量。分阶段耗时和 token 可在 `requests.jsonl/requests.json` 中按 request_id 查看；结果中不再重复保存阶段明细、new_*、cumulative_usage 或成本完整性标记。summary 累计整个运行已报告的 token 并标记完整性，不将未知用量当作零。

`status=ok` 表示本轮两个请求与格式有效，不表示原因正确或 Checker 通过；通过与否由 `stop_reason` 和报告里的布尔判断区分。所有轮次保留，后处理按 sample_id 分组、attempt 排序；uuid 可能重复，不能作为样本唯一键。

使用 `--retry-failed` 后，同一个 sample_id/attempt 可能有多个执行记录，`execution` 从 1 递增，保留原来的失败结果。后处理先取每个 sample_id/attempt 最大 execution 的记录，再评估第一轮或最终轮。结果中的成本描述这份报告的生成与检查，包括复用的已保存响应；相同 request_id 表示同一次调用，不能重复累计。

整个运行的实际已报告用量以 `summary.json` 为准，它直接按请求日志统计，包含失败调用的已知消耗，不重复累计复用响应；中断且未保存响应的请求通过 unknown_request_count 和完整性标记注明。按样本计算包含技术重试的成本时，从 requests.jsonl 按 sample_id 分组并按 request_id 去重。

已有带 request_id 的旧结果可通过 `python3 experiments/error_message_only/tools/repair_usage.py RUN_DIR` 从保存的请求记录重建成本字段并去掉冗余字段，无需模型调用。工具先备份原 results.jsonl，再更新 JSONL 和可读 JSON，报告正文与请求日志保持原内容；运行中的目录不能修复。

API 失败、无效 JSON 或截断响应保存后结束该样本，再处理下一条。SDK 自动重试关闭，技术失败不占用后续质量修订轮次。Checker 失败时保留生成报告，评分和继续判断为 null。存在技术失败时退出码为 1；输入/配置错误为 2；达到三轮但解释仍未通过会在结果和 summary 中单独标记，退出码为 0。

## 中断后恢复

本版新启动的实验支持按样本和请求阶段恢复，不需要 Bash 脚本记行号。启动时打印完整 Run directory；结果、响应和请求开始记录每次写入后 flush/fsync，progress 和 summary 使用原子替换更新。查看 progress.json 即可了解当前进度；它是派生信息，恢复实际依据输入快照和持久日志。

进程中断后，指定原来的时间戳运行目录即可：

```bash
python3 experiments/error_message_only/run.py \
  --resume PATH/TO/TIMESTAMP-RUN-DIRECTORY
```

恢复追加到同一目录，不新建实验目录，不重复指定输入/模型/输出参数。它沿用保存的输入、配置和两个 prompt 副本，校验输入及 prompt 哈希，继续使用原来的 OPENAI_BASE_URL；密钥仍从当前环境读取。原始 CSV 移动或外部 prompt 修改不影响这份已保存快照。恢复时的代码哈希记录在 sessions.jsonl。

- 已通过或达到最多轮次数的样本：跳过，不重新调用。
- 报告已保存、Checker 未完成：复用报告，只调用 Checker。
- 两个响应均已保存、但轮次结果尚未写入：从响应重建结果，不再调用模型。
- Checker 已要求修订：带上前次报告和反馈，继续下一轮，仍然最多三轮。
- 已记录的 API/格式错误：默认保留并跳过；若要重试这些失败项，显式添加 `--retry-failed`。只重试失败阶段，不提高质量轮次上限。

```bash
python3 experiments/error_message_only/run.py \
  --resume PATH/TO/TIMESTAMP-RUN-DIRECTORY --retry-failed
```

可以先检查恢复计划而不调用模型、不改变日志（进程停止后执行）：

```bash
python3 experiments/error_message_only/run.py \
  --resume PATH/TO/TIMESTAMP-RUN-DIRECTORY --dry-run
```

该命令显示 samples_to_run；summary/progress 中 completed_samples 表示已有最终结果或已记录技术失败的样本，failed_samples 单独列出，pending_samples 表示尚无最终结果的样本。Ctrl-C 正常中断时退出码为 130；SIGKILL 时无需 finally 执行，下一次恢复仍可从持久日志重建状态。相同运行目录有进程锁，避免两个恢复进程同时写入。

若进程在模型已收到请求但响应落盘前中断，无法保证那一次没有计费。恢复会重新调用未保存的阶段，并保留原请求为 unknown_request。summary 的 reported_total_tokens 只累计已保存且服务报告的用量；unknown_request_count 非零时全局 token_usage_complete/time_cost_complete 为 false。total_time_cost 为已记录调用耗时之和，不包含断开期间的等待。本次成功响应的结果成本字段仍保存其实际数值。仅当所有请求有效、没有技术恢复调用时，manifest 的 max_model_requests 才是该实验的请求次数上限；技术重试可能增加实际次数。

若硬中断留下半条 JSONL 末尾，恢复先将残留字节保存到 `.partial-*` 文件，再修复末尾；日志中间的损坏会报错，不静默丢弃。可读 results.json/requests.json 在退出或恢复完成时重新生成，不用它们作为恢复依据。

首次运行为 dry-run 的目录不能直接变成 live run。旧版本目录（包括之前三条真实测试的目录）缺少输入快照和恢复协议，也不能用 --resume；已有输出照常保留，新实验使用当前 runner 即具备断点恢复能力。

## 输入与对比

历史 Table 5.10 输入已核对：n166 的 data-4 为 619 条，n168 的 data-5-refine 为 843 条。生成 JSONL：

```bash
python3 experiments/error_message_only/tools/audit_inputs.py

python3 experiments/error_message_only/run.py \
  --input experiments/error_message_only/data/local/input-audit/dataset-1.jsonl \
  --limit 1 --dry-run
```

dataset-2 对应同目录的 `dataset-2.jsonl`。详见 `docs/input-provenance.md` 和 `data/README.md`。保留输入行及重复次数以维持论文评估分母。参考答案单独保存，不进入模型请求。

1462 条样本在所有请求有效的情况下，每个模型共需 2924–8772 次调用，取决于 Checker 是否要求修订。比较时分别评估第一轮与最终报告，固定样本、模型和人工评估口径，同时记录图 RCA 可见的额外状态信息。旧脚本依赖图结构，需要针对新结构适配；模型评分不能替代人工评估。
