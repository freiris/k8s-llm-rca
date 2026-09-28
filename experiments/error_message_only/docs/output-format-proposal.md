# Message-only 输出约定

已实现生成报告后交给新的 MessageOnlyReportQualityChecker，依据报告能否充分解释消息决定是否修订，最多 3 轮。检查通过就停止，不做固定三次独立采样。runner 和两个阶段的 prompt 已接入此流程，后处理脚本仍需针对新结构适配。当前 Checker prompt 位于 prompts/report_quality_v3.txt。

## 一次分析尝试对应一条结果

```json
{
  "schema_version": "1.0",
  "method": "error_message_only",
  "sample_id": "dataset-1:FailedScheduling:UnboundPVC:0001",
  "dataset": "dataset-1",
  "reason": "FailedScheduling",
  "type": "UnboundPVC",
  "error_message": "pod has unbound immediate PersistentVolumeClaims",
  "namespace": "<原始 namespace>",
  "timestamp": "<原始 timestamp>",
  "uuid": "<原始 uuid>",
  "attempt": 1,
  "destkind": "PersistentVolumeClaim",
  "analysis": [
    {
      "report": {
        "summary": [
          {
            "kind": "PersistentVolumeClaim",
            "explanation": "The message states that an immediate-binding PVC is unbound; the reason it remains unbound is not established."
          }
        ],
        "conclusion": "Most likely the Pod cannot be scheduled because a required immediate-binding PVC has not bound. The message alone cannot distinguish an unavailable matching PV from a provisioning or configuration issue.",
        "resolution": "Identify the PVC referenced by the Pod and inspect its status and events. If no compatible PV is available, provision compatible storage; if dynamic provisioning failed, correct the verified StorageClass or provisioner issue.",
        "overall_score": 7.0,
        "further_investigation": true,
        "quality_feedback": "The explanation does not yet describe how to distinguish the alternative storage causes using the relevant diagnostic checks.",
        "missing_information": [
          "The Pod's PVC references",
          "PVC status and events",
          "PV availability and StorageClass configuration"
        ]
      }
    }
  ],
  "status": "ok",
  "stop_reason": "quality_retry",
  "time_cost": 1.25,
  "token_usage": {
    "prompt_tokens": 180,
    "completion_tokens": 220,
    "total_tokens": 400
  }
}
```

以上为合成示例，计时和 token 数仅示意。namespace、timestamp、uuid 由程序原样复制，不由模型生成。sample_id 标识输入行出现次数，不能用 uuid 代替；输入中有重复 UUID 和重复完整样本。

## 程序字段与模型字段

程序生成：schema_version、method、sample_id、dataset、reason、type、error_message、namespace、timestamp、uuid、attempt、status、time_cost、token_usage。

生成器输出 destkind 和 report 的 summary、conclusion、resolution、missing_information。新 Checker 独立读取消息和本轮候选报告，输出 overall_score、further_investigation、quality_feedback。程序将检查结果合并到 report，保存 analysis。生成器的自评分不能替代 Checker 结果。

根因分析只依据固定任务说明和原始 error_message，不读取 stategraph/metagraph 或人工标签。输出中保留 namespace 等字段不代表它们进入模型输入，reason/type 也不能作为隐藏提示传入。

不提供图查找得到的 srckind。destkind 指向与问题最直接相关、最可能导致故障的单个 Kubernetes API 或外部资源类型，不能仅因某资源承受或报告故障就选择它。模型考虑资源之间的交互和依赖关系，所选 kind 与 conclusion 中的原因一致。它是消息推断的预测，不表示真实根因已验证；无法合理确定时为 JSON null。生成器和 Checker 只使用这个通用定义，不添加具体资源类型或故障场景示例，不提供来自图数据库的候选类型清单。

v2 借鉴 find_metapath/find_srckind_destkind_metapath.py 中 setup_root_cause_locator/build_prompt_template 的通用目标：从消息及资源交互/依赖推测原因，定位最关键的根因相关资源，再解释原因如何导致故障。保留 message-only 的输入边界与不确定性表达，不复制旧 prompt 中的具体示例、名称后缀提示、图中的候选类型清单或查图得到的 involved_object。Checker 同步检查 destkind 的根因语义及其与 conclusion 的一致性。

当前默认 v3 只在 v2 基础上明确 resolution 提供“针对最可能原因的可操作建议”，Checker 同步此要求；不新增固定步骤、分支模板或强制命令要求。v1/v2 文件保留供对照。已有运行通过 --resume 继续使用其保存的 prompt 快照。若比较新旧规则，使用不同输出根目录新建实验，不在同一批次混用版本。

## report 的含义

- summary：基于消息的解释或推理，不作为检索到的集群状态证据。保持 kind/explanation 的列表结构，便于人工阅读；可为空。不强制新增各项 relevance_score。
- conclusion：给出最可能的直接原因及无法确认的更深层原因，标明推测与消息直接陈述的内容。没有图证据，但消息本身仍可提供错误类型、资源名或具体数值等线索。
- resolution：针对最可能原因的可操作建议，包括相关排查与条件性的处置建议。不得虚构资源名、namespace、命令执行结果或实际集群配置。消息没有的具体标识使用占位符。
- overall_score：新 Checker 对报告解释质量的评分，保存为 0–10 的 JSON 数字，避免字符串 8/10。它不是根因正确概率或人工正确率。评分量表记录在 checker prompt，采用 0–3 / 4–6 / 7–8 / 9–10 四档。
- further_investigation：新 Checker 决定是否再次生成报告，延续原实验的执行含义。true 表示当前报告解释不充分，需要修订；false 表示对仅有消息的解释已足够。它不表示实际集群根因已验证。
- quality_feedback：解释质量判断的理由；需要重跑时指出具体不足，作为下一轮修订输入。程序不以某个未约定的 score 阈值代替 Checker 的布尔决定。
- missing_information：列出若要实际确认根因还缺哪些数据；若消息已明确说明足够的原因，可为空。字段不表示程序已查询这些数据。

method=error_message_only 已明确来源，不构造虚假的 statepath、empty_statepath、metapath、clue 或 support_evidence 来兼容旧代码。

## attempt、预算和失败

attempt 表示本输入行的第几轮“报告生成 + 检查”，从 1 开始，最多 3。第一轮生成报告后检查；false 时结束，true 且未满 3 轮时结合原消息、前次报告和质量反馈修订，再次检查。第 3 轮结束时保留 Checker 的真实判断，不强行把 true 改成 false。每轮保存一条结果，不只保存最后一轮。

用户明确暂不研究固定三次独立采样。本设计是按解释质量自适应修订，不是无条件跑三次。可分别记录第一轮结果、最终结果及按尝试顺序累积的人工正确率。新 Checker 仅检查消息、推理和建议；不单因缺少外部数据而拒绝报告，也不假装执行了集群验证。

每轮一般包含两次模型调用。对 1462 个输入行，在每轮请求均有效的情况下，每个模型至少 2924 次调用（全部首轮通过），最多 8772 次调用（全部运行三轮）。结果中的 time_cost 与 token_usage 合计生成器和 Checker 两部分，分阶段明细只保留在 requests 日志。请求失败直接记录并结束本样本，不做自动技术重试。

stop_reason 为 quality_passed（通过）、quality_retry（进入下一轮）、max_attempts_reached（仍需修订但预算耗尽）、generator_error、checker_error 或 dry_run。status=ok 不等于检查通过。配置可将 max_attempts 降为 1 或 2，不能超过 3。

保留所有输入行，保证与论文评估样本一致。数据中既有同类但不同的消息，也有完全重复样本；这些重复不能被解释为不同故障事件。可补充每类唯一消息/样本身份数量，但不擅自去重或改变主评估分母。

当前 SDK 自动重试关闭。API 失败或无效 JSON 的技术重试与质量触发的报告修订不同；已实现的显式恢复通过 execution 记录技术执行次数，通过 requests 日志与 summary 累加已保存调用的实际耗时与已报告 token。失败不能被 Checker 判定为报告质量通过。

已实现的显式技术恢复入口是 --resume RUN_DIR --retry-failed；默认 --resume 不重复已记录的失败项。技术重试沿用同一 attempt，execution 递增；原失败结果保留，后处理每个 sample_id/attempt 取最后 execution。request_id 标识每次调用，结果只保留 token_usage/time_cost 两项成本字段，对应这条结果 request_ids 引用的生成与检查请求（包括复用响应）。历史失败不会把本次成功请求的用量覆盖为 null。

结果中不再保存重复的阶段明细、new_*、cumulative_usage 或成本完整性标记。summary 直接按请求日志累计整个运行的已报告用量，包含失败调用的已知消耗，不重复累计复用响应。按样本统计含技术重试的成本时，从 requests 按 sample_id 分组、按 request_id 去重，不能简单累计同一轮的多次执行结果。

恢复沿用 samples.jsonl、配置与 prompt 快照；每次请求与轮次结果均持久记录，progress.json 为派生进度。响应尚未保存的调用保留为 unknown_request，不能保证服务未计费；summary 保存完整性标记。本次有有效响应的已知用量仍保留在结果成本字段中。具体命令、失败策略和日志修复规则见 README 的“中断后恢复”。

失败仍保存身份字段、attempt、time_cost、status、error。不填伪报告；无有效报告时 analysis=[]、destkind=null；Checker 失败时保留生成报告，评分与继续判断为 null，标记检查失败。token_usage 未报告时填 null，不填 0；阶段用量缺失时总量不伪装成完整已知值。status=ok 表示生成及检查请求与格式有效，不表示结论经人工确认正确。

## 文件和后处理

- 主输出 results.jsonl：每行一条尝试记录。按需要导出每个 Reason-Type 的标准 JSON 数组给 json.load 使用，不写逗号拼接的非标准 JSON。
- 可读输出 results.json：运行结束时自动导出全部尝试记录的缩进 JSON 数组，字段和顺序与 results.jsonl 一致；不改变 JSONL 的单行记录约定。已有 JSONL 可用 pretty_json.py 单独转换。
- requests.jsonl 等调试文件：保存实际 messages、raw_response、finish_reason 和解析错误细节，不挤入人工标注表。
- requests.json：运行结束时自动导出 requests.jsonl 的缩进 JSON 数组，便于检查两个阶段的模型输入与响应。
- manifest.json：模型和服务标识、prompt、配置、代码与输入哈希、尝试协议；用于复现。
- 新标注入口读取 analysis[*].report，保留 conclusion/resolution 并增加人工评估栏。模型不得生成 human_label。
- 新统计入口保留 time_cost、token_usage；不计算 locator_attempts、metapaths、cypher_attempts 等不存在的步骤。
- precision 汇总按 sample_id 分组，按 attempt 排序，不能按 uuid 分组。标注为空时明确视为未标注，不能将 True/False 占位字符串计为正确。
- 原有 report 的 conclusion/resolution/overall_score/further_investigation 名称保留。旧 get_simple_for_label、extract_statistic、calculate_error_type_strict 都依赖图结构，需通过适配函数或实验专用脚本处理新结构。
