# Table 5.10 输入数据溯源与原 CSV 核对

本次核对 Mac 仓库中的 INPUT、full-test 和四组 Azure 输出。没有修改原始 CSV 或模型输出，没有调用模型。论文的 #Correct 与 Precision 未重新计算。

## 已确认输入

| 数据集 | 来源机器及原路径 | Mac 原始副本 | 类型数 | 样本数 |
| --- | --- | --- | ---: | ---: |
| dataset-1 | n166: full-test/data-4 | INPUT/data-4-n166 | 18 | 619 |
| dataset-2 | n168: full-test/data-5-refine | INPUT/data-5-refine | 20 | 843 |

用户提及 data-5-refine-n168，但当前实际复制到 Mac 的目录名是 INPUT/data-5-refine。本轮保持该目录名，并在 manifest 中明确标记来源机器和原路径。目录名不同不影响内容核对。

这两组是论文 RCA 评估子集，不是原始数据集的全部 Warning 事件。INPUT/data-2 是旧处理阶段的目录，不是论文 dataset-2。

## dataset-1 与 Mac 旧副本

- n166 复制的 18 个 CSV，与 full-test/data-4 的对应文件逐字节相同。full-test 另有一个空的 NoSuchFileDir-crash CSV，不计入 18 类。
- 这 18 个 CSV 与 INPUT/data-4 对应文件也逐字节相同；部分文件名不同，例如 n166 的 FailedCreate-ExceedQuota-Job.csv 对应 INPUT/data-4/FailedCreate-ExceedQuota-Job-2-3.csv。
- 表头、所有 CSV 列、数据行、行顺序及重复次数均保留。详细映射和 SHA-256 在 data/local/input-audit/manifest.json。
- 18 类样本数逐项符合 Table 5.10，共 619 条。

## dataset-2 与 Azure 输出

- 使用 CSV parser 读取 n168 原 CSV，排除 20 行表头后为 843 条，20 类数量逐项符合 Table 5.10。
- GPT-4o 核对范围：OUTPUT-server-5-azure/output-5-bracket 与 output-6-bracket。原 CSV 的 843 个输入行全部能按 message、namespace、timestamp、uuid 找到对应输出。
- 两边不同消息的集合完全相同：各 482 条，无缺失、无额外消息。namespace + message 的不同组合也完全一致（773 个）。
- 两边完整四字段身份的集合完全相同：各 819 个，无缺失、无额外样本身份。输入仍是 843 行，因为原输入中存在重复样本。
- GPT-4o 输出共有 1051 条记录，包含不同尝试和重复记录，因此不能要求其原始记录数等于输入行数。
- 与 OUTPUT-server-5-mini-azure/output-1220-mini-merge-bracket 对照：保留 attempt == 1 后，各类完整四字段身份、重复次数和行顺序都与 n168 原 CSV 完全一致，共 843 条。
- 原 CSV 表头为 namespace2,message,timestamp,uuid,name2；转换统一前四列名称，不给模型添加额外上下文。

## 事件时间

- dataset-1：`2020-12-08 18:50:02.008` 至 `2020-12-14 19:10:02.014`。
- dataset-2：`2023-01-09 10:21:28.009` 至 `2023-06-15 20:00:08.015`。
- dataset-2 事件时间是 2023 年；目前没有证据确认 dataset-2022 这个别名，继续使用 dataset-1/dataset-2。

## 类型数量与来源

ClaimLost/PVLost 对应历史文件名 ClaimLost-PV-new-repeat-*；UnboundPVC / UnBoundPVC 大小写统一为表中名称。

| Reason | Type | dataset-1 | dataset-2 | n166 CSV | n168 CSV |
| --- | --- | ---: | ---: | --- | --- |
| ClaimLost | PVLost | 0 | 23 | — | ClaimLost-PV-new-repeat-2.csv |
| Evicted | LowOnResource | 20 | 35 | Evicted-LowOnResource-repeat.csv | Evicted-LowOnResource-comb-2.csv |
| Evicted | NodeDiskPressure | 24 | 24 | Evicted-NodeDiskPressure-repeat.csv | Evicted-NodeDiskPressure.csv |
| Failed | AccessDenied | 39 | 0 | Failed-AccessDenied.csv | — |
| Failed | ArtifactNotFound | 20 | 22 | Failed-ArtifactNotFound-repeat.csv | Failed-ArtifactNotFound.csv |
| Failed | NetworkUnreachable | 21 | 0 | Failed-NetworkUnreachable-repeat.csv | — |
| Failed | NoVolumeToMount | 24 | 37 | Failed-NoVolumeToMount-repeat.csv | Failed-NoVolumeToMount.csv |
| FailedCreate | ExceedQuotaJob | 45 | 46 | FailedCreate-ExceedQuota-Job.csv | FailedCreate-ExceedQuota-Job.csv |
| FailedCreate | ExceedQuotaReplicaSet | 32 | 54 | FailedCreate-ExceedQuota-ReplicaSet.csv | FailedCreate-ExceedQuota-ReplicaSet.csv |
| FailedCreate | ExceedQuotaStatefulSet | 20 | 44 | FailedCreate-ExceedQuota-StatefulSet-repeat.csv | FailedCreate-ExceedQuota-StatefulSet.csv |
| FailedCreate | ServiceAccountNotFound | 32 | 40 | FailedCreate-ServiceAccountNotFound.csv | FailedCreate-ServiceAccountNotFound-comb.csv |
| FailedMount | ConfigMapNotFound | 43 | 58 | FailedMount-ConfigMapNotFound.csv | FailedMount-ConfigMapNotFound.csv |
| FailedMount | FailedSyncConfigMapCache | 56 | 64 | FailedMount-FailedSyncConfigMapCache.csv | FailedMount-FailedSyncConfigMapCache.csv |
| FailedMount | FailedSyncSecretCache | 57 | 60 | FailedMount-FailedSyncSecretCache.csv | FailedMount-FailedSyncSecretCache.csv |
| FailedMount | NoSuchFileDir | 47 | 42 | FailedMount-NoSuchFileDir.csv | FailedMount-NoSuchFileDir.csv |
| FailedMount | ObjectNotRegistered | 24 | 34 | FailedMount-ObjectNotRegistered-repeat.csv | FailedMount-ObjectNotRegistered.csv |
| FailedMount | PVCNotBound | 0 | 56 | — | FailedMount-PVCNotBound-new.csv |
| FailedMount | SecretNotFound | 56 | 55 | FailedMount-SecretNotFound.csv | FailedMount-SecretNotFound.csv |
| FailedMount | ServiceAccountNotFound | 0 | 25 | — | FailedMount-ServiceAccountNotFound-new.csv |
| FailedMount | StaleNFS | 21 | 0 | FailedMount-StaleNFS-repeat.csv | — |
| FailedScheduling | PVCNotFound | 0 | 34 | — | FailedScheduling-PVCNotFound-new.csv |
| FailedScheduling | UnboundPVC | 38 | 71 | FailedScheduling-UnboundPVC.csv | FailedScheduling-UnBoundPVC.csv |
| OutOfpods | NodeNotEnough | 0 | 19 | — | OutOfpods-NodeNotEnough-new.csv |
| Total | | 619 | 843 | | |

## 换行、重复和旧结果差异

- n168 的 wc -l 为 863，减去 20 个表头得到 843；原 CSV 已正式解析确认。
- n166 的 wc -l 为 620：LowOnResource 文件末尾有换行，另外 17 个无末尾换行。CSV parser 读取是 619 条样本，不应统一从 wc -l 总数减去表头数。
- 保留重复输入行。例如 dataset-1 LowOnResource 有 20 个输入行，但只有 2 个不同的完整四字段身份。生成的 sample_id 用数据集、类型和行编号，避免 UUID 重复导致合并。
- dataset-1 mini 的历史差异仍保留记录：AccessDenied 的 namespace + message 多重集合与 CSV 一致，但 5 个行在 timestamp / uuid 上不匹配；UnboundPVC 的 38 个样本中，四字段多重集合及 namespace + message 多重集合均只有 33 个匹配。严格比较旧 mini 结果时仍需调查该问题。新 baseline 从原始 CSV 读取，不从这些旧结果反推输入。
- 输出根目录与模型的对应关系沿用用户实验记录和目录命名，原输出 JSON 未保存实际模型版本信息。

## 新实验输入

- data/local/input-audit/dataset-1.jsonl：直接从 INPUT/data-4-n166 转换，619 行。
- data/local/input-audit/dataset-2.jsonl：直接从 INPUT/data-5-refine 转换，843 行。
- 两份均标记 original_csv，携带原文件路径及记录编号，不包含模型报告或人工标签。baseline runner 只使用 sample_id 与 error_message。
- 之前的 dataset-2-recovered.jsonl 是输出恢复版，保留作历史核对，正式实验应使用 dataset-2.jsonl。
- 两份原始转换输入已通过完整 loader 校验与各取一条的 dry-run；未调用模型。数据和生成文件默认被 Git 忽略，源文件哈希记录在 manifest。

## 复现

```bash
python3 experiments/error_message_only/tools/audit_inputs.py
```

脚本自动读取本轮复制目录。可用 --dataset1-dir 和 --dataset2-dir 指定其他仓库内路径。输出包括 manifest.json、counts.csv 和两份原始 CSV 转换 JSONL。
