---
name: fraud-association-analysis
description: Detect insurance gang/cluster fraud through MO similarity analysis, graph clustering, and Leiden sub-community detection. Trigger when (1) user provides three-layer input (seed case + policy graph + claim list); (2) user asks about "gang fraud", "cluster risk", "collusion", or "modus operandi similarity"; (3) user wants sub-community assignment and MO comparison. Outputs in Chinese gang_score, Leiden communities, MO scores, risk report, optional PNG. Do NOT trigger for single-case scoring or general policy queries.
---
# 反欺诈关联分析 Skill

本 Skill 基于**三层输入**（种子案件 + 保单传播图 + 关联案件），自动完成
MO 相似度评分 → 团伙聚类 → Leiden 子社群细分 → 三段式中文报告。

---

## 一、输入结构（三层 JSON）

示例文件见 `sample_input.json`。


| 层    | 字段                          | 说明                                                   |
| ----- | ----------------------------- | ------------------------------------------------------ |
| 第1层 | `种子案件`                    | 案件号/保单号 + MO特征（疾病/医院/天数/金额/治疗手段） |
| 第2层 | `保单节点列表` + `传播边列表` | 保单同构图，边为相邻跳 B→C 格式                       |
| 第3层 | `关联案件列表`                | 有案件的保单对应案件记录，一保单可多案件               |

**完整字段说明**: 读取 [references/metrics_reference.md](references/metrics_reference.md)

---

## 二、三阶段工作流

### 阶段 1：MO 相似度评分

```bash
python scripts/modus_operandi.py sample_input.json --output output/mo_scores.json
```

- 提取种子案件 MO 特征：疾病分组、住院类型、LOS超阈值、医院、金额区间、治疗手段
- 对所有关联案件批量计算 `mo_score`（0~1），key 为案件号
- **MO软匹配规则**: 7个语义分组 + ICD-10前缀 + 关键词匹配 → 读取 [references/modus_operandi_reference.md](references/modus_operandi_reference.md)

### 阶段 2：聚类分析（主流程）

```bash
python scripts/cluster_analysis.py sample_input.json \
  --output output/cluster_v2_output.json \
  --force-leiden \
  --leiden-resolution 1.0
```

脚本完成：

1. 从 `保单节点列表` + `传播边列表` 构建**保单同构图**
2. 调用 `modus_operandi.batch_score()` 获取案件级 MO 评分
3. 按连通分量计算 **8 项基础指标**并得出 `gang_score`，叠加 MO 增强权重
4. 强规则判断：黑名单命中 / 医院重复 / burst 突增
5. 超级节点 ego 拆分（度数 > 50 时）
6. 按 Leiden 触发条件判断是否运行子社群细分
7. 输出 `clusters` + `leiden_社群` + `mo_scores`

**Leiden触发条件**（任一满足）：节点≥500 或（节点≥200 且 avg_risk<0.6）或 `--force-leiden`
**详细配置与降级策略**: 读取 [references/leiden_config_reference.md](references/leiden_config_reference.md)

### 阶段 3：报告生成

```bash
python scripts/generate_report.py output/cluster_v2_output.json \
  --raw-input sample_input.json \
  --viz-dir output \
  --report output/report_v2.md
```

生成三段式中文 Markdown 报告：


| 段落                 | 内容                                                                              |
| -------------------- | --------------------------------------------------------------------------------- |
| **一、综合结论**     | 风险等级、结论描述、置信度、最高分值、关键依据                                    |
| **二、风险详情**     | 连通分量明细（gang_score、指标表格）+ Leiden 子社群明细（桥接节点、高相似案件表） |
| **三、传播全局概述** | 总保单/案件/赔付金额、分层统计、关联类型分布、MO相似度分布                        |

**可选：社群可视化**

```bash
python scripts/visualize_community.py sample_input.json \
  --leiden-results output/leiden_results.json \
  --output-dir output
# 输出：output/community_comm_0.png 等
```

---

## 三、指标与评分

**8项基础指标**（规模/密度/平均分/高风险占比/突增/疾病集中度/重复率/模式分）→ gang_score
**4项扩展指标**（MO均值/MO高占比/医院集中度/金额集中度）→ 社群风险分

**查阅详细定义、权重、公式和计算示例**: 读取 [references/metrics_reference.md](references/metrics_reference.md)

---

## 四、强规则与分值解读

**3条白盒强规则**（黑名单/医院重复/突增）可覆盖 gang_score 至 ≥0.75
**3档风险等级**: ≥0.7高危（立即复核）、0.4~0.7中危（补充调查）、<0.4低危（持续监控）

**查阅强规则详情和推荐动作**: 读取 [references/metrics_reference.md](references/metrics_reference.md) 末尾章节

---

## 五、脚本总览


| 脚本                             | 功能                                     |
| -------------------------------- | ---------------------------------------- |
| `scripts/modus_operandi.py`      | MO 特征提取 + 批量评分                   |
| `scripts/cluster_analysis.py`    | **主流程**：图构建 + 聚类 + Leiden 集成  |
| `scripts/community_detection.py` | Leiden 子社群细分（可单独调用）          |
| `scripts/visualize_community.py` | 社群 PNG 可视化（networkx + matplotlib） |
| `scripts/generate_report.py`     | 三段式中文报告生成                       |

---

## 六、注意事项

- 最大输入节点数：5000（超限时返回 `error: too_many_nodes`，建议分批处理）
- 所有分析结果须写入审计日志（含执行时间戳、参数快照）
- 结果中手机号/身份证号等敏感字段在展示前按权限脱敏
- 参考文档：`references/metrics_reference.md`、`references/modus_operandi_reference.md`、`references/leiden_config_reference.md`
