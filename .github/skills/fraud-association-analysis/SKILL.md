````skill
---
name: fraud-association-analysis
description: 分析用户描述的可疑保险赔案，判断风险传播网络中是否存在团伙/聚集性欺诈风险，并解读社群关联机制。触发条件：(1) 用户用自然语言或结构化标签描述一个可疑案件（如"住院天数34天，膝挫伤，疑似小病久住"），要求判断是否存在聚集性风险；(2) 用户只给出案件号/保单号，让你调查关联欺诈风险；(3) 用户已有三层 JSON 输入（种子案件+保单图+案件列表），直接运行聚类分析。不触发：单纯保单查询、理赔进度查询、非欺诈相关的案件处理。
---
# 反欺诈关联分析 Skill

从用户输入的可疑案件描述出发，经过四个阶段自动完成：
输入解析 → 数据组装 → 图计算聚类 → 社群智能解读，输出聚集性风险结论与调查建议。

---

## 工作流总览（四阶段）

```
用户输入（自然语言/案件号/完整JSON）
        │
        ▼ Stage 0
[输入解析] 提取种子案件结构化信息 + 欺诈假设
  → 详见 references/01_input_parsing.md
        │
        ▼ Stage 1（如需）
[数据组装] 按案件号/保单号查询 ODPS，拉取保单传播图和关联案件
  → 详见 references/02_db_schema.md
        │
        ▼ Stage 2
[图计算] MO评分 → 聚类 → Leiden子社群 → 基础报告
  → 见下方"Stage 2 详细"
        │
        ▼ Stage 3
[智能解读] 基于欺诈假设，解读各社群关联机制与跨社群共性信号
  → 详见 references/04_interpret.md
```

**入口判断规则**：
- 用户给自然语言描述 → 从 Stage 0 开始
- 用户只给案件号/保单号 → Stage 0 仅提取 ID，Stage 1 查 DB 补全
- 用户已给完整三层 JSON → 直接跳至 Stage 2

---

## Stage 2 详细（图计算，三步）

### 步骤 2-1：MO 相似度评分

```bash
python scripts/modus_operandi.py \
  --input sample_input.json \
  --seed-mo seed_mo.json \
  --output output/mo_scores.json
```

- 提取种子案件 MO 特征：疾病分组、住院类型、LOS超阈值、医院、金额区间、治疗手段
- 对所有关联案件批量计算 `mo_score`（0~1），key 为案件号
- **MO软匹配规则**: 7个语义分组 + ICD-10前缀 + 关键词匹配 → 读取 [references/modus_operandi_reference.md](references/modus_operandi_reference.md)

### 步骤 2-2：聚类分析（主流程）

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

### 步骤 2-3：基础报告生成

```bash
python scripts/generate_report.py output/cluster_v2_output.json \
  --raw-input sample_input.json \
  --viz-dir output \
  --report output/report_base.md
```

生成结构化中文 Markdown 报告（数据层，可复现）：

| 段落               | 内容                                                                            |
| ------------------ | ------------------------------------------------------------------------------- |
| **一、聚集性结论** | 风险等级、相似社群数、可疑案件数、最高团伙分                                    |
| **二、社群明细**   | 每个社群：规模/关联类型/桥接节点/可疑案件列表（MO分/医院/疾病/天数/金额）      |
| **三、传播概览**   | 总保单/案件/赔付、分层统计、关联类型分布、MO相似度分布                          |

**可选：社群可视化**

```bash
python scripts/visualize_community.py sample_input.json \
  --leiden-results output/leiden_results.json \
  --output-dir output
```

---

## 指标与评分

**8项基础指标**（规模/密度/平均分/高风险占比/突增/疾病集中度/重复率/模式分）→ gang_score
**4项扩展指标**（MO均值/MO高占比/医院集中度/金额集中度）→ 社群风险分

**3档风险等级**: ≥0.7 高危（立即复核）、0.4~0.7 中危（补充调查）、<0.4 低危（持续监控）

**查阅详细定义、权重、公式、强规则**: 读取 [references/metrics_reference.md](references/metrics_reference.md)

---

## 脚本总览

| 脚本                             | 功能                                     |
| -------------------------------- | ---------------------------------------- |
| `scripts/extract_from_text.py`   | 从自然语言/标签文本提取种子案件 JSON      |
| `scripts/query_claim_db.py`      | 按案件号/保单号查询 ODPS 补全字段        |
| `scripts/modus_operandi.py`      | MO 特征提取 + 批量评分                   |
| `scripts/cluster_analysis.py`    | **主流程**：图构建 + 聚类 + Leiden 集成  |
| `scripts/community_detection.py` | Leiden 子社群细分（可单独调用）          |
| `scripts/visualize_community.py` | 社群 PNG 可视化（networkx + matplotlib） |
| `scripts/generate_report.py`     | 结构化中文报告生成（数据层）             |

---

## 注意事项

- 最大输入节点数：5000（超限时返回 `error: too_many_nodes`，建议分批处理）
- 所有分析结果须写入审计日志（含执行时间戳、参数快照）
- 手机号/身份证号等敏感字段在展示前按权限脱敏
- Stage 3 智能解读结论须标注置信度，不确定时写"需进一步核实"，禁止编造数据

````