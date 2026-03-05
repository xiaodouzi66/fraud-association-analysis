# Leiden 社群细分配置参考手册

本文档说明 `scripts/community_detection.py` 中 Leiden 社群细分的触发条件、算法参数、降级策略及输出结构。

---

## 1. 触发条件（`should_run_leiden()`）

Leiden 细分默认**不强制触发**；满足以下任一条件才运行：

| 条件 | 说明 |
|------|------|
| 保单图节点数 ≥ **500** | 大图无条件触发，避免连通分量级别分析粒度过粗 |
| 节点数 ≥ **200** 且图平均风险分 < **0.6** | 中型图但整体风险分较低时，需细分找核心子簇 |
| `--force-leiden` 参数显式传入 | 强制触发，不受节点数限制（适用于调试与测试） |

**不触发时**：`analyze_seed_communities()` 返回 `([], "skipped")`，`cluster_analysis.py` 输出中 `leiden_社群` 字段为空列表。

> **设计思路**：平均风险分已高（≥0.6）且图较小时，连通分量级别的分析已足够精准；Leiden 主要用于挖掘"整体平均分不高但内部存在高风险子簇"的场景。

---

## 2. 算法参数

### 2.1 分辨率（`resolution`）

$$Q = \sum_{c} \left[ \frac{e_c}{m} - \gamma \left(\frac{d_c}{2m}\right)^2 \right]$$

- 默认值：`DEFAULT_LEIDEN_RESOLUTION = 1.0`
- 取值范围：> 0；通常在 0.5 ~ 2.0 之间调整
- 影响：
  - **增大 resolution（如 1.5~2.0）**：社群更小、更多，适合用于精细拆分大型异构社群
  - **减小 resolution（如 0.5~0.8）**：社群更大、更少，适合用于寻找宏观团伙结构
- CLI 参数：`--leiden-resolution`

### 2.2 边权重属性

- 固定使用 `weight` 字段
- 权重值建议取**目标保单传播风险分**（即 `传播边列表.边权重 = 目标保单.传播风险分`）
- 若边权重全为 0，Leiden 等效为无权模块度优化

### 2.3 桥接节点 Top-K

- 默认值：`DEFAULT_TOP_BRIDGE_K = 5`
- 意义：对每个种子子社群，取中介中心性（betweenness centrality）最高的 Top-K 节点输出到 `桥接节点列表`
- CLI 参数：`--top-bridge-k`

### 2.4 最小边权重过滤

- 默认值：`DEFAULT_MIN_EDGE_WEIGHT = 0.05`
- 低于此阈值的边在构建保单图时被丢弃
- CLI 参数（cluster_analysis.py）：`--min-edge-score`

---

## 3. 依赖库与降级策略

Leiden 算法依赖 `leidenalg` + `igraph`，为可选依赖：

```
安装（可选）：
pip install leidenalg python-igraph
```

| 情况 | 实际使用算法 | 输出中 `leiden_算法` 字段 |
|------|------------|--------------------------|
| leidenalg 已安装 | `leidenalg.RBConfigurationVertexPartition` | `"leidenalg"` |
| leidenalg 未安装 | `networkx.community.greedy_modularity_communities`（Louvain近似） | `"louvain-fallback"` |
| 图为空 | 跳过 | `"empty"` |
| 未满足触发条件 | 跳过 | `"skipped"` |

---

## 4. 种子社群定位逻辑

Leiden 运行后，只关注**种子保单所在社群**，过滤掉无关社群：

1. 从 `保单节点列表` 中提取 `是否种子 = true` 的保单号
2. 若无显式种子保单，从 `种子案件.保单号` 补充
3. 在 Leiden 划分结果中找到这些保单所属的 `comm_id`
4. 只对这些 comm_id 进行详细指标计算和结果输出

---

## 5. 输出结构（`SubCommunityResult`）

每个种子社群对应一个 `SubCommunityResult` dataclass，序列化后字段如下：

| 字段 | 类型 | 说明 |
|------|------|------|
| `社群编号` | string | 如 `comm_0`、`comm_2` |
| `包含种子节点` | bool | 是否包含种子保单 |
| `规模` | int | 社群内保单数 |
| `边数` | int | 社群内传播边数 |
| `涉及案件数` | int | 社群内挂载案件总数 |
| `涉及赔付金额` | float | 社群内案件赔付金额合计 |
| `基础指标` | dict | 8项基础指标字典（同连通分量级别） |
| `扩展指标` | dict | 4项 MO 扩展指标（见 metrics_reference.md） |
| `社群风险分` | float | 综合风险分，计算见社群风险分公式 |
| `桥接节点列表` | list | Top-K 高中介中心性节点 |
| `关联类型分布` | dict | 各关联类型边数统计 |
| `传播路径描述` | string | 自然语言描述（自动生成） |
| `高相似案件列表` | list | mo_score ≥ 0.6 的案件明细，按 mo_score 降序 |
| `置信度原因` | list | 触发扩展指标阈值的语言描述 |

---

## 6. 社群风险分公式

$$\text{社群风险分} = 0.60 \times \text{gang\_score}_{base} + \sum_{i} w_{ext,i} \times m_{ext,i}$$

扩展指标权重：

| 扩展指标 | 权重 |
|----------|------|
| `mo_similarity_avg` | 0.20 |
| `mo_similarity_high` | 0.05 |
| `hospital_conc` | 0.08 |
| `amount_cluster` | 0.07 |
| **扩展合计** | **0.40** |

> 基础 gang_score 使用与 cluster_analysis.py 完全相同的8项指标和权重，再乘以缩放系数 0.60，确保基础与扩展两部分之和上限为 1.0。

---

## 7. CLI 快速参考

```bash
# 作为独立模块运行（需先生成 mo_scores 文件）
python scripts/community_detection.py sample_input.json \
  --mo-scores output/mo_scores.json \
  --output output/leiden_results.json \
  --resolution 1.0 \
  --min-edge-weight 0.05 \
  --top-bridge-k 5 \
  --force-leiden        # 强制触发，忽略节点数阈值

# 通过 cluster_analysis.py 集成调用（推荐）
python scripts/cluster_analysis.py sample_input.json \
  --output output/cluster_v2_output.json \
  --force-leiden \
  --leiden-resolution 1.0 \
  --top-bridge-k 5
```
