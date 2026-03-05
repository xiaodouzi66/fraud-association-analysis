# 判定规则参考手册（Rules Reference）

本文档定义 `fraud-association-analysis` Skill 使用的白盒规则引擎，
所有规则可直接导入规则平台（YAML 格式可用于 Drools / OPA / 自研引擎）。

---

## 规则分层结构

```
Level 1：强规则（Strong Rules）    → 任意命中直接覆盖 gang_score ≥ 0.7，立即人工复核
Level 2：结构规则（Structure Rules）→ 多条件组合，命中加分
Level 3：行为规则（Behavior Rules） → 行为模式异常，命中加分 + 添加解释
Level 4：辅助规则（Context Rules）  → 外部信号辅助，命中加注，不单独触发
```

---

## Level 1：强规则（立即高危）

### S-01：黑名单命中规则
```yaml
rule_id: S-01
name: blacklist_hit_with_high_risk
description: 任一组件成员在司法/保险黑名单中，且高风险节点占比>=25%
conditions:
  - any(member.blacklisted == true)
  - high_risk_ratio >= 0.25
action:
  gang_score: max(current, 0.80)
  recommended_action: manual_review
  confidence_reason: "命中黑名单且高风险节点占比≥25%，建议立即人工复核"
severity: CRITICAL
```

### S-02：短期批量同收款规则
```yaml
rule_id: S-02
name: same_payee_burst_with_disease_conc
description: 同一收款账户在7天内被3+个不同保单收款，且诊疗项目高度集中
conditions:
  - same_payee_count_in_7d >= 3
  - disease_conc >= 0.6
action:
  gang_score: max(current, 0.75)
  recommended_action: manual_review
  confidence_reason: "同一收款账户短期密集收款，叠加诊疗高度集中，疑似模板化团伙作案"
severity: CRITICAL
```

### S-03：高速突增规则
```yaml
rule_id: S-03
name: high_burst_with_shared_hub
description: 24小时内新增高风险节点>=4个，且这些节点共用同一代理人或收款账户
conditions:
  - burst_score >= 0.75     # 对应 max_count >= 3
  - size >= 4
  - shared_agent_or_payee == true
action:
  gang_score: max(current, 0.75)
  recommended_action: manual_review
  confidence_reason: "短时间内高风险节点急速增长，共用同一代理/收款，疑似集中作案"
severity: CRITICAL
```

### S-04：证据重复规则
```yaml
rule_id: S-04
name: duplicate_evidence_across_policies
description: 同一张照片哈希/文档指纹出现在3+个不同被保人的理赔材料中
conditions:
  - photo_hash_collision_count >= 3
action:
  gang_score: max(current, 0.72)
  recommended_action: manual_review
  confidence_reason: "多份理赔材料使用相同影像文件，存在伪造证据嫌疑"
severity: CRITICAL
```

---

## Level 2：结构规则（组合加分）

### R-01：密度+传播+集中度组合
```yaml
rule_id: R-01
name: dense_high_suspicion_cluster
description: 高密度+高平均传播分+高疾病集中度，提示结构性团伙特征
conditions:
  - density >= 0.15
  - avg_suspicion >= 0.50
  - disease_conc >= 0.50
action:
  gang_score: current + 0.15
  confidence_reason: "子图密度高、传播分高且诊疗集中，具备典型团伙结构特征"
severity: HIGH
```

### R-02：大规模高风险组件
```yaml
rule_id: R-02
name: large_high_risk_component
description: 规模较大且高风险节点密集的组件
conditions:
  - size >= 8
  - high_risk_ratio >= 0.35
action:
  gang_score: current + 0.10
  confidence_reason: f"组件规模{size}，高风险节点占比{high_risk_ratio:.0%}，建议排查成员关系"
severity: HIGH
```

### R-03：时间压缩规则
```yaml
rule_id: R-03
name: short_timespan_dense_cluster
description: 短时间内大量理赔高度集中（时间跨度<=30天但组件规模>=5）
conditions:
  - time_span_days <= 30
  - size >= 5
  - avg_suspicion >= 0.45
action:
  gang_score: current + 0.08
  confidence_reason: f"{time_span_days}天内聚集{size}个关联高风险实体，时间高度集中"
severity: MEDIUM
```

---

## Level 3：行为规则（模式加分）

### B-01：诊疗高集中+金额一致性
```yaml
rule_id: B-01
name: template_claim_pattern
description: 诊疗集中+理赔金额标准差极低，疑似按模板批量提交
conditions:
  - disease_conc >= 0.60
  - claim_amount_cv <= 0.25   # 变异系数 = std/mean
action:
  gang_score: current + 0.12
  confidence_reason: "诊疗编码集中度高且金额高度一致，疑似使用相同话术/模板批量作案"
severity: HIGH
```

### B-02：三角形模式
```yaml
rule_id: B-02
name: triangle_motif_detected
description: 发现三角形关联结构（三方相互关联），常见于中介型团伙欺诈
conditions:
  - motif_score >= 3
  - size >= 5
action:
  gang_score: current + 0.06
  confidence_reason: f"发现{motif_score}个三角形关联模式，疑似中介网络结构"
severity: MEDIUM
```

### B-03：代理人重复代理规则
```yaml
rule_id: B-03
name: agent_repeat_high_value
description: 同一代理人在30天内代理5+件高风险保单理赔，且每件金额均较高
conditions:
  - agent_claim_count_30d >= 5
  - agent_avg_claim_amount >= threshold_high_amount   # 业务自定义
action:
  gang_score: current + 0.08
  confidence_reason: f"代理人{agent_id}近30天代理{n}件高风险理赔，金额偏高"
severity: MEDIUM
```

---

## Level 4：辅助规则（注释信号，不单独触发）

### C-01：就诊医院集中
```yaml
rule_id: C-01
name: hospital_concentration
description: 组件内>60%的保单在同一家或同一区域医院就诊
conditions:
  - hospital_conc >= 0.60
note: "附加至 confidence_reasons，提供地理/机构集中线索，供人工排查"
```

### C-02：投保-理赔时间极短
```yaml
rule_id: C-02
name: short_insure_to_claim
description: 投保后30天内即申请理赔，且多件保单呈现相同模式
conditions:
  - insure_to_claim_days <= 30
  - count_short_insure_to_claim >= 3
note: "附加至 confidence_reasons，提示'刚投保就申请理赔'集中模式"
```

### C-03：同一设备/IP多保单
```yaml
rule_id: C-03
name: device_ip_collision
description: 同一设备指纹或IP地址关联3+个不同被保人
conditions:
  - device_or_ip_collision_count >= 3
note: "附加至 confidence_reasons，提示设备/网络层面共用异常"
```

---

## 规则执行顺序

```
1. 执行所有 Level 1 强规则（任意命中即覆盖 gang_score 并终止加分规则）
2. 若无 Level 1 命中，依次执行 Level 2、3 规则并累加分值（不超过1.0）
3. 始终执行 Level 4 辅助规则，将注释追加到 confidence_reasons
4. 最终 gang_score = min(base_gang_score + rule_boosts, 1.0)
```

---

## 阈值调优建议

| 参数 | 初始值 | 调优方法 |
|------|-------|---------|
| `disease_conc` 高危阈值 | 0.6 | 用已标注团伙样本做ROC分析确定最优cut-off |
| `burst_score` threshold | 4（24h内） | 按业务高峰期调整（节假日可适当放宽） |
| `density` 阈值 | 0.15 | 按险种调整（健康险比财险可适当降低） |
| `same_payee_count_in_7d` | 3 | 按业务规模和代理层级分别设阈 |
| 强规则覆盖下限 | 0.7 | 不建议低于0.65（避免过多误报进入人工队列） |

**离线校准流程**：
1. 取最近12个月已标注的团伙/非团伙样本
2. 用上述规则对历史数据打分，计算 precision@k 和 recall
3. 调整阈值使 precision@20 ≥ 0.8（优先减少误报），或 recall ≥ 0.75（优先发现团伙）
4. 每季度重新校准一次

---

## 规则版本管理

每次修改规则须记录：
- `rule_version`（语义版本号，如 v1.2.0）
- `modified_by`（修改人）
- `change_reason`（变更原因）
- `effective_date`（生效日期）
- `test_case_ids`（用于验证的测试用例列表）
