---
name: case-seed-extractor
description: Extract structured seed-case JSON from a user's natural language description of a suspicious insurance claim. Trigger when (1) a user describes a suspicious case in text (structured labels like "住院天数: 34天" OR free-form narrative) and needs to produce the seed_case JSON required by fraud-association-analysis; (2) user asks to "extract case info", "convert to JSON", or "prepare seed case". Automatically fills missing fields by querying ODPS claim/policy tables. Do NOT trigger for bulk batch processing or when the user already has a complete seed_case JSON.
---
# 案件种子信息提取 Skill

从用户输入的案件文字描述，提取结构化种子案件 JSON，供 `fraud-association-analysis` Skill 使用。

## 一、输入类型与提取策略

| 输入类型 | 示例 | 策略 |
|---------|------|------|
| **带标签结构化文本** | `疾病诊断: 膝挫伤\n住院天数: 34天` | 正则直接提取，速度快、100% 准确 |
| **自由文字描述** | "这个客户因腰扭伤住了两周，医院在河南某县" | LLM 语义理解提取（本 skill 内由 Claude 处理） |
| **仅有案件号/保单号** | `案件号: CL0300003949497494` | DB 直接查询补全所有字段 |
| **混合** | 部分有标签，部分描述 | 正则优先，LLM 兜底缺失字段 |

## 二、提取流程（三步）

### 步骤 1：正则提取（脚本）

对带标签的结构化输入运行 `scripts/extract_from_text.py`：

```bash
python scripts/extract_from_text.py --text "案件号: CL001\n疾病诊断: 膝挫伤\n住院天数: 34天\n总费用: 16371.93元" \
  --output seed_partial.json
```

输出示例（含 `_缺失字段` 标记）：
```json
{
  "案件号": "CL001",
  "MO特征": {
    "就诊类型": "住院",
    "住院天数": 34,
    "疾病名称": ["膝挫伤"],
    "医院名称": null,
    "赔付金额": 16371.93,
    "治疗手段": ["无手术治疗"]
  },
  "_缺失字段": ["医院名称"]
}
```

### 步骤 2：LLM 补全（自由文字时）

若输入为自由文字，Claude 直接从文字中推断以下字段并输出 JSON：

```
目标字段：案件号 / 保单号 / 被保人姓名 / 报案日期 / 就诊类型 / 住院天数 /
          疾病名称（列表）/ 医院名称 / 赔付金额 / 治疗手段（列表）
规则：无法确定的字段填 null；不推测、不编造。
```

### 步骤 3：DB 补全（ODPS 查询）

若仍有 `_缺失字段`（且已有案件号/保单号），查询赔案表/保单表：

```bash
python scripts/query_claim_db.py --seed seed_partial.json \
  --output seed_full.json
```

或直接按案件号查询：
```bash
python scripts/query_claim_db.py --case-id CL0300003949497494 \
  --output seed_full.json
```

**DB 查询字段映射和表名配置**: 读取 [references/db_schema.md](references/db_schema.md)  
**输出 JSON 字段规范**: 读取 [references/seed_case_schema.md](references/seed_case_schema.md)

## 三、输出 → 传递给 fraud-association-analysis

提取完成后的 `seed_full.json` 即为 `fraud-association-analysis` 的 `种子案件` 字段，直接嵌入三层输入：

```json
{
  "种子案件": { ...seed_full.json 内容... },
  "保单节点列表": [...],
  "传播边列表":   [...],
  "关联案件列表": [...]
}
```

## 四、交互确认规则

- 若 `_缺失字段` 中包含 `疾病名称` 或 `医院名称`，且 DB 也未能补全 → **必须向用户询问**，不得用 null 继续传下游
- 若赔付金额缺失但其他字段完整 → 可用 null 继续（金额非 MO 评分核心字段）
- 若案件号/保单号均缺失 → 向用户确认身份信息后再查 DB

## 五、注意事项

- `query_claim_db.py` 需要 ODPS 网络访问权限；离线环境可跳过 DB 步骤，仅使用正则/LLM 提取
- 被保人姓名等敏感字段在输出前保持脱敏状态（中间字符用 * 替换）
- 表名和字段映射按实际情况修改 `query_claim_db.py` 中的 `CLAIM_TABLE` / `CLAIM_FIELD_MAP`
