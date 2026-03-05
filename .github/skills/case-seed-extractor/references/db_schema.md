# 数据库表结构参考

本文件记录 `query_claim_db.py` 依赖的 ODPS 表结构。  
**按实际表名和列名修改 `query_claim_db.py` 中的 `CLAIM_TABLE`、`POLICY_TABLE` 和字段映射。**

## 1. 赔案明细表（CLAIM_TABLE）

默认表名：`zhongan_dev.dw_claim_detail`  
可通过环境变量覆盖：`export CLAIM_TABLE=your_project.your_table`

| DB 列名（示例） | 对应种子字段 | 类型 | 说明 |
|---------------|------------|------|------|
| `claim_no` | 案件号 | string | 主键 |
| `policy_no` | 保单号 | string | 外键 |
| `insured_name` | 被保人姓名 | string | 脱敏 |
| `report_date` | 报案日期 | string | YYYY-MM-DD |
| `visit_type` | 就诊类型 | string | 住院/门诊/急诊 |
| `hosp_days` | 住院天数 | bigint | |
| `diagnosis` | 疾病名称 | string | 多个以分号分隔 |
| `hospital_name` | 医院名称 | string | |
| `claim_amount` | 赔付金额 | double | 元 |
| `treatment` | 治疗手段 | string | 多个以分号分隔 |
| `blacklist_flag` | 是否黑名单 | bigint | 0/1 |

### 常用查询 SQL 模板

```sql
-- 按案件号查询
SELECT claim_no, policy_no, insured_name, report_date,
       visit_type, hosp_days, diagnosis, hospital_name,
       claim_amount, treatment
FROM zhongan_dev.dw_claim_detail
WHERE claim_no = '${案件号}'
LIMIT 1;

-- 按保单号查询最新案件
SELECT claim_no, policy_no, insured_name, report_date,
       visit_type, hosp_days, diagnosis, hospital_name,
       claim_amount, treatment
FROM zhongan_dev.dw_claim_detail
WHERE policy_no = '${保单号}'
ORDER BY report_date DESC
LIMIT 1;

-- 按姓名+日期范围模糊查找
SELECT claim_no, policy_no, insured_name, report_date,
       visit_type, hosp_days, diagnosis, hospital_name,
       claim_amount, treatment
FROM zhongan_dev.dw_claim_detail
WHERE insured_name LIKE '%${姓名}%'
  AND report_date BETWEEN '${开始日期}' AND '${结束日期}'
ORDER BY report_date DESC
LIMIT 5;
```

## 2. 保单信息表（POLICY_TABLE）

默认表名：`zhongan_dev.dw_policy_detail`  
可通过环境变量覆盖：`export POLICY_TABLE=your_project.your_table`

| DB 列名（示例） | 对应种子字段 | 类型 | 说明 |
|---------------|------------|------|------|
| `policy_no` | 保单号 | string | 主键 |
| `insured_name` | 被保人姓名 | string | |
| `insured_id` | 被保人ID | string | 内部人员 ID |
| `agent_id` | 代理人ID | string | |
| `issue_date` | 投保日期 | string | YYYY-MM-DD |

### 常用查询 SQL 模板

```sql
-- 按保单号查询基本信息
SELECT policy_no, insured_name, insured_id, agent_id, issue_date
FROM zhongan_dev.dw_policy_detail
WHERE policy_no = '${保单号}'
LIMIT 1;
```

## 3. 修改字段映射

如果实际列名与上表不同，在 `query_claim_db.py` 中修改对应的映射字典：

```python
# query_claim_db.py
CLAIM_FIELD_MAP = {
    "你的实际列名":  "种子JSON字段名",
    "actual_col":   "案件号",
    ...
}
```

## 4. 连接配置

`ODPSClient` 从 `odps_client.py` 中的 `DEFAULT_TOKENS` 读取默认连接信息，或通过初始化参数覆盖：

```python
client = ODPSClient(tokens={
    "access_id": "...",
    "secret_access_key": "...",
    "project": "zhongan_dev",
    "endpoint": "http://service.cn.maxcompute.aliyun-inc.com/api",
})
```
