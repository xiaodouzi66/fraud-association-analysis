#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
query_claim_db.py
通过 ODPS(MaxCompute) 查询保单表/赔案表，补全种子案件缺失字段。

依赖：
  - odps_client.py（从工作区复制或 PYTHONPATH 指向）
  - pyodps

用法（独立运行）：
  python query_claim_db.py --case-id CL0300003949497494
  python query_claim_db.py --policy-id P0012345678
  python query_claim_db.py --seed seed_case_partial.json --output seed_case_full.json

与 extract_from_text.py 配合使用（自动补全缺失字段）：
  python query_claim_db.py --seed seed_partial.json --fill-missing --output seed_full.json
"""

import json
import argparse
import sys
import os
from typing import Optional, Dict, Any

# ─── 导入 ODPSClient ──────────────────────────────────────────────────────────
# 支持从环境变量 ODPS_CLIENT_PATH 指定路径
_odps_client_path = os.environ.get(
    "ODPS_CLIENT_PATH",
    "/Users/jiangxueyan/Library/Mobile Documents/com~apple~CloudDocs/work-zhongan"
)
if _odps_client_path not in sys.path:
    sys.path.insert(0, _odps_client_path)

try:
    from odps_client import ODPSClient
    _ODPS_AVAILABLE = True
except ImportError:
    _ODPS_AVAILABLE = False


# ─── 表名配置（按实际表名修改）────────────────────────────────────────────────
# 可通过环境变量覆盖，例如：
#   export CLAIM_TABLE=zhongan_dev.dw_claim_detail
#   export POLICY_TABLE=zhongan_dev.dw_policy_detail
CLAIM_TABLE  = os.environ.get("CLAIM_TABLE",  "zhongan_dev.dw_claim_detail")
POLICY_TABLE = os.environ.get("POLICY_TABLE", "zhongan_dev.dw_policy_detail")

# ─── 字段映射：DB 列名 → 种子案件 JSON 字段名 ─────────────────────────────────
# 修改左侧 key 以匹配实际 DB 列名，右侧 value 为种子 JSON 字段
CLAIM_FIELD_MAP = {
    "claim_no":        "案件号",
    "policy_no":       "保单号",
    "insured_name":    "被保人姓名",
    "report_date":     "报案日期",
    "visit_type":      "就诊类型",
    "hosp_days":       "住院天数",
    "diagnosis":       "疾病名称",
    "hospital_name":   "医院名称",
    "claim_amount":    "赔付金额",
    "treatment":       "治疗手段",
}

POLICY_FIELD_MAP = {
    "policy_no":       "保单号",
    "insured_name":    "被保人姓名",
    "insured_id":      "被保人ID",
}


# ─── 查询函数 ─────────────────────────────────────────────────────────────────

def _get_client() -> "ODPSClient":
    if not _ODPS_AVAILABLE:
        raise ImportError(
            "无法导入 ODPSClient，请确认 odps_client.py 路径\n"
            f"当前 ODPS_CLIENT_PATH={_odps_client_path}"
        )
    return ODPSClient()


def query_by_case_id(case_id: str) -> Optional[Dict[str, Any]]:
    """通过案件号查询赔案表，返回种子案件字段 dict（缺失字段为 None）"""
    client = _get_client()
    sql = f"""
        SELECT {', '.join(CLAIM_FIELD_MAP.keys())}
        FROM {CLAIM_TABLE}
        WHERE claim_no = '{case_id}'
        LIMIT 1
    """
    df = client.read_df_from_odps(sql=sql)
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    return _map_row(row, CLAIM_FIELD_MAP)


def query_by_policy_id(policy_id: str) -> Optional[Dict[str, Any]]:
    """通过保单号查询最新一条赔案记录"""
    client = _get_client()
    sql = f"""
        SELECT {', '.join(CLAIM_FIELD_MAP.keys())}
        FROM {CLAIM_TABLE}
        WHERE policy_no = '{policy_id}'
        ORDER BY report_date DESC
        LIMIT 1
    """
    df = client.read_df_from_odps(sql=sql)
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    return _map_row(row, CLAIM_FIELD_MAP)


def query_by_name_and_date(name: str, date_from: str, date_to: str) -> Optional[Dict[str, Any]]:
    """通过被保人姓名 + 日期范围模糊匹配（兜底策略）"""
    client = _get_client()
    sql = f"""
        SELECT {', '.join(CLAIM_FIELD_MAP.keys())}
        FROM {CLAIM_TABLE}
        WHERE insured_name LIKE '%{name}%'
          AND report_date BETWEEN '{date_from}' AND '{date_to}'
        ORDER BY report_date DESC
        LIMIT 5
    """
    df = client.read_df_from_odps(sql=sql)
    if df.empty:
        return None
    # 返回最新的一条
    row = df.iloc[0].to_dict()
    return _map_row(row, CLAIM_FIELD_MAP)


def _map_row(row: dict, field_map: dict) -> dict:
    """DB 行 dict → 种子案件字段 dict（处理列表型字段）"""
    result = {}
    for db_col, seed_field in field_map.items():
        val = row.get(db_col)
        if val is None or str(val).strip() in ("", "None", "nan"):
            continue
        # 对列表型字段（疾病名称、治疗手段）做字符串分割
        if seed_field in ("疾病名称", "治疗手段") and isinstance(val, str):
            val = [v.strip() for v in val.replace("；", ";").split(";") if v.strip()]
        result[seed_field] = val
    return result


# ─── 补全逻辑 ─────────────────────────────────────────────────────────────────

def fill_missing(seed: dict) -> dict:
    """
    根据种子案件中已有字段查询 DB，补全 _缺失字段 中标注的字段。
    返回更新后的 seed dict。
    """
    missing = seed.get("_缺失字段", [])
    if not missing:
        return seed

    db_result = None

    # 优先级：案件号 > 保单号 > 姓名模糊
    if seed.get("案件号"):
        db_result = query_by_case_id(seed["案件号"])
    elif seed.get("保单号"):
        db_result = query_by_policy_id(seed["保单号"])
    elif seed.get("被保人姓名") and seed.get("报案日期"):
        d = seed["报案日期"]
        db_result = query_by_name_and_date(
            seed["被保人姓名"], d, d
        )

    if not db_result:
        print("⚠️  DB 查询未返回结果，缺失字段无法补全", file=sys.stderr)
        return seed

    # 仅补全缺失字段，不覆盖已有值
    mo = seed.setdefault("MO特征", {})
    mo_fields = {"就诊类型", "住院天数", "疾病名称", "医院名称", "赔付金额", "治疗手段"}

    for field, value in db_result.items():
        if field in mo_fields:
            if not mo.get(field):
                mo[field] = value
        else:
            if not seed.get(field):
                seed[field] = value

    # 更新缺失字段列表
    still_missing = [
        f for f in missing
        if not seed.get(f) and not mo.get(f)
    ]
    if still_missing:
        seed["_缺失字段"] = still_missing
        print(f"⚠️  DB 补全后仍缺失: {still_missing}", file=sys.stderr)
    else:
        seed.pop("_缺失字段", None)
        print("✅ 所有缺失字段已通过 DB 补全")

    return seed


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="查询 ODPS，补全种子案件缺失字段")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--case-id",   type=str, help="按案件号查询")
    group.add_argument("--policy-id", type=str, help="按保单号查询")
    group.add_argument("--seed",      type=str, help="读取 extract_from_text.py 输出的 JSON，自动补全 _缺失字段")
    parser.add_argument("--output",   type=str, default=None, help="输出 JSON 文件路径（默认 stdout）")
    args = parser.parse_args()

    if args.seed:
        with open(args.seed, "r", encoding="utf-8") as f:
            seed = json.load(f)
        result = fill_missing(seed)
    elif args.case_id:
        raw = query_by_case_id(args.case_id)
        if not raw:
            print(f"❌ 未找到案件号 {args.case_id}", file=sys.stderr)
            sys.exit(1)
        from extract_from_text import build_seed_case
        result = build_seed_case(raw, case_id=args.case_id)
    elif args.policy_id:
        raw = query_by_policy_id(args.policy_id)
        if not raw:
            print(f"❌ 未找到保单号 {args.policy_id}", file=sys.stderr)
            sys.exit(1)
        from extract_from_text import build_seed_case
        result = build_seed_case(raw)
    else:
        parser.print_help()
        sys.exit(1)

    output_str = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_str)
        print(f"✅ 已保存到 {args.output}")
    else:
        print(output_str)


if __name__ == "__main__":
    main()
