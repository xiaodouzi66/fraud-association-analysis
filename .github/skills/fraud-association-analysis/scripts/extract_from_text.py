#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_from_text.py
从案件文字描述中提取种子案件 JSON 结构。

策略：
  1. 正则/模式优先（带标签结构化文本，如 "住院天数: 34天"）
  2. LLM 兜底（自由文字描述，由 Claude 在 skill 层调用，本脚本仅负责正则部分）
  3. 缺失必填字段时，输出待查询字段列表（由 query_claim_db.py 补全）

用法：
  python extract_from_text.py --text "案件号: CL001\n疾病诊断: 膝挫伤\n住院天数: 34天\n总费用: 16371.93元"
  python extract_from_text.py --file input.txt
  python extract_from_text.py --text "..." --output seed_case.json
"""

import re
import json
import argparse
import sys
from typing import Optional


# ─── 正则规则表 ───────────────────────────────────────────────────────────────
# 每条规则：(目标字段名, 正则模式, 值处理函数)
_PATTERNS = [
    # 案件号
    ("案件号", re.compile(
        r"案件号[:\uff1a\s]+([A-Za-z0-9\-_\u6848\u4ef6]+)", re.IGNORECASE
    ), str),

    # 保单号
    ("保单号", re.compile(
        r"保单号[:\uff1a\s]+([A-Za-z0-9\-_\u4fdd\u5355]+)", re.IGNORECASE
    ), str),

    # 被保人姓名
    ("被保人姓名", re.compile(
        r"(?:被保人|被保险人|患者|客户)(?:姓名)?[:\uff1a\s]+([\u4e00-\u9fa5*]{2,10})"
    ), str),

    # 报案日期
    ("报案日期", re.compile(
        r"(?:报案日期|出险日期|发生日期)[:\uff1a\s]+(\d{4}[-/年]\d{1,2}[-/月]\d{1,2})"
    ), lambda s: s.replace("年", "-").replace("月", "-").replace("/", "-").rstrip("日")),

    # 就诊类型
    ("就诊类型", re.compile(
        r"(?:就诊类型|住院类型)[:\uff1a\s]*(住院|门诊|急诊)"
    ), str),

    # 住院天数
    ("住院天数", re.compile(
        r"住院(?:天数|时长|日数)[:\uff1a\s]*(\d+)\s*天?"
    ), int),

    # 疾病名称（支持多值：逗号/顿号/换行分隔）
    ("疾病名称", re.compile(
        r"(?:疾病(?:诊断|名称)|诊断(?:结果|病名)?)[:\uff1a\s]+([\u4e00-\u9fa5\w\s,\uff0c\u3001\(\)\uff08\uff09]+?)(?:\n|$|风险|排除|手术|住院天数)"
    ), lambda s: [d.strip() for d in re.split(r"[,\uff0c\u3001\n]+", s) if d.strip()]),

    # 医院名称
    ("医院名称", re.compile(
        r"(?:就诊医院|医院名称|医院)[:\uff1a\s]+([\u4e00-\u9fa5\w\s（）()]+?(?:医院|诊所|卫生院))"
    ), str),

    # 赔付金额（支持"总费用"/"赔付金额"/"赔款"等）
    ("赔付金额", re.compile(
        r"(?:总费用|赔付金额|赔款|理赔金额|赔付)[:\uff1a\s]*([\d,，.]+)\s*元?"
    ), lambda s: float(s.replace(",", "").replace("，", ""))),

    # 治疗手段（正则提取括号内或列表内容）
    ("治疗手段", re.compile(
        r"(?:治疗手段|治疗方式|手术项目)[:\uff1a\s]+([\u4e00-\u9fa5\w\s,\uff0c\u3001（）()、]+?)(?:\n|$|风险|费用)"
    ), lambda s: [t.strip() for t in re.split(r"[,\uff0c\u3001\u3001、\n]+", s) if t.strip()]),
]

# 若有"无手术"描述，则将"无手术治疗"添加到治疗手段
_NO_SURGERY_PATTERN = re.compile(r"(?:0元|无)[（(]?无手术(?:治疗)?[）)]?")

# ─── 核心函数 ─────────────────────────────────────────────────────────────────

def extract_by_regex(text: str) -> dict:
    """
    用正则从文本中提取种子案件字段。
    返回：提取到的字段 dict，未匹配字段不在 dict 中（由调用方判断缺失）
    """
    result = {}
    for field, pattern, converter in _PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(1).strip()
            try:
                result[field] = converter(raw)
            except Exception:
                result[field] = raw

    # 特殊处理：如果有"无手术"描述，补充治疗手段
    if _NO_SURGERY_PATTERN.search(text):
        hands = result.get("治疗手段", [])
        if isinstance(hands, list) and "无手术治疗" not in hands:
            hands.append("无手术治疗")
        result["治疗手段"] = hands

    # 特殊处理：如果只有"住院"关键词但没有就诊类型字段
    if "就诊类型" not in result:
        if re.search(r"住院\d+天|住院天数", text):
            result["就诊类型"] = "住院"

    return result


def build_seed_case(extracted: dict, case_id: Optional[str] = None) -> dict:
    """
    将提取的字段组装为标准种子案件 JSON。
    缺失的必填字段值为 None，并附加 _缺失字段 列表供后续 DB 补全。
    """
    REQUIRED = {"案件号", "疾病名称", "医院名称"}
    OPTIONAL_DEFAULTS = {
        "保单号": None,
        "被保人姓名": None,
        "报案日期": None,
        "就诊类型": "住院",
        "住院天数": None,
        "赔付金额": None,
        "治疗手段": [],
    }

    # 优先使用命令行传入的 case_id 覆盖
    if case_id:
        extracted["案件号"] = case_id

    mo = {}
    for k, default in {
        "就诊类型": "住院",
        "住院天数": None,
        "疾病名称": None,
        "医院名称": None,
        "赔付金额": None,
        "治疗手段": [],
    }.items():
        mo[k] = extracted.get(k, default)

    seed = {
        "案件号": extracted.get("案件号"),
        "保单号": extracted.get("保单号"),
        "被保人姓名": extracted.get("被保人姓名"),
        "报案日期": extracted.get("报案日期"),
        "MO特征": mo,
    }

    # 标注缺失字段（供后续 query_claim_db.py 补全）
    missing = [f for f in REQUIRED if not seed.get(f) and not mo.get(f)]
    if missing:
        seed["_缺失字段"] = missing

    return seed


def extract(text: str, case_id: Optional[str] = None) -> dict:
    """主入口：文本 → 种子案件 JSON dict"""
    extracted = extract_by_regex(text)
    return build_seed_case(extracted, case_id=case_id)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="从案件文字描述提取种子案件 JSON")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--text", type=str, help="直接传入案件描述文字")
    group.add_argument("--file", type=str, help="读取包含案件描述的文本文件")
    parser.add_argument("--case-id", type=str, default=None, help="强制指定案件号（覆盖解析结果）")
    parser.add_argument("--output", type=str, default=None, help="输出 JSON 文件路径（不填则打印到 stdout）")
    args = parser.parse_args()

    if args.text:
        text = args.text
    else:
        with open(args.file, "r", encoding="utf-8") as f:
            text = f.read()

    result = extract(text, case_id=args.case_id)

    missing = result.get("_缺失字段", [])
    if missing:
        print(f"⚠️  以下必填字段未能从文字中提取，需要 DB 补全: {missing}", file=sys.stderr)

    output_str = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_str)
        print(f"✅ 已保存到 {args.output}")
    else:
        print(output_str)


if __name__ == "__main__":
    main()
