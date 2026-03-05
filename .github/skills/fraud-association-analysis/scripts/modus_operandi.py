#!/usr/bin/env python3
"""
作案手法（MO, Modus Operandi）相似度分析模块。

能力：
1) 从传播记录提取 MO 特征
2) 支持疾病软匹配（如：摔伤/扭伤/颈椎不适 → 主观_肌肉骨骼轻伤）
3) 支持医院名称软匹配
4) 计算种子案件与传播节点的 mo_score（0~1）
5) 支持命令行批量输出评分结果
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set


DEFAULT_LOS_THRESHOLD_DAYS = 5


DISEASE_GROUP_KEYWORDS: Dict[str, List[str]] = {
    "主观_肌肉骨骼轻伤": [
        "扭伤", "摔伤", "跌伤", "挫伤", "劳损", "软组织损伤",
        "颈椎不适", "颈椎病", "腰肌劳损", "腰痛", "肩周炎", "肌肉拉伤",
    ],
    "主观_神经系统": ["头晕", "眩晕", "头痛", "偏头痛", "失眠", "神经衰弱"],
    "主观_消化系统": ["腹痛", "胃痛", "胃炎", "肠炎", "消化不良"],
    "主观_呼吸系统": ["咳嗽", "咽炎", "上呼吸道感染", "感冒", "支气管炎"],
    "客观_骨折": ["骨折", "骨裂", "骨折术后"],
    "客观_心脑血管": ["心梗", "脑梗", "冠心病", "脑出血", "心绞痛"],
    "客观_肿瘤": ["肿瘤", "恶性肿瘤", "癌"],
}


ICD_PREFIX_GROUPS: Dict[str, List[str]] = {
    "主观_肌肉骨骼轻伤": ["M79", "S93", "S80", "M54", "M47"],
    "主观_神经系统": ["G43", "G44", "F51", "R42"],
    "主观_消化系统": ["K29", "K58", "R10"],
    "主观_呼吸系统": ["J06", "J20", "R05"],
    "客观_骨折": ["S02", "S12", "S22", "S32", "S42", "S52", "S62", "S72", "S82"],
    "客观_心脑血管": ["I20", "I21", "I61", "I63"],
    "客观_肿瘤": ["C", "D0"],
}


TREATMENT_GROUP_KEYWORDS: Dict[str, List[str]] = {
    "治疗_保守治疗": ["理疗", "对症治疗", "观察", "卧床", "康复"],
    "治疗_手术介入": ["手术", "介入", "内固定", "缝合"],
    "治疗_影像检查": ["ct", "mri", "核磁", "x线", "彩超"],
    "治疗_药物治疗": ["降压", "降糖", "抗炎", "抗生素", "输液"],
}


LOSS_TYPE_ALIASES: Dict[str, str] = {
    "住院": "住院",
    "门诊": "门诊",
    "急诊": "急诊",
}


@dataclass
class MOFeature:
    疾病分组集合: Set[str]
    住院类型: str
    住院天数超阈值: bool
    涉及医院集合: Set[str]
    金额区间标签: str
    治疗手段分组集合: Set[str]


def _ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def normalize_text(text: Any) -> str:
    if text is None:
        return ""
    s = str(text).strip().lower()
    s = re.sub(r"\s+", "", s)
    return s


def normalize_hospital_name(name: Any) -> str:
    s = normalize_text(name)
    if not s:
        return ""
    # 去常见后缀，提升“同院不同写法”匹配效果
    s = s.replace("有限公司", "")
    for suffix in ["附属医院", "人民医院", "中医院", "医院", "医疗中心", "卫生院", "门诊部"]:
        s = s.replace(suffix, "")
    return s


def map_diag_to_group(code_or_name: Any) -> Optional[str]:
    s_raw = str(code_or_name or "").strip()
    if not s_raw:
        return None
    s_norm = normalize_text(s_raw)

    # 1) ICD 前缀匹配（保留原始大小写用于前缀判断）
    upper = s_raw.upper()
    for group, prefixes in ICD_PREFIX_GROUPS.items():
        for prefix in prefixes:
            if upper.startswith(prefix.upper()):
                return group

    # 2) 中文关键词软匹配
    for group, keywords in DISEASE_GROUP_KEYWORDS.items():
        for kw in keywords:
            if normalize_text(kw) in s_norm:
                return group
    return None


def map_treatment_to_group(code_or_name: Any) -> Optional[str]:
    s = normalize_text(code_or_name)
    if not s:
        return None
    for group, keywords in TREATMENT_GROUP_KEYWORDS.items():
        for kw in keywords:
            if normalize_text(kw) in s:
                return group
    return None


def map_amount_to_label(amount: Any) -> str:
    try:
        x = float(amount)
    except Exception:
        return "未知"
    if x < 5000:
        return "低"
    if x <= 30000:
        return "中"
    return "高"


def normalize_loss_type(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return "未知"
    for k, v in LOSS_TYPE_ALIASES.items():
        if k in s:
            return v
    return s


def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union > 0 else 0.0


def extract_mo_feature(record: Dict[str, Any], los_threshold_days: int = DEFAULT_LOS_THRESHOLD_DAYS) -> MOFeature:
    diag_codes = _ensure_list(record.get("diag_codes") or record.get("疾病编码") or record.get("疾病名称"))
    treatment_codes = _ensure_list(record.get("treatment_codes") or record.get("治疗手段") or record.get("治疗编码"))

    disease_groups: Set[str] = set()
    for item in diag_codes:
        grp = map_diag_to_group(item)
        if grp:
            disease_groups.add(grp)

    treatment_groups: Set[str] = set()
    for item in treatment_codes:
        grp = map_treatment_to_group(item)
        if grp:
            treatment_groups.add(grp)

    los_days = record.get("los_days", record.get("住院天数"))
    try:
        los_days_val = int(los_days) if los_days is not None else 0
    except Exception:
        los_days_val = 0

    hospital_names = _ensure_list(
        record.get("hospital_names")
        or record.get("hospital_name")
        or record.get("医院名称")
        or record.get("医院")
    )
    normalized_hospitals = {normalize_hospital_name(h) for h in hospital_names if normalize_hospital_name(h)}

    loss_type = normalize_loss_type(record.get("loss_type") or record.get("就诊类型"))
    amount_label = map_amount_to_label(record.get("claim_amount") or record.get("赔付金额") or record.get("金额"))

    return MOFeature(
        疾病分组集合=disease_groups,
        住院类型=loss_type,
        住院天数超阈值=los_days_val >= los_threshold_days,
        涉及医院集合=normalized_hospitals,
        金额区间标签=amount_label,
        治疗手段分组集合=treatment_groups,
    )


def load_seed_mo(seed_case_mo: Dict[str, Any], los_threshold_days: int = DEFAULT_LOS_THRESHOLD_DAYS) -> MOFeature:
    return extract_mo_feature(seed_case_mo or {}, los_threshold_days=los_threshold_days)


def compute_mo_score(seed_mo: MOFeature, node_mo: MOFeature) -> float:
    disease_sim = jaccard(seed_mo.疾病分组集合, node_mo.疾病分组集合)
    hosp_sim = jaccard(seed_mo.涉及医院集合, node_mo.涉及医院集合)
    treatment_sim = jaccard(seed_mo.治疗手段分组集合, node_mo.治疗手段分组集合)

    loss_type_sim = 1.0 if seed_mo.住院类型 != "未知" and seed_mo.住院类型 == node_mo.住院类型 else 0.0
    los_flag_sim = 1.0 if seed_mo.住院天数超阈值 == node_mo.住院天数超阈值 else 0.0
    amount_sim = 1.0 if seed_mo.金额区间标签 != "未知" and seed_mo.金额区间标签 == node_mo.金额区间标签 else 0.0

    score = (
        0.40 * disease_sim
        + 0.15 * loss_type_sim
        + 0.20 * los_flag_sim
        + 0.10 * hosp_sim
        + 0.10 * amount_sim
        + 0.05 * treatment_sim
    )
    return max(0.0, min(1.0, score))


def batch_score(seed_mo: MOFeature, records: Iterable[Dict[str, Any]], los_threshold_days: int = DEFAULT_LOS_THRESHOLD_DAYS) -> Dict[str, float]:
    """对关联案件列表批量计算 mo_score，以案件号为 key 返回评分字典。

    ID 字段优先级：案件号 > 保单号 > entity_id > 实体ID，
    兼容新版三层输入格式与旧版传播记录格式。
    """
    result: Dict[str, float] = {}
    for r in records:
        record_id = str(
            r.get("案件号") or r.get("保单号")
            or r.get("entity_id") or r.get("实体ID") or ""
        ).strip()
        if not record_id:
            continue
        node_mo = extract_mo_feature(r, los_threshold_days=los_threshold_days)
        result[record_id] = round(compute_mo_score(seed_mo, node_mo), 6)
    return result


def _feature_to_json(mo: MOFeature) -> Dict[str, Any]:
    """将 MOFeature 序列化为可 JSON 输出的字典，手动构建避免 asdict 对 Set 的隐患。"""
    return {
        "疾病分组集合": sorted(mo.疾病分组集合),
        "住院类型": mo.住院类型,
        "住院天数超阈值": mo.住院天数超阈值,
        "涉及医院集合": sorted(mo.涉及医院集合),
        "金额区间标签": mo.金额区间标签,
        "治疗手段分组集合": sorted(mo.治疗手段分组集合),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MO 软匹配评分")
    p.add_argument("input", help="传播结果 JSON 文件（数组或含 propagation_results 字段）")
    p.add_argument("--seed-mo", required=True, help="种子 MO JSON 文件")
    p.add_argument("--output", default="mo_scores.json", help="输出文件路径")
    p.add_argument("--los-threshold-days", type=int, default=DEFAULT_LOS_THRESHOLD_DAYS)
    return p.parse_args()


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _extract_seed_mo_raw(data: Any) -> Dict[str, Any]:
    """从各种格式的输入中提取种子 MO 特征原始字典。

    支持以下路径（优先级从高到低）：
    1. 新格式：data["种子案件"]["MO特征"]
    2. 旧格式：data["seed_case_mo"]
    3. data 本身即为 MO 特征字典
    """
    if not isinstance(data, dict):
        return {}
    if "种子案件" in data:
        seed_case = data["种子案件"]
        return seed_case.get("MO特征") or seed_case or {}
    if "seed_case_mo" in data:
        return data["seed_case_mo"] or {}
    # 兜底：data 本身就是 MO 特征
    return data


def main() -> None:
    args = parse_args()

    input_data = _load_json(args.input)
    # 新格式：关联案件列表；旧格式：propagation_results；兜底：直接是数组
    if isinstance(input_data, list):
        records = input_data
    else:
        records = (
            input_data.get("关联案件列表")
            or input_data.get("propagation_results")
            or []
        )

    seed_data = _load_json(args.seed_mo)
    seed_mo_raw = _extract_seed_mo_raw(seed_data)
    seed_mo = load_seed_mo(seed_mo_raw, los_threshold_days=args.los_threshold_days)

    mo_scores = batch_score(seed_mo, records, los_threshold_days=args.los_threshold_days)

    out = {
        "种子MO特征": _feature_to_json(seed_mo),
        "节点MO评分": mo_scores,
        "参数": {
            "住院天数阈值": args.los_threshold_days,
            "疾病分组数量": len(DISEASE_GROUP_KEYWORDS),
        },
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("Wrote", args.output)


if __name__ == "__main__":
    main()
