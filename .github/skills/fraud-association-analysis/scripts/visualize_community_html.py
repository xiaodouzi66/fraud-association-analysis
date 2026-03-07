#!/usr/bin/env python3
"""
HTML 可视化模块（与 Notebook 风格对齐）：
1) 有 Leiden 分群结果：输出宏观社群图（pyecharts）+ Top2 风险子社群图（pyvis）
2) 无 Leiden 分群结果：输出全量保单↔ID 异构图（pyvis）

可视化均支持节点搜索：
- pyecharts: toolbox + tooltip
- pyvis: select_menu + filter_menu + show_buttons(filter_=['nodes'])
"""

from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter, defaultdict
from html import escape
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx
from pyvis.network import Network
from pyecharts import options as opts
from pyecharts.charts import Graph


ID_NODE_PREFIX = "__id__"

NODE_COLOR_MAP = {
    "policy": "#4e79a7",
    "共用手机号": "#f28e2b",
    "共用身份证号": "#e15759",
    "共用邮箱": "#76b7b2",
    "共用代理人ID": "#59a14f",
    "共用银行卡号": "#edc948",
    "unknown": "#9c9c9c",
}

MACRO_RED_SCALE = [
    "#ffe5e5",
    "#ffcccc",
    "#ff9999",
    "#ff6666",
    "#cc0000",
]

# ── 字段映射：规范名 → 所有别名列表（中文优先） ──────────────────────────────
FIELD_ALIASES: Dict[str, List[str]] = {
    "疾病名称":   ["疾病名称", "疾病编码", "diag_codes"],
    "就诊类型":   ["就诊类型", "loss_type"],
    "住院天数":   ["住院天数", "los_days"],
    "医院名称":   ["医院名称", "医院", "hospital_names", "hospital_name"],
    "赔付金额":   ["赔付金额", "金额", "claim_amount"],
    "治疗手段":   ["治疗手段", "治疗编码", "treatment_codes"],
    "是否黑名单": ["是否黑名单", "blacklist"],
    "案件号":     ["案件号", "case_no"],
    "保单号":     ["保单号", "policy_no"],
    "报案日期":   ["报案日期", "report_date"],
}

# Profile 统计行配置：(profile_key, 展示标签, 对应规范字段（用于高亮）, fmt)
# fmt: ".3f"/".2f"/".1f" 数值格式；"i" 整数；"list" 频次列表
PROFILE_STAT_CONFIG: List[Tuple[str, str, Optional[str], str]] = [
    ("avg_mo",          "平均MO相似分",   None,          ".3f"),
    ("avg_los",         "平均住院天数",   "住院天数",     ".1f"),
    ("avg_amount",      "平均赔付金额",   "赔付金额",     ".2f"),
    ("blacklist_count", "黑名单案件数",   "是否黑名单",   "i"),
    ("top_hospitals",   "高频医院",       "医院名称",     "list"),
    ("top_diseases",    "高频疾病",       "疾病名称",     "list"),
    ("visit_types",     "就诊类型分布",   "就诊类型",     "list"),
]


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _score_to_color(score: float, min_score: float, max_score: float) -> str:
    if max_score <= min_score:
        ratio = 1.0
    else:
        ratio = (score - min_score) / (max_score - min_score)
    ratio = max(0.0, min(1.0, ratio))
    low = (219, 233, 246)
    high = (204, 0, 0)
    r = int(low[0] + (high[0] - low[0]) * ratio)
    g = int(low[1] + (high[1] - low[1]) * ratio)
    b = int(low[2] + (high[2] - low[2]) * ratio)
    return f"#{r:02x}{g:02x}{b:02x}"


def _extract_comm_mo_map(leiden_results: List[Dict[str, Any]]) -> Dict[str, float]:
    mo_map: Dict[str, float] = {}
    for item in leiden_results:
        cid = str(item.get("社群编号") or "").strip()
        if not cid:
            continue
        ext = item.get("扩展指标") or {}
        mo_map[cid] = _safe_float(ext.get("mo_similarity_avg", 0.0), 0.0)
    return mo_map


def resolve_field(record: Dict[str, Any], canonical: str) -> Any:
    """按 FIELD_ALIASES 别名顺序查找字段，返回首个非 None 值；全部缺失则返回 None。"""
    for alias in FIELD_ALIASES.get(canonical, [canonical]):
        if alias in record:
            return record[alias]
    return None


def detect_available_fields(cases: List[Dict[str, Any]]) -> Set[str]:
    """扫描案件列表，返回至少存在一个非空值的规范字段集合（schema-adaptive）。"""
    available: Set[str] = set()
    for case in cases:
        for canonical in FIELD_ALIASES:
            if canonical in available:
                continue
            val = resolve_field(case, canonical)
            if val is None or val == "" or val == [] or val == 0 or val is False:
                continue
            available.add(canonical)
    return available


def extract_fraud_hypothesis(
    raw: Dict[str, Any],
    cluster: Dict[str, Any],
) -> Dict[str, Any]:
    """
    提取欺诈假设字段集合，用于可视化高亮。

    优先级：
      1. cluster 输出中有 'fraud_hypothesis' 字段时直接使用。
      2. 否则从 raw['种子案件'] 自动推导关键信号字段。

    返回:
      {
        "highlighted_fields": List[str],  # 规范字段名
        "description":        str,         # 假设描述
        "source":             str,         # 'explicit' | 'auto'
      }
    """
    # ① cluster 显式提供
    hyp = cluster.get("fraud_hypothesis")
    if hyp and isinstance(hyp, dict):
        fields = hyp.get("highlighted_fields") or hyp.get("high_risk_fields") or []
        return {
            "highlighted_fields": [str(f) for f in fields],
            "description": str(hyp.get("description") or ""),
            "source": "explicit",
        }

    # ② 从种子案件自动推导
    seed_raw = raw.get("种子案件") or {}
    # 优先从 MO特征 子字典读取（部分数据格式会嵌套在此）
    seed = seed_raw.get("MO特征") or seed_raw

    highlighted: List[str] = []
    desc_parts: List[str] = []

    los = _safe_int(resolve_field(seed, "住院天数"), 0)
    if los >= 5:
        highlighted.append("住院天数")
        desc_parts.append(f"住院天数={los}天（≥5天阈值）")

    diseases = resolve_field(seed, "疾病名称") or []
    if isinstance(diseases, str):
        diseases = [diseases]
    diseases = [str(d).strip() for d in diseases if str(d).strip()]
    if diseases:
        highlighted.append("疾病名称")
        desc_parts.append("疾病: " + "、".join(diseases[:3]))

    hospital = str(resolve_field(seed, "医院名称") or "").strip()
    if hospital:
        highlighted.append("医院名称")

    visit = str(resolve_field(seed, "就诊类型") or "").strip()
    if visit:
        highlighted.append("就诊类型")
        desc_parts.append(f"就诊类型: {visit}")

    return {
        "highlighted_fields": highlighted,
        "description": ("种子案件特征: " + "；".join(desc_parts)) if desc_parts else "",
        "source": "auto",
    }


def build_policy_case_index(
    cases: List[Dict[str, Any]],
    mo_scores: Dict[str, float],
) -> Dict[str, List[Dict[str, Any]]]:
    """将关联案件列表以保单号为 key 索引，字段访问通过 resolve_field 保持 schema-adaptive。"""
    index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for case in cases:
        policy_no = str(resolve_field(case, "保单号") or "").strip()
        if not policy_no:
            continue
        case_no = str(resolve_field(case, "案件号") or "").strip()
        mo_score = _safe_float(mo_scores.get(case_no, mo_scores.get(policy_no, 0.0)), 0.0)

        diseases = resolve_field(case, "疾病名称") or []
        if isinstance(diseases, str):
            diseases = [diseases]
        treatments = resolve_field(case, "治疗手段") or []
        if isinstance(treatments, str):
            treatments = [treatments]

        index[policy_no].append(
            {
                "案件号": case_no,
                "保单号": policy_no,
                "报案日期": str(resolve_field(case, "报案日期") or ""),
                "就诊类型": str(resolve_field(case, "就诊类型") or ""),
                "住院天数": _safe_int(resolve_field(case, "住院天数"), 0),
                "赔付金额": _safe_float(resolve_field(case, "赔付金额") or 0.0, 0.0),
                "疾病名称": [str(x) for x in diseases if str(x).strip()],
                "医院名称": str(resolve_field(case, "医院名称") or ""),
                "治疗手段": [str(x) for x in treatments if str(x).strip()],
                "是否黑名单": bool(resolve_field(case, "是否黑名单") or False),
                "mo_score": mo_score,
            }
        )

    for policy_no in index:
        index[policy_no].sort(key=lambda x: x.get("mo_score", 0.0), reverse=True)
    return index


def build_community_profile(
    policy_set: Set[str],
    policy_case_index: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    all_cases: List[Dict[str, Any]] = []
    for pid in policy_set:
        all_cases.extend(policy_case_index.get(pid, []))

    if not all_cases:
        return {
            "policy_count": len(policy_set),
            "case_count": 0,
            "avg_mo": 0.0,
            "blacklist_count": 0,
            "avg_los": 0.0,
            "avg_amount": 0.0,
            "top_hospitals": [],
            "top_diseases": [],
            "visit_types": [],
        }

    mo_vals = [_safe_float(c.get("mo_score", 0.0), 0.0) for c in all_cases]
    los_vals = [_safe_int(c.get("住院天数"), 0) for c in all_cases if _safe_int(c.get("住院天数"), 0) > 0]
    amount_vals = [_safe_float(c.get("赔付金额", 0.0), 0.0) for c in all_cases if _safe_float(c.get("赔付金额", 0.0), 0.0) > 0]

    disease_counter: Counter[str] = Counter()
    hospital_counter: Counter[str] = Counter()
    visit_counter: Counter[str] = Counter()
    blacklist_count = 0
    for c in all_cases:
        for d in c.get("疾病名称") or []:
            if d:
                disease_counter[str(d)] += 1
        hosp = str(c.get("医院名称") or "").strip()
        if hosp:
            hospital_counter[hosp] += 1
        vtype = str(c.get("就诊类型") or "").strip()
        if vtype:
            visit_counter[vtype] += 1
        if bool(c.get("是否黑名单", False)):
            blacklist_count += 1

    return {
        "policy_count": len(policy_set),
        "case_count": len(all_cases),
        "avg_mo": sum(mo_vals) / len(mo_vals) if mo_vals else 0.0,
        "blacklist_count": blacklist_count,
        "avg_los": sum(los_vals) / len(los_vals) if los_vals else 0.0,
        "avg_amount": sum(amount_vals) / len(amount_vals) if amount_vals else 0.0,
        "top_hospitals": hospital_counter.most_common(3),
        "top_diseases": disease_counter.most_common(3),
        "visit_types": visit_counter.most_common(3),
    }


def _inject_before_body_end(output_path: str, html_chunk: str) -> None:
    with open(output_path, "r", encoding="utf-8") as f:
        content = f.read()

    if "</body>" in content:
        content = content.replace("</body>", html_chunk + "\n</body>", 1)
    else:
        content += "\n" + html_chunk

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)


def _mo_border_color(mo_score: float) -> str:
    """根据 MO 相似分返回赔案卡片左侧色条颜色。"""
    if mo_score >= 0.6:
        return "#cc0000"
    if mo_score >= 0.4:
        return "#f28e2b"
    return "#4e79a7"


def _build_case_tooltip_html(
    node_id: str,
    attrs: Dict[str, Any],
    degree: int,
    cases: List[Dict[str, Any]],
    highlighted_fields: Optional[Set[str]] = None,
) -> str:
    """构建保单节点 tooltip HTML（卡片式布局）；highlighted_fields 中的字段以黄色背景高亮。"""
    hl: Set[str] = highlighted_fields or set()

    TD_LABEL = "padding:2px 6px 2px 0; color:#666; white-space:nowrap; vertical-align:top; font-size:11px;"
    TD_VALUE = "padding:2px 0; vertical-align:top; font-size:12px;"

    def _td_style(canonical: str) -> str:
        """高亮字段的值单元格加黄底。"""
        base_style = TD_VALUE
        if canonical in hl:
            return base_style + " background:#fff3cd; font-weight:600; padding:2px 4px; border-radius:2px;"
        return base_style

    risk = _safe_float(attrs.get("传播风险分", 0.0))
    level = attrs.get("传播层级", 0)

    # ── 头部：保单基本信息 ──────────────────────────────────────────────────
    short_id = escape(node_id[:20] + "…" if len(node_id) > 20 else node_id)
    header = (
        f'<div style="font-family:Arial,sans-serif; font-size:12px; max-width:340px;">'
        f'<div style="background:#2c3e50; color:#fff; padding:5px 10px; border-radius:4px 4px 0 0; font-weight:600; font-size:13px;">'
        f'  保单: {short_id}'
        f'</div>'
        f'<div style="background:#f0f4f8; padding:4px 10px; border-bottom:1px solid #dee2e6; font-size:11px; color:#444;">'
        f'  传播层级: <b>{level}</b>'
        f'  &nbsp;·&nbsp; 风险分: <b>{risk:.4f}</b>'
        f'  &nbsp;·&nbsp; 连接度: <b>{degree}</b>'
        f'</div>'
    )

    if not cases:
        return (
            header
            + '<div style="padding:8px 10px; font-size:12px; color:#888;">未找到关联赔案信息</div>'
            + '</div>'
        )

    # ── 赔案卡片列表 ────────────────────────────────────────────────────────
    top_cases = cases[:3]
    section_title = (
        f'<div style="padding:5px 10px 2px 10px; font-size:11px; font-weight:600; color:#555;">'
        f'关联赔案（Top {len(top_cases)}）</div>'
    )

    cards: List[str] = []
    for idx, c in enumerate(top_cases, start=1):
        mo = _safe_float(c.get("mo_score", 0.0), 0.0)
        border_color = _mo_border_color(mo)
        disease_text = "、".join(c.get("疾病名称") or []) or "-"
        hosp = escape(str(c.get("医院名称") or "-"))
        case_no = escape(str(c.get("案件号") or "-"))
        report_date = escape(str(c.get("报案日期") or "-"))
        visit_type = escape(str(c.get("就诊类型") or "-"))
        los = _safe_int(c.get("住院天数"), 0)
        amount = _safe_float(c.get("赔付金额", 0.0), 0.0)
        blacklist = c.get("是否黑名单", False)
        bl_badge = (
            ' <span style="background:#cc0000;color:#fff;border-radius:3px;padding:0 4px;font-size:10px;">黑名单</span>'
            if blacklist else ""
        )
        mo_bar_width = int(mo * 60)
        mo_bar = (
            f'<div style="display:inline-block; width:{mo_bar_width}px; height:6px; '
            f'background:{border_color}; border-radius:3px; vertical-align:middle; margin-left:4px;"></div>'
        )

        rows_html = (
            f'<table style="border-collapse:collapse; width:100%;">'
            f'<tr><td style="{TD_LABEL}">案件号</td>'
            f'    <td style="{TD_VALUE}">{case_no}{bl_badge}</td></tr>'
            f'<tr><td style="{TD_LABEL}">报案日期</td>'
            f'    <td style="{TD_VALUE}">{report_date}</td></tr>'
            f'<tr><td style="{TD_LABEL}">就诊类型</td>'
            f'    <td style="{_td_style("就诊类型")}">{visit_type}</td></tr>'
            f'<tr><td style="{TD_LABEL}">住院天数</td>'
            f'    <td style="{_td_style("住院天数")}">{los} 天</td></tr>'
            f'<tr><td style="{TD_LABEL}">赔付金额</td>'
            f'    <td style="{_td_style("赔付金额")}">{amount:,.2f}</td></tr>'
            f'<tr><td style="{TD_LABEL}">疾病</td>'
            f'    <td style="{_td_style("疾病名称")}">{escape(disease_text)}</td></tr>'
            f'<tr><td style="{TD_LABEL}">医院</td>'
            f'    <td style="{_td_style("医院名称")}">{hosp}</td></tr>'
            f'<tr><td style="{TD_LABEL}">MO 相似分</td>'
            f'    <td style="{TD_VALUE}">{mo:.3f}{mo_bar}</td></tr>'
            f'</table>'
        )

        bg = "#fffbf0" if idx % 2 == 0 else "#ffffff"
        cards.append(
            f'<div style="margin:4px 8px 4px 8px; padding:6px 8px; background:{bg}; '
            f'border:1px solid #e0e0e0; border-left:4px solid {border_color}; border-radius:0 4px 4px 0;">'
            + rows_html
            + '</div>'
        )

    return header + section_title + "".join(cards) + '</div>'


def _render_profile_card_html(
    title: str,
    profile: Dict[str, Any],
    fraud_hypothesis: Optional[Dict[str, Any]] = None,
) -> str:
    """生成集体画像卡片 HTML；欺诈假设相关统计行以黄色背景高亮，底部显示假设描述横幅。"""
    highlighted_fields: Set[str] = set((fraud_hypothesis or {}).get("highlighted_fields") or [])
    hyp_desc = str((fraud_hypothesis or {}).get("description") or "").strip()

    def _row_style(canonical: Optional[str]) -> str:
        if canonical and canonical in highlighted_fields:
            return "margin:3px 0; background:#fff3cd; padding:2px 4px; border-radius:3px; font-weight:600;"
        return "margin:3px 0;"

    def _marker(canonical: Optional[str]) -> str:
        return " \U0001f50d" if canonical and canonical in highlighted_fields else ""

    rows: List[str] = [
        f'<p style="{_row_style(None)}">'
        f'保单数: {profile.get("policy_count", 0)} / 赔案数: {profile.get("case_count", 0)}</p>'
    ]
    for prof_key, label, canonical, fmt in PROFILE_STAT_CONFIG:
        val = profile.get(prof_key)
        st = _row_style(canonical)
        mk = _marker(canonical)
        if fmt == "list":
            items = val or []
            text = "；".join([f"{escape(str(n))}({c})" for n, c in items]) or "-"
            rows.append(f'<p style="{st}"><b>{label}{mk}</b>: {text}</p>')
        else:
            try:
                text = str(int(val or 0)) if fmt == "i" else format(_safe_float(val or 0.0, 0.0), fmt)
            except Exception:
                text = str(val or "-")
            rows.append(f'<p style="{st}">{label}{mk}: {text}</p>')

    hyp_banner = ""
    if hyp_desc:
        hyp_banner = (
            f'<p style="margin:6px 0 0 0; font-size:11px; color:#856404; background:#fff3cd; '
            f'padding:3px 5px; border-radius:3px;">\U0001f50d {escape(hyp_desc)}</p>'
        )

    rows_html = "\n    ".join(rows)
    return (
        f'<div style="position:absolute; bottom:12px; right:12px; z-index:9998; '
        f'background:rgba(255,255,255,0.94); padding:10px; border:1px solid #ccc; '
        f'border-radius:6px; font-family:Arial; font-size:12px; max-width:320px; '
        f'box-shadow:0 2px 6px rgba(0,0,0,0.12);">\n'
        f'    <h4 style="margin:0 0 6px 0;font-size:13px;">{escape(title)} | 集体画像</h4>\n'
        f'    {rows_html}\n'
        f'    {hyp_banner}\n'
        f'</div>'
    )


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _normalize_id_node(rel_type: str, rel_value: str) -> str:
    return f"{ID_NODE_PREFIX}{rel_type}__{rel_value}"


def build_hetero_graph(data: Dict[str, Any], min_edge_weight: float = 0.05) -> nx.Graph:
    nodes = data.get("保单节点列表") or []
    edges = data.get("传播边列表") or []
    G = nx.Graph()

    for n in nodes:
        pid = str(n.get("保单号") or "").strip()
        if not pid:
            continue
        G.add_node(
            pid,
            node_type="policy",
            label=pid,
            是否种子=bool(n.get("是否种子", False)),
            传播层级=int(n.get("传播层级", 0) or 0),
            传播风险分=_safe_float(n.get("传播风险分", 0.0)),
        )

    for e in edges:
        src = str(e.get("源保单号") or "").strip()
        dst = str(e.get("目标保单号") or "").strip()
        if not src and not dst:
            continue
        if src == dst:
            continue

        weight = _safe_float(e.get("边权重", 0.0))
        if weight < min_edge_weight:
            continue

        rel_type = str(e.get("关联类型") or "未知")
        rel_val = str(e.get("关联ID值") or "未知")
        id_node = _normalize_id_node(rel_type, rel_val)

        if id_node not in G:
            G.add_node(
                id_node,
                node_type="id",
                关联类型=rel_type,
                关联ID值=rel_val,
                label=f"{rel_type}:{rel_val}",
            )

        # 不要求两端保单同时在 G，单端在 G 即可建连接 ID 节点（第 0 层种子保单可能在边中只出现一次）
        if src and src in G and not G.has_edge(src, id_node):
            G.add_edge(src, id_node, weight=weight, 关联类型=rel_type, 关联ID值=rel_val)
        if dst and dst in G and not G.has_edge(dst, id_node):
            G.add_edge(dst, id_node, weight=weight, 关联类型=rel_type, 关联ID值=rel_val)

    return G


def build_community_graph_from_edges(
    raw_edges: List[Dict[str, Any]],
    policy_set: Set[str],
    policy_attrs: Dict[str, Dict[str, Any]],
) -> nx.Graph:
    """为指定保单集合构建保单↔ID 二部图。

    直接扫描传播边列表，凡 src 或 dst 在 policy_set 中的边均纳入，
    不过滤 min_edge_weight（子社群边数少，全量保留以保证结构完整性）。
    """
    G = nx.Graph()
    for pid in policy_set:
        attrs = policy_attrs.get(pid, {})
        G.add_node(
            pid,
            node_type="policy",
            label=pid,
            是否种子=bool(attrs.get("是否种子", False)),
            传播层级=int(attrs.get("传播层级", 0) or 0),
            传播风险分=_safe_float(attrs.get("传播风险分", 0.0)),
        )

    for e in raw_edges:
        src = str(e.get("源保单号") or "").strip()
        dst = str(e.get("目标保单号") or "").strip()
        if not src and not dst:
            continue
        if src not in policy_set and dst not in policy_set:
            continue
        rel_type = str(e.get("关联类型") or "未知")
        rel_val = str(e.get("关联ID值") or "未知")
        weight = _safe_float(e.get("边权重", 0.0))
        id_node = _normalize_id_node(rel_type, rel_val)

        if id_node not in G:
            G.add_node(
                id_node,
                node_type="id",
                关联类型=rel_type,
                关联ID值=rel_val,
                label=f"{rel_type}:{rel_val}",
            )
        if src in policy_set and not G.has_edge(src, id_node):
            G.add_edge(src, id_node, weight=weight, 关联类型=rel_type, 关联ID值=rel_val)
        if dst in policy_set and not G.has_edge(dst, id_node):
            G.add_edge(dst, id_node, weight=weight, 关联类型=rel_type, 关联ID值=rel_val)

    return G


def _risk_to_red(score: float, min_score: float, max_score: float) -> str:
    if max_score <= min_score:
        return MACRO_RED_SCALE[-1]
    ratio = (score - min_score) / (max_score - min_score)
    ratio = max(0.0, min(1.0, ratio))
    idx = int(ratio * (len(MACRO_RED_SCALE) - 1))
    return MACRO_RED_SCALE[idx]


def _extract_policy_members(
    node_to_comm: Dict[str, str],
    comm_id: str,
) -> Set[str]:
    return {n for n, c in node_to_comm.items() if c == comm_id and not n.startswith(ID_NODE_PREFIX)}


def _extract_comm_risk_map(leiden_results: List[Dict[str, Any]]) -> Dict[str, float]:
    risk_map: Dict[str, float] = {}
    for item in leiden_results:
        cid = str(item.get("社群编号") or "").strip()
        if not cid:
            continue
        risk_map[cid] = _safe_float(item.get("社群风险分", 0.0))
    return risk_map


def _extract_comm_size_map(leiden_results: List[Dict[str, Any]], node_to_comm: Dict[str, str]) -> Dict[str, int]:
    size_map: Dict[str, int] = {}
    for item in leiden_results:
        cid = str(item.get("社群编号") or "").strip()
        if not cid:
            continue
        scale = int(item.get("规模", 0) or 0)
        if scale > 0:
            size_map[cid] = scale
        else:
            size_map[cid] = len(_extract_policy_members(node_to_comm, cid))
    return size_map


def _community_label_by_degree(
    raw_edges: List[Dict[str, Any]],
    comm_id: str,
    node_to_comm: Dict[str, str],
) -> str:
    """用传播边中的出现频次（度）找出该社群内最具代表性的保单作为标签。"""
    members = {n for n, c in node_to_comm.items() if c == comm_id}
    if not members:
        return comm_id

    deg: Dict[str, int] = defaultdict(int)
    for e in raw_edges:
        src = str(e.get("源保单号") or "").strip()
        dst = str(e.get("目标保单号") or "").strip()
        if src in members:
            deg[src] += 1
        if dst in members:
            deg[dst] += 1

    if not deg:
        top = next(iter(members))
    else:
        top = max(deg.items(), key=lambda x: x[1])[0]

    short = str(top)[:12] + "..." if len(str(top)) > 12 else str(top)
    return f"{comm_id} ({short})"


def build_macro_edges(
    raw_edges: List[Dict[str, Any]],
    node_to_comm: Dict[str, str],
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """遍历传播边列表，统计跨社群的共享 ID 数量。

    每条边 (src_policy, dst_policy, rel_type, rel_id_value)：
      - 查 node_to_comm 确认两端所属社群
      - 若社群不同，则该 rel_id_value 是跨社群的共享 ID
    用 (comm_a, comm_b, rel_id_value) 三元组去重，防止同一 ID
    因多条边被重复计数。
    """
    # seen[(comm_a, comm_b)] = set of unique rel_id_values
    seen: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    rel_types: Dict[Tuple[str, str], Set[str]] = defaultdict(set)

    for e in raw_edges:
        src = str(e.get("源保单号") or "").strip()
        dst = str(e.get("目标保单号") or "").strip()
        if not src or not dst:
            continue
        comm_src = node_to_comm.get(src)
        comm_dst = node_to_comm.get(dst)
        if not comm_src or not comm_dst or comm_src == comm_dst:
            continue
        rel_type = str(e.get("关联类型") or "未知")
        rel_val = str(e.get("关联ID值") or "")
        key: Tuple[str, str] = tuple(sorted([comm_src, comm_dst]))  # type: ignore[assignment]
        seen[key].add(rel_val)
        rel_types[key].add(rel_type)

    return {
        key: {"weight": len(ids), "relation_types": rel_types[key]}
        for key, ids in seen.items()
    }


def render_macro_graph(
    raw_edges: List[Dict[str, Any]],
    leiden_results: List[Dict[str, Any]],
    node_to_comm: Dict[str, str],
    output_path: str,
) -> str:
    risk_map = _extract_comm_risk_map(leiden_results)
    mo_map = _extract_comm_mo_map(leiden_results)
    size_map = _extract_comm_size_map(leiden_results, node_to_comm)

    comm_ids = sorted(size_map.keys())
    if not comm_ids:
        return ""

    min_risk = min(risk_map.get(cid, 0.0) for cid in comm_ids)
    max_risk = max(risk_map.get(cid, 0.0) for cid in comm_ids)
    min_mo = min(mo_map.get(cid, 0.0) for cid in comm_ids)
    max_mo = max(mo_map.get(cid, 0.0) for cid in comm_ids)

    nodes = []
    for cid in comm_ids:
        size_val = size_map.get(cid, 1)
        risk_val = risk_map.get(cid, 0.0)
        mo_val = mo_map.get(cid, 0.0)
        label = _community_label_by_degree(raw_edges, cid, node_to_comm)
        nodes.append(
            {
                "name": cid,
                "symbolSize": max(10, min(60, 8 + int(math.log(max(size_val, 1) + 1, 1.5)))),
                "value": size_val,
                "itemStyle": {"color": _risk_to_red(risk_val, min_risk, max_risk)},
                "riskVal": round(risk_val, 6),
                "moVal": round(mo_val, 6),
                "tooltip": {
                    "formatter": (
                        f"社群: {cid}<br/>标签: {label}<br/>保单规模: {size_val}"
                        f"<br/>风险分: {risk_val:.4f}<br/>手法相似分: {mo_val:.4f}"
                    )
                },
                "label": {"show": True, "formatter": cid},
            }
        )

    macro_edges = build_macro_edges(raw_edges, node_to_comm)
    links = []
    if macro_edges:
        weights = [v["weight"] for v in macro_edges.values()]
        min_w = min(weights)
        max_w = max(weights)

        for (s, d), meta in macro_edges.items():
            w = int(meta["weight"])
            if max_w == min_w:
                width = 2
            else:
                width = 1 + (w - min_w) / (max_w - min_w) * 6
            rel_info = "、".join(sorted(meta["relation_types"]))
            links.append(
                {
                    "source": s,
                    "target": d,
                    "value": w,
                    "lineStyle": {"width": width, "color": "#b0b0b0"},
                    "tooltip": {"formatter": f"跨社群共享ID数: {w}<br/>关联类型: {rel_info}"},
                }
            )

    graph = (
        Graph(init_opts=opts.InitOpts(width="1800px", height="1200px"))
        .add(
            "",
            nodes,
            links,
            repulsion=1200,
            layout="force",
            is_roam=True,
            is_draggable=True,
            linestyle_opts=opts.LineStyleOpts(curve=0.2, opacity=0.7),
            label_opts=opts.LabelOpts(is_show=True),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="Leiden 社群宏观关联图"),
            tooltip_opts=opts.TooltipOpts(trigger="item"),
            toolbox_opts=opts.ToolboxOpts(is_show=True),
            legend_opts=opts.LegendOpts(is_show=False),
        )
    )

    dir_part = os.path.dirname(output_path)
    if dir_part:
        os.makedirs(dir_part, exist_ok=True)
    graph.render(output_path)

    macro_switcher_html = f"""
<div id="macro-color-switcher" style="position:absolute; top:12px; right:12px; z-index:9999; background:rgba(255,255,255,0.95); border:1px solid #ccc; border-radius:6px; padding:8px 10px; font-family:Arial; font-size:12px;">
    <label style="margin-right:6px;">颜色维度</label>
    <select id="macro-color-mode">
        <option value="risk">风险分</option>
        <option value="mo">手法相似分</option>
    </select>
</div>
<script>
(function() {{
    function colorScale(v, min, max) {{
        if (isNaN(v)) return '#9c9c9c';
        var ratio = (max <= min) ? 1 : (v - min) / (max - min);
        ratio = Math.max(0, Math.min(1, ratio));
        var r = Math.round(219 + (204 - 219) * ratio);
        var g = Math.round(233 + (0 - 233) * ratio);
        var b = Math.round(246 + (0 - 246) * ratio);
        return '#' + [r,g,b].map(function(x){{return x.toString(16).padStart(2,'0');}}).join('');
    }}

    function findChart() {{
        var keys = Object.keys(window);
        for (var i = 0; i < keys.length; i++) {{
            var k = keys[i];
            var obj = window[k];
            if (k.indexOf('chart_') === 0 && obj && typeof obj.getOption === 'function' && typeof obj.setOption === 'function') {{
                return obj;
            }}
        }}
        return null;
    }}

    var chart = findChart();
    var modeEl = document.getElementById('macro-color-mode');
    if (!chart || !modeEl) return;

    function recolor(mode) {{
        var option = chart.getOption();
        if (!option || !option.series || !option.series[0] || !option.series[0].data) return;
        var data = option.series[0].data;
        var vals = [];
        for (var i = 0; i < data.length; i++) {{
            var dv = mode === 'mo' ? Number(data[i].moVal) : Number(data[i].riskVal);
            if (!isNaN(dv)) vals.push(dv);
        }}
        var min = vals.length ? Math.min.apply(null, vals) : 0;
        var max = vals.length ? Math.max.apply(null, vals) : 1;
        for (var j = 0; j < data.length; j++) {{
            var v = mode === 'mo' ? Number(data[j].moVal) : Number(data[j].riskVal);
            data[j].itemStyle = data[j].itemStyle || {{}};
            data[j].itemStyle.color = colorScale(v, min, max);
        }}
        chart.setOption(option, false, true);
    }}

    modeEl.addEventListener('change', function() {{ recolor(this.value); }});
}})();
</script>
"""
    _inject_before_body_end(output_path, macro_switcher_html)
    return output_path


def _id_node_color(node_attrs: Dict[str, Any]) -> str:
    rel_type = str(node_attrs.get("关联类型") or "unknown")
    return NODE_COLOR_MAP.get(rel_type, NODE_COLOR_MAP["unknown"])


def _node_title(node_id: str, attrs: Dict[str, Any], degree: int) -> str:
    """兜底纯文本 tooltip（保留给无自定义 tooltip 的场景）。"""
    if attrs.get("node_type") == "policy":
        return (
            f"保单号: {node_id}<br/>"
            f"传播层级: {attrs.get('传播层级', 0)}<br/>"
            f"传播风险分: {_safe_float(attrs.get('传播风险分', 0.0)):.4f}<br/>"
            f"连接度: {degree}"
        )
    return (
        f"关联类型: {attrs.get('关联类型', '未知')}<br/>"
        f"关联ID值: {attrs.get('关联ID值', '')}<br/>"
        f"连接保单数: {degree}"
    )


def _build_id_tooltip_html(
    node_id: str,
    attrs: Dict[str, Any],
    degree: int,
    sub: "nx.Graph",
) -> str:
    """为关联 ID 节点生成卡片式 tooltip，列出所有直接连接的保单节点。"""
    rel_type = str(attrs.get("关联类型") or "未知")
    rel_val  = str(attrs.get("关联ID值")  or "")
    color    = NODE_COLOR_MAP.get(rel_type, NODE_COLOR_MAP["unknown"])

    # 收集直连保单
    neighbors = [
        n for n in sub.neighbors(node_id)
        if not str(n).startswith(ID_NODE_PREFIX)
    ]
    neighbors_sorted = sorted(neighbors)

    TD_L = "padding:2px 6px 2px 0; color:#666; white-space:nowrap; vertical-align:top; font-size:11px;"
    TD_V = "padding:2px 0; vertical-align:top; font-size:12px;"

    # 头部色块与基本信息
    short_val = escape(rel_val[:28] + "…" if len(rel_val) > 28 else rel_val)
    header = (
        f'<div style="font-family:Arial,sans-serif; font-size:12px; max-width:320px;">'
        f'<div style="background:{color}; color:#fff; padding:5px 10px; '
        f'border-radius:4px 4px 0 0; font-weight:600; font-size:13px;">'
        f'  {escape(rel_type)}'
        f'</div>'
        f'<div style="background:#f0f4f8; padding:4px 10px; border-bottom:1px solid #dee2e6; font-size:11px; color:#444;">'
        f'  ID值: <b>{short_val}</b>'
        f'  &nbsp;·&nbsp; 连接保单数: <b>{degree}</b>'
        f'</div>'
    )

    if not neighbors_sorted:
        return header + '<div style="padding:8px 10px; color:#888;">无直连保单</div></div>'

    section = (
        f'<div style="padding:5px 10px 2px 10px; font-size:11px; font-weight:600; color:#555;">'
        f'直连保单（{len(neighbors_sorted)} 个）</div>'
    )
    rows_html = "".join(
        f'<div style="padding:3px 10px; font-size:12px; border-bottom:1px solid #f0f0f0; '
        f'font-family:monospace;">'
        f'{escape(str(p))}'
        f'</div>'
        for p in neighbors_sorted[:10]
    )
    more = ""
    if len(neighbors_sorted) > 10:
        more = (
            f'<div style="padding:3px 10px 6px 10px; font-size:11px; color:#888;">'
            f'… 另 {len(neighbors_sorted) - 10} 个保单</div>'
        )

    return header + section + rows_html + more + "</div>"


def _filtered_subgraph_if_needed(sub: nx.Graph, max_nodes_before_filter: int) -> nx.Graph:
    if sub.number_of_nodes() <= max_nodes_before_filter:
        return sub

    keep_nodes = [n for n in sub.nodes() if sub.degree(n) > 1]
    filtered = sub.subgraph(keep_nodes).copy()

    # 防御：过滤后为空则回退原图
    if filtered.number_of_nodes() == 0:
        return sub
    return filtered


def render_pyvis_graph(
    G: nx.Graph,
    output_path: str,
    title: str,
    max_nodes_before_filter: int,
    policy_case_index: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    community_profile: Optional[Dict[str, Any]] = None,
    fraud_hypothesis: Optional[Dict[str, Any]] = None,
) -> str:
    sub = _filtered_subgraph_if_needed(G, max_nodes_before_filter=max_nodes_before_filter)
    highlighted_fields: Set[str] = set((fraud_hypothesis or {}).get("highlighted_fields") or [])

    net = Network(
        height="900px",
        width="100%",
        select_menu=True,
        filter_menu=True,
        cdn_resources="remote",
        directed=False,
        notebook=False,
    )

    # 自定义 tooltip 字典：node_id -> HTML，绕过 vis.js 的纯文本 title 限制
    custom_tooltips: Dict[str, str] = {}

    for node, attrs in sub.nodes(data=True):
        degree = sub.degree(node)
        node_type = attrs.get("node_type", "policy")
        if node_type == "policy":
            risk_score = _safe_float(attrs.get("传播风险分", 0.0), 0.0)
            cases = (policy_case_index or {}).get(str(node), [])
            mo_score = max((_safe_float(c.get("mo_score", 0.0), 0.0) for c in cases), default=0.0)
            color = _score_to_color(risk_score, 0.0, 1.0)
            size = 8 + min(30, degree)
            label = str(node)
            # 富文本 tooltip 放进自定义字典，title 留空避免 vis.js 纯文本渲染
            custom_tooltips[str(node)] = _build_case_tooltip_html(
                str(node), attrs, degree, cases, highlighted_fields
            )
            vis_title = ""
        else:
            color = _id_node_color(attrs)
            size = 8 + min(35, degree)
            rel_type = str(attrs.get("关联类型") or "ID")
            rel_val = str(attrs.get("关联ID值") or "")
            short_val = rel_val[:12] + "..." if len(rel_val) > 12 else rel_val
            label = f"{rel_type}:{short_val}"
            risk_score = 0.0
            mo_score = 0.0
            # ID 节点也走自定义卡片 tooltip
            custom_tooltips[str(node)] = _build_id_tooltip_html(str(node), attrs, degree, sub)
            vis_title = ""

        net.add_node(
            str(node),
            label=label,
            size=size,
            color=color,
            title=vis_title,
            group=node_type,
            risk_score=round(risk_score, 6),
            mo_score=round(mo_score, 6),
        )

    for u, v, d in sub.edges(data=True):
        rel_type = str(d.get("关联类型") or "未知")
        rel_val = str(d.get("关联ID值") or "")
        w = _safe_float(d.get("weight", 0.0), 0.0)
        net.add_edge(
            str(u),
            str(v),
            width=1 + min(5.0, w * 5),
            color="#b0b0b0",
            title=f"{rel_type}: {rel_val}",
        )

    net.show_buttons(filter_=["nodes"])

    net.set_options(
        """
        var options = {
          "edges": {
            "color": {"inherit": false},
            "smooth": false
          },
          "interaction": {
            "hover": true,
            "navigationButtons": true,
            "multiselect": true
          },
          "physics": {
            "enabled": true,
            "forceAtlas2Based": {
              "gravitationalConstant": -50,
              "springLength": 120,
              "springConstant": 0.08
            },
            "maxVelocity": 50,
            "minVelocity": 0.1,
            "solver": "forceAtlas2Based"
          }
                    ,
                    "configure": {
                        "enabled": true
                    }
                }
        """
    )

    dir_part = os.path.dirname(output_path)
    if dir_part:
        os.makedirs(dir_part, exist_ok=True)
    net.show(output_path, notebook=False)

    profile = community_profile or {}
    profile_card_html = _render_profile_card_html(title, profile, fraud_hypothesis)

    # 序列化自定义 tooltip 字典为 JS 对象字面量（json.dumps 负责转义）
    tooltips_js = json.dumps(custom_tooltips, ensure_ascii=False)

    overlay_html = f"""
<!-- 颜色维度切换器 -->
<div id="subgraph-color-switcher" style="position:absolute; top:12px; left:12px; z-index:9999; background:rgba(255,255,255,0.95); border:1px solid #ccc; border-radius:6px; padding:8px 10px; font-family:Arial; font-size:12px;">
    <label style="margin-right:6px;">颜色维度</label>
    <select id="subgraph-color-mode">
        <option value="risk">风险分</option>
        <option value="mo">手法相似分</option>
    </select>
</div>

<!-- 社群画像卡片 -->
{profile_card_html}

<!-- 自定义富文本 tooltip 容器（绕过 vis.js 纯文本渲染限制） -->
<div id="custom-node-tooltip"
     style="position:fixed; z-index:99999; pointer-events:none; display:none;
            max-width:380px; border-radius:4px; overflow:hidden;
            box-shadow:0 4px 20px rgba(0,0,0,0.22);
            font-family:Arial,sans-serif; font-size:12px;
            background:#fff;">
</div>

<script>
(function() {{
    function placeSwitcher() {{
        var sw = document.getElementById('subgraph-color-switcher');
        if (!sw) return;
        try {{
            var candidates = Array.prototype.slice.call(document.querySelectorAll('body > div'));
            var ctrl = null;
            for (var i = 0; i < candidates.length; i++) {{
                var el = candidates[i];
                var r = el.getBoundingClientRect();
                if (r.left <= 80 && r.top <= 80 && (el.querySelector('button') || el.querySelector('select') || el.querySelector('svg'))) {{
                    ctrl = el; break;
                }}
            }}
            if (ctrl) {{
                var r = ctrl.getBoundingClientRect();
                sw.style.left = (r.left) + 'px';
                sw.style.top = (r.bottom + 6) + 'px';
            }} else {{
                sw.style.left = '12px'; sw.style.top = '60px';
            }}
        }} catch (e) {{}}
    }}
    window.addEventListener('resize', function() {{ setTimeout(placeSwitcher, 50); }});

    /* ── 1. 颜色维度切换 ──────────────────────────────────────── */
    var modeEl = document.getElementById('subgraph-color-mode');
    if (modeEl && typeof nodes !== 'undefined') {{
        function colorScale(v, min, max) {{
            if (isNaN(v)) return '#4e79a7';
            var ratio = (max <= min) ? 1 : (v - min) / (max - min);
            ratio = Math.max(0, Math.min(1, ratio));
            var r = Math.round(219 + (204 - 219) * ratio);
            var g = Math.round(233 + (0 - 233) * ratio);
            var b = Math.round(246 + (0 - 246) * ratio);
            return '#' + [r,g,b].map(function(x){{return x.toString(16).padStart(2,'0');}}).join('');
        }}
        function applyColorMode(mode) {{
            var all = nodes.get();
            var vals = [];
            for (var i = 0; i < all.length; i++) {{
                if (all[i].group !== 'policy') continue;
                var v = mode === 'mo' ? Number(all[i].mo_score) : Number(all[i].risk_score);
                if (!isNaN(v)) vals.push(v);
            }}
            var min = vals.length ? Math.min.apply(null, vals) : 0;
            var max = vals.length ? Math.max.apply(null, vals) : 1;
            var updates = [];
            for (var j = 0; j < all.length; j++) {{
                if (all[j].group !== 'policy') continue;
                var val = mode === 'mo' ? Number(all[j].mo_score) : Number(all[j].risk_score);
                var c = colorScale(val, min, max);
                updates.push({{id: all[j].id, color: c}});
                if (typeof nodeColors !== 'undefined') nodeColors[all[j].id] = c;
            }}
            if (updates.length) nodes.update(updates);
        }}
        modeEl.addEventListener('change', function() {{ applyColorMode(this.value); }});
        // 在 DOM 就绪并在 pyvis 创建控制面板后，尝试把开关放到控制面板下方
        setTimeout(placeSwitcher, 300);
    }}

    /* ── 2. 自定义富文本 tooltip ─────────────────────────────── */
    var TOOLTIPS = {tooltips_js};
    var tipEl    = document.getElementById('custom-node-tooltip');
    var mouseX   = 0;
    var mouseY   = 0;

    document.addEventListener('mousemove', function(e) {{
        mouseX = e.clientX;
        mouseY = e.clientY;
        if (tipEl && tipEl.style.display !== 'none') repositionTip();
    }});

    function repositionTip() {{
        var GAP = 16;
        var vw  = window.innerWidth  || document.documentElement.clientWidth;
        var vh  = window.innerHeight || document.documentElement.clientHeight;
        var w   = tipEl.offsetWidth  || 360;
        var h   = tipEl.offsetHeight || 200;
        var x   = mouseX + GAP;
        var y   = mouseY + GAP;
        if (x + w > vw) x = mouseX - w - GAP;
        if (y + h > vh) y = mouseY - h - GAP;
        if (x < 0) x = 4;
        if (y < 0) y = 4;
        tipEl.style.left = x + 'px';
        tipEl.style.top  = y + 'px';
    }}

    // 等待 network 对象就绪（pyvis 在同一脚本块里创建，通常同步可得）
    function attachNetworkEvents() {{
        if (typeof network === 'undefined' || !network) {{
            setTimeout(attachNetworkEvents, 100);
            return;
        }}
        network.on('hoverNode', function(params) {{
            var html = TOOLTIPS[params.node];
            if (!html || !tipEl) return;
            tipEl.innerHTML = html;
            tipEl.style.display = 'block';
            repositionTip();
        }});
        network.on('blurNode', function() {{
            if (tipEl) tipEl.style.display = 'none';
        }});
    }}
    attachNetworkEvents();
}})();
</script>
"""

    _inject_before_body_end(output_path, overlay_html)

    return output_path


def _top2_risky_communities(leiden_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # 至少有 2 个保单节点的社群才有延可视化意义（孤立种子节点不绘）
    candidates = [
        c for c in leiden_results
        if int(c.get("规模", 0) or 0) >= 2
    ]
    ranked = sorted(
        candidates,
        key=lambda x: _safe_float(x.get("社群风险分", 0.0), 0.0),
        reverse=True,
    )
    return ranked[:2]


def generate_html_visualizations(
    raw_input_path: str,
    cluster_output_path: str,
    output_dir: str,
    min_edge_weight: float = 0.05,
    max_nodes_before_filter: int = 300,
) -> Dict[str, Any]:
    raw = _load_json(raw_input_path)
    cluster = _load_json(cluster_output_path)

    raw_edges: List[Dict[str, Any]] = raw.get("传播边列表") or []

    # 构建保单属性字典，供子社群构图时使用
    policy_attrs: Dict[str, Dict[str, Any]] = {
        str(n.get("保单号") or "").strip(): n
        for n in (raw.get("保单节点列表") or [])
        if n.get("保单号")
    }

    leiden_results = cluster.get("leiden_社群") or []
    node_to_comm = cluster.get("node_to_community_map") or {}
    mo_scores = cluster.get("mo_scores") or {}
    raw_cases: List[Dict[str, Any]] = raw.get("关联案件列表") or []
    policy_case_index = build_policy_case_index(raw_cases, mo_scores)
    fraud_hypothesis = extract_fraud_hypothesis(raw, cluster)

    generated_files: Dict[str, Any] = {
        "macro": None,
        "subgraphs": [],
        "full": None,
    }

    has_leiden = bool(leiden_results) and bool(node_to_comm)

    if has_leiden:
        # 宏观社群图：直接用原始边列表 + node_to_community_map
        macro_file = os.path.join(output_dir, "macro_community_graph.html")
        rendered = render_macro_graph(
            raw_edges=raw_edges,
            leiden_results=leiden_results,
            node_to_comm=node_to_comm,
            output_path=macro_file,
        )
        generated_files["macro"] = rendered or None

        top2 = _top2_risky_communities(leiden_results)
        for idx, comm in enumerate(top2, start=1):
            cid = str(comm.get("社群编号") or "").strip()
            if not cid:
                continue
            # 社群内保单集合：从 node_to_community_map 取（仅保单，无 __id__ 前缀）
            policy_set: Set[str] = {
                n for n, c in node_to_comm.items()
                if c == cid and not n.startswith(ID_NODE_PREFIX)
            }
            if not policy_set:
                continue
            # 直接扫描传播边列表构建子社群图（保单↔ID 二部图）
            sub = build_community_graph_from_edges(raw_edges, policy_set, policy_attrs)
            comm_profile = build_community_profile(policy_set, policy_case_index)
            sub_path = os.path.join(output_dir, f"subgraph_{cid}_rank{idx}.html")
            render_pyvis_graph(
                sub,
                output_path=sub_path,
                title=f"Top{idx} 风险子社群 {cid}",
                max_nodes_before_filter=max_nodes_before_filter,
                policy_case_index=policy_case_index,
                community_profile=comm_profile,
                fraud_hypothesis=fraud_hypothesis,
            )
            generated_files["subgraphs"].append(sub_path)
    else:
        # 无 Leiden 时才构建全量异构图
        G = build_hetero_graph(raw, min_edge_weight=min_edge_weight)
        full_path = os.path.join(output_dir, "full_graph.html")
        render_pyvis_graph(
            G,
            output_path=full_path,
            title="全量保单-ID 异构图",
            max_nodes_before_filter=max_nodes_before_filter,
            policy_case_index=policy_case_index,
            community_profile={"policy_count": len(policy_attrs), "case_count": len(raw_cases)},
            fraud_hypothesis=fraud_hypothesis,
        )
        generated_files["full"] = full_path
        return {
            "has_leiden": has_leiden,
            "graph_nodes": G.number_of_nodes(),
            "graph_edges": G.number_of_edges(),
            "generated_files": generated_files,
        }

    return {
        "has_leiden": has_leiden,
        "raw_edges_count": len(raw_edges),
        "communities_total": len(leiden_results),
        "generated_files": generated_files,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="生成社群 HTML 可视化（macro + subgraph / full）")
    p.add_argument("--raw-input", required=True, help="原始三层输入 JSON")
    p.add_argument("--cluster-output", required=True, help="cluster_analysis 输出 JSON")
    p.add_argument("--output-dir", required=True, help="HTML 输出目录")
    p.add_argument("--min-edge-weight", type=float, default=0.05, help="最小边权")
    p.add_argument("--max-viz-nodes", type=int, default=300, help="超过该节点数才过滤 degree=1")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    result = generate_html_visualizations(
        raw_input_path=args.raw_input,
        cluster_output_path=args.cluster_output,
        output_dir=args.output_dir,
        min_edge_weight=args.min_edge_weight,
        max_nodes_before_filter=args.max_viz_nodes,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
