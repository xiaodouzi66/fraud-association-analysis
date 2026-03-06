#!/usr/bin/env python3
"""
根据 cluster_analysis v2 输出生成中文报告（聚焦聚集性风险结论 + 相似社群详解 + 全局传播概览）。

Usage:
    python scripts/generate_report.py output/cluster_v2_output.json \
      --raw-input sample_input.json \
      --viz-dir output \
      --report output/report_v2.md
"""

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple


MEDIUM_SIM_THRESHOLD = 0.4
HIGH_SIM_THRESHOLD = 0.6
HOSPITAL_EXCLUDE_KEYWORDS = ("药房", "药店")

# 金额分桶：(下界含, 上界不含, 标签)
AMOUNT_BUCKETS: List[Tuple[float, float, str]] = [
    (0,        1_000,        "0~1000元"),
    (1_000,    5_000,        "1000~5000元"),
    (5_000,    10_000,       "5000~1万元"),
    (10_000,   50_000,       "1万~5万元"),
    (50_000,   float("inf"), "5万以上"),
]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("input", help="cluster_analysis.py v2 输出 JSON")
    p.add_argument("--raw-input", default=None, help="原始三层输入 JSON（可选，用于全局统计）")
    p.add_argument("--viz-dir", default=None, help="社群可视化 PNG 目录（可选）")
    p.add_argument("--report", default="report_v2.md")
    return p.parse_args()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _to_date(x: Any) -> Optional[datetime.date]:
    if not x:
        return None
    s = str(x).strip()
    if not s or s.lower() in {"nat", "none", "null"}:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


def fmt_score(x: Any, ndigits: int = 4) -> str:
    try:
        return f"{float(x):.{ndigits}f}"
    except Exception:
        return "N/A"


def fmt_amount(x: Any) -> str:
    val = _safe_float(x, 0.0)
    if val >= 10000:
        return f"{val / 10000:.2f}万元"
    return f"{val:.2f}元"


def risk_level_cn(score: float) -> str:
    if score >= 0.7:
        return "🔴 高风险"
    if score >= 0.4:
        return "🟡 中风险"
    return "🟢 低风险"


def _find_seed_case_id(raw: Optional[Dict[str, Any]]) -> str:
    if not raw:
        return ""
    return str((raw.get("种子案件") or {}).get("案件号") or "").strip()


def _build_claim_maps(raw: Optional[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    by_policy: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    if not raw:
        return by_id, by_policy

    for c in raw.get("关联案件列表") or []:
        cid = str(c.get("案件号") or "").strip()
        pid = str(c.get("保单号") or "").strip()
        if cid:
            by_id[cid] = c
        if pid:
            by_policy[pid].append(c)
    return by_id, by_policy


def _extract_similar_claims_for_community(
    comm: Dict[str, Any],
    claim_by_policy: Dict[str, List[Dict[str, Any]]],
    mo_scores: Dict[str, float],
    seed_case_id: str,
) -> List[Dict[str, Any]]:
    members = set(str(x) for x in (comm.get("成员保单") or []))
    if not members:
        bridges = comm.get("桥接节点列表") or []
        members = set(str(b.get("保单号") or "").strip() for b in bridges if str(b.get("保单号") or "").strip())

    rows: List[Dict[str, Any]] = []
    for pid in members:
        for c in claim_by_policy.get(pid, []):
            cid = str(c.get("案件号") or "").strip()
            if not cid or cid == seed_case_id:
                continue
            mo = _safe_float(mo_scores.get(cid), 0.0)
            if mo < MEDIUM_SIM_THRESHOLD:
                continue
            rows.append({
                "案件号": cid,
                "保单号": str(c.get("保单号") or ""),
                "报案日期": c.get("报案日期"),
                "医院名称": c.get("医院名称"),
                "疾病名称": c.get("疾病名称"),
                "住院天数": c.get("住院天数"),
                "赔付金额": _safe_float(c.get("赔付金额"), 0.0),
                "是否黑名单": bool(c.get("是否黑名单", False)),
                "mo_score": mo,
            })

    uniq = {}
    for r in rows:
        uniq[r["案件号"]] = r

    out = list(uniq.values())
    out.sort(key=lambda x: (x["mo_score"], x["赔付金额"]), reverse=True)
    return out


def _community_visual_path(viz_dir: Optional[str], comm_id: str) -> Optional[str]:
    if not viz_dir:
        return None
    candidates = [
        os.path.join(viz_dir, f"community_{comm_id}.png"),
        os.path.join(viz_dir, f"community_comm_{comm_id}.png"),
        os.path.join(viz_dir, f"community_{str(comm_id).replace('comm_', '')}.png"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def _risk_conclusion(
    top_score: float,
    similar_communities: List[Dict[str, Any]],
    similar_claim_total: int,
) -> str:
    if similar_communities and similar_claim_total > 0:
        return "识别到聚集性相似风险，建议对相关社群及可疑案件开展专项复核。"
    if top_score >= 0.4:
        return "存在传播关联但未形成明确相似手法聚集，建议持续观察。"
    return "未识别到显著聚集性相似风险，建议常规监控。"


def render_section_1(
    clusters: List[Dict[str, Any]],
    communities: List[Dict[str, Any]],
    seed_case_id: str,
    similar_claim_total: int,
) -> str:
    max_cluster = max((_safe_float(c.get("gang_score"), 0.0) for c in clusters), default=0.0)
    max_comm = max((_safe_float(s.get("社群风险分"), 0.0) for s in communities), default=0.0)
    top_score = max(max_cluster, max_comm)

    sim_comm_cnt = len(communities)
    sim_policy_cnt = sum(_safe_int(c.get("规模"), 0) for c in communities)

    lines: List[str] = []
    lines.append("## 一、是否存在聚集性风险")
    lines.append("")
    lines.append(f"- 种子案件：{seed_case_id or '未知'}")
    lines.append(f"- 结论：{_risk_conclusion(top_score, communities, similar_claim_total)}")
    lines.append(f"- 风险等级：{risk_level_cn(top_score)}")
    lines.append(f"- 相似风险社群数：{sim_comm_cnt}")
    lines.append(f"- 相似风险涉及保单规模（社群累计）：{sim_policy_cnt}")
    lines.append(f"- 相似可疑案件数（高相似+中相似）：{similar_claim_total}")
    lines.append(f"- 最高团伙分：{fmt_score(max_cluster)}；最高社群分：{fmt_score(max_comm)}")
    lines.append("")
    return "\n".join(lines)


def _community_risk_clues(comm: Dict[str, Any], suspicious_claims: List[Dict[str, Any]]) -> List[str]:
    clues: List[str] = []
    ext = comm.get("扩展指标") or {}
    rel_dist = comm.get("关联类型分布") or {}

    high_cnt = sum(1 for c in suspicious_claims if c.get("mo_score", 0.0) >= HIGH_SIM_THRESHOLD)
    med_cnt = sum(1 for c in suspicious_claims if MEDIUM_SIM_THRESHOLD <= c.get("mo_score", 0.0) < HIGH_SIM_THRESHOLD)

    if high_cnt > 0:
        clues.append(f"存在{high_cnt}件高相似案件（MO≥{HIGH_SIM_THRESHOLD}）")
    if med_cnt > 0:
        clues.append(f"存在{med_cnt}件中相似案件（{MEDIUM_SIM_THRESHOLD}≤MO<{HIGH_SIM_THRESHOLD}）")

    if _safe_float(ext.get("hospital_conc"), 0.0) >= 0.2:
        clues.append("医院聚集度偏高")
    if _safe_float(ext.get("amount_cluster"), 0.0) >= 0.2:
        clues.append("赔付金额区间存在聚集")

    if rel_dist:
        k, v = sorted(rel_dist.items(), key=lambda x: x[1], reverse=True)[0]
        clues.append(f"主要传播关系为{k}（{v}条）")

    if not clues:
        clues.append("未识别到突出线索，建议结合人工规则复核")
    return clues[:4]


def render_section_2(
    communities: List[Dict[str, Any]],
    suspicious_by_comm: Dict[str, List[Dict[str, Any]]],
    viz_dir: Optional[str],
) -> str:
    lines: List[str] = []
    lines.append("## 二、相似风险社群与可疑案件")
    lines.append("")

    if not communities:
        lines.append("- 未识别到满足条件的相似风险社群（社群内至少1件中相似及以上案件）。")
        lines.append("")
        return "\n".join(lines)

    for idx, comm in enumerate(sorted(communities, key=lambda x: _safe_float(x.get("社群风险分"), 0.0), reverse=True), start=1):
        comm_id = str(comm.get("社群编号") or f"comm_{idx}")
        suspicious = suspicious_by_comm.get(comm_id, [])
        bridges = comm.get("桥接节点列表") or []

        lines.append(f"### 2.{idx} 社群 {comm_id}")
        lines.append("")
        lines.append(f"- 风险等级：{risk_level_cn(_safe_float(comm.get('社群风险分'), 0.0))}（社群风险分={fmt_score(comm.get('社群风险分'))}）")
        lines.append(f"- 整体规模：{_safe_int(comm.get('规模'), 0)} 张保单，{_safe_int(comm.get('边数'), 0)} 条关联边，{_safe_int(comm.get('涉及案件数'), 0)} 件案件，赔付金额 {fmt_amount(comm.get('涉及赔付金额'))}")
        lines.append(f"- 传播结构：{comm.get('传播路径描述', '暂无')}")

        clues = _community_risk_clues(comm, suspicious)
        lines.append("- 风险线索：")
        for c in clues:
            lines.append(f"  - {c}")

        if bridges:
            lines.append("- 关键传播节点（Top5）：")
            for b in bridges[:5]:
                lines.append(
                    f"  - {b.get('保单号')}｜层级{b.get('传播层级')}｜风险{fmt_score(b.get('传播风险分'), 3)}｜中介中心性{fmt_score(b.get('中介中心性'), 4)}"
                )

        viz = _community_visual_path(viz_dir, comm_id)
        if viz:
            lines.append(f"- 社群可视化：{viz}")

        lines.append("- 可疑案件明细（高相似+中相似）：")
        lines.append("  - 案件号 | 保单号 | 报案日期 | 医院 | 疾病 | 住院天数 | 赔付金额 | MO")
        for c in suspicious[:30]:
            dis = c.get("疾病名称") or []
            dis_txt = "、".join(str(x) for x in dis) if isinstance(dis, list) else str(dis)
            lines.append(
                f"  - {c.get('案件号')} | {c.get('保单号')} | {c.get('报案日期') or ''} | {c.get('医院名称') or ''} | {dis_txt} | {c.get('住院天数')} | {fmt_amount(c.get('赔付金额'))} | {fmt_score(c.get('mo_score'))}"
            )
        lines.append("")

    return "\n".join(lines)


def _is_valid_hospital(name: str) -> bool:
    s = str(name or "").strip()
    if not s:
        return False
    return not any(k in s for k in HOSPITAL_EXCLUDE_KEYWORDS)


def _detect_product_field(records: List[Dict[str, Any]]) -> Optional[str]:
    if not records:
        return None
    candidates = ["产品", "产品名称", "险种", "险种名称", "产品代码", "product", "product_name"]
    for key in candidates:
        for r in records:
            val = str(r.get(key) or "").strip()
            if val:
                return key
    return None


def _get_claim_disease_label(c: Dict[str, Any]) -> str:
    dis = c.get("疾病名称")
    if isinstance(dis, list):
        vals = [str(x).strip() for x in dis if str(x).strip()]
        return "、".join(vals) if vals else "未知疾病"
    txt = str(dis or "").strip()
    return txt if txt else "未知疾病"


def _top3_distribution(counter: Counter, amount_map: Dict[str, float], total_cnt: int, total_amount: float) -> Dict[str, Any]:
    top3 = []
    for name, cnt in counter.most_common(3):
        amt = float(amount_map.get(name, 0.0))
        top3.append({
            "名称": name,
            "案件数": cnt,
            "案件占比": (cnt / total_cnt) if total_cnt else 0.0,
            "赔付金额": amt,
            "赔付占比": (amt / total_amount) if total_amount else 0.0,
        })
    return {
        "top3": top3,
        "top3案件集中度": sum(x["案件数"] for x in top3) / total_cnt if total_cnt else 0.0,
        "top3赔付集中度": sum(x["赔付金额"] for x in top3) / total_amount if total_amount else 0.0,
    }


def _compute_concentration(claims_subset: List[Dict[str, Any]], claim_product_field: Optional[str], policy_to_product: Dict[str, str]) -> Dict[str, Any]:
    total_claims = len(claims_subset)
    total_amount = sum(_safe_float(c.get("赔付金额"), 0.0) for c in claims_subset)

    hosp_counter = Counter()
    hosp_amount = defaultdict(float)
    disease_counter = Counter()
    disease_amount = defaultdict(float)
    product_counter = Counter()
    product_amount = defaultdict(float)

    amounts = []
    for c in claims_subset:
        amt = _safe_float(c.get("赔付金额"), 0.0)
        amounts.append(amt)

        hosp = str(c.get("医院名称") or "").strip()
        if _is_valid_hospital(hosp):
            hosp_counter[hosp] += 1
            hosp_amount[hosp] += amt

        dis_label = _get_claim_disease_label(c)
        disease_counter[dis_label] += 1
        disease_amount[dis_label] += amt

        product = ""
        if claim_product_field:
            product = str(c.get(claim_product_field) or "").strip()
        if not product:
            pid = str(c.get("保单号") or "").strip()
            product = policy_to_product.get(pid, "")
        if product:
            product_counter[product] += 1
            product_amount[product] += amt

    amount_top3 = sorted(amounts, reverse=True)[:3]
    amount_conc = (sum(amount_top3) / total_amount) if total_amount else 0.0

    out = {
        "总案件数": total_claims,
        "总赔付金额": total_amount,
        "医院": _top3_distribution(hosp_counter, hosp_amount, total_claims, total_amount),
        "疾病": _top3_distribution(disease_counter, disease_amount, total_claims, total_amount),
        "金额": {
            "Top3赔案金额集中度": amount_conc,
            "Top3赔案金额": amount_top3,
        },
    }

    if product_counter:
        out["产品"] = _top3_distribution(product_counter, product_amount, total_claims, total_amount)
    return out


def _compute_visit_type_dist(claims: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """统计就诊类型分布（件数 + 金额）。
    - 先按案件号去重；
    - 就诊类型字段按逗号拆分，每个子类型独立计数；
    - 金额归入该案件涉及的每个子类型。
    """
    # 按案件号去重：保留第一条
    seen: Set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for c in claims:
        cid = str(c.get("案件号") or "").strip()
        if cid and cid in seen:
            continue
        if cid:
            seen.add(cid)
        deduped.append(c)

    counter: Counter = Counter()
    amount_map: Dict[str, float] = defaultdict(float)
    for c in deduped:
        raw_vt = str(c.get("就诊类型") or "").strip()
        parts = [p.strip() for p in raw_vt.split(",") if p.strip()] if raw_vt else []
        if not parts:
            parts = ["未知"]
        amt = _safe_float(c.get("赔付金额"), 0.0)
        for vt in parts:
            counter[vt] += 1
            amount_map[vt] += amt

    total_cnt = sum(counter.values())
    total_amt = sum(amount_map.values())
    return [
        {
            "就诊类型": vt,
            "案件数": cnt,
            "案件占比": (cnt / total_cnt) if total_cnt else 0.0,
            "赔付金额": amount_map[vt],
            "赔付占比": (amount_map[vt] / total_amt) if total_amt else 0.0,
        }
        for vt, cnt in counter.most_common()
    ]


def _compute_amount_dist(claims: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """按金额区间统计件数与赔付金额分布。"""
    cnt_map: Dict[str, int] = {label: 0 for _, _, label in AMOUNT_BUCKETS}
    amt_map: Dict[str, float] = {label: 0.0 for _, _, label in AMOUNT_BUCKETS}
    for c in claims:
        amt = _safe_float(c.get("赔付金额"), 0.0)
        for lo, hi, label in AMOUNT_BUCKETS:
            if lo <= amt < hi:
                cnt_map[label] += 1
                amt_map[label] += amt
                break
    total_cnt = sum(cnt_map.values())
    total_amt = sum(amt_map.values())
    return [
        {
            "区间": label,
            "案件数": cnt_map[label],
            "案件占比": (cnt_map[label] / total_cnt) if total_cnt else 0.0,
            "赔付金额": amt_map[label],
            "赔付占比": (amt_map[label] / total_amt) if total_amt else 0.0,
        }
        for _, _, label in AMOUNT_BUCKETS
    ]


def _render_top3_block(lines: List[str], title: str, stats: Dict[str, Any]) -> None:
    top3 = stats.get("top3") or []
    lines.append(f"- {title} Top3：")
    if not top3:
        lines.append("  - 无可用数据")
        return
    for row in top3:
        lines.append(
            f"  - {row.get('名称')}：{row.get('案件数', 0)}件（{fmt_score(row.get('案件占比', 0.0) * 100, 1)}%），"
            f"赔付{fmt_amount(row.get('赔付金额', 0.0))}（{fmt_score(row.get('赔付占比', 0.0) * 100, 1)}%）"
        )
    lines.append(
        f"  - Top3集中度：案件{fmt_score(stats.get('top3案件集中度', 0.0) * 100, 1)}%，"
        f"赔付{fmt_score(stats.get('top3赔付集中度', 0.0) * 100, 1)}%"
    )


def _stats_all_and_last_year(raw: Optional[Dict[str, Any]], report_date: datetime.date) -> Dict[str, Any]:
    if raw is None:
        return {}

    nodes = raw.get("保单节点列表") or []
    claims = raw.get("关联案件列表") or []
    edges = raw.get("传播边列表") or []

    one_year_ago = report_date.replace(year=report_date.year - 1)

    all_insured_ids: Set[str] = set()
    for n in nodes:
        insured_id = str(n.get("被保人ID") or "").strip()
        if insured_id:
            all_insured_ids.add(insured_id)

    level_stats_all = defaultdict(lambda: {"保单数": 0, "风险分": [], "案件数": 0, "赔付金额": 0.0})
    level_stats_1y = defaultdict(lambda: {"保单数": 0, "风险分": [], "案件数": 0, "赔付金额": 0.0})

    policy_to_level: Dict[str, int] = {}
    policy_to_insured: Dict[str, str] = {}
    for n in nodes:
        pid = str(n.get("保单号") or "").strip()
        lv = _safe_int(n.get("传播层级"), 0)
        risk = _safe_float(n.get("传播风险分"), 0.0)
        insured = str(n.get("被保人ID") or "").strip()
        if pid:
            policy_to_level[pid] = lv
            policy_to_insured[pid] = insured
        level_stats_all[lv]["保单数"] += 1
        level_stats_all[lv]["风险分"].append(risk)

    # 关联类型统计：按保单号去重 + 按关联ID值去重（辅助）
    relation_policy_set = defaultdict(set)
    relation_id_set = defaultdict(set)
    for e in edges:
        rel = str(e.get("关联类型") or "未知")
        p_src = str(e.get("源保单号") or "").strip()
        p_dst = str(e.get("目标保单号") or "").strip()
        rel_val = str(e.get("关联ID值") or "").strip()
        if p_src:
            relation_policy_set[rel].add(p_src)
        if p_dst:
            relation_policy_set[rel].add(p_dst)
        if rel_val:
            relation_id_set[rel].add(rel_val)

    relation_dist = {
        rel: {
            "涉及保单数(去重)": len(policies),
            "唯一关联ID数": len(relation_id_set.get(rel, set())),
        }
        for rel, policies in relation_policy_set.items()
    }

    claim_product_field = _detect_product_field(claims)
    node_product_field = _detect_product_field(nodes)
    policy_to_product: Dict[str, str] = {}
    if node_product_field:
        for n in nodes:
            pid = str(n.get("保单号") or "").strip()
            product = str(n.get(node_product_field) or "").strip()
            if pid and product:
                policy_to_product[pid] = product

    total_amount_all = 0.0
    total_amount_1y = 0.0
    case_cnt_1y = 0
    insured_1y: Set[str] = set()
    policies_1y: Set[str] = set()

    claims_all = []
    claims_1y = []
    claims_by_level_all = defaultdict(list)
    claims_by_level_1y = defaultdict(list)

    for c in claims:
        amount = _safe_float(c.get("赔付金额"), 0.0)
        total_amount_all += amount
        claims_all.append(c)

        pid = str(c.get("保单号") or "").strip()
        lv = policy_to_level.get(pid)
        if lv is not None:
            level_stats_all[lv]["案件数"] += 1
            level_stats_all[lv]["赔付金额"] += amount
            claims_by_level_all[lv].append(c)

        d = _to_date(c.get("报案日期"))
        if d and one_year_ago <= d <= report_date:
            case_cnt_1y += 1
            total_amount_1y += amount
            claims_1y.append(c)
            if pid:
                policies_1y.add(pid)
                lv2 = policy_to_level.get(pid)
                if lv2 is not None:
                    level_stats_1y[lv2]["案件数"] += 1
                    level_stats_1y[lv2]["赔付金额"] += amount
                    claims_by_level_1y[lv2].append(c)
                insured = policy_to_insured.get(pid, "")
                if insured:
                    insured_1y.add(insured)

    for n in nodes:
        pid = str(n.get("保单号") or "").strip()
        lv = _safe_int(n.get("传播层级"), 0)
        risk = _safe_float(n.get("传播风险分"), 0.0)
        if pid in policies_1y:
            level_stats_1y[lv]["保单数"] += 1
            level_stats_1y[lv]["风险分"].append(risk)

    def _pack_level(level_stat: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        out = {}
        for lv, s in sorted(level_stat.items(), key=lambda x: x[0]):
            avg = (sum(s["风险分"]) / len(s["风险分"])) if s["风险分"] else 0.0
            out[lv] = {
                "保单数": int(s["保单数"]),
                "案件数": int(s["案件数"]),
                "赔付金额": float(s["赔付金额"]),
                "平均风险分": avg,
            }
        return out

    concentration_all = _compute_concentration(claims_all, claim_product_field, policy_to_product)
    concentration_1y = _compute_concentration(claims_1y, claim_product_field, policy_to_product)
    concentration_level_all = {
        lv: _compute_concentration(lst, claim_product_field, policy_to_product)
        for lv, lst in claims_by_level_all.items()
    }
    concentration_level_1y = {
        lv: _compute_concentration(lst, claim_product_field, policy_to_product)
        for lv, lst in claims_by_level_1y.items()
    }

    visit_type_all = _compute_visit_type_dist(claims_all)
    visit_type_1y = _compute_visit_type_dist(claims_1y)
    amount_dist_all = _compute_amount_dist(claims_all)
    amount_dist_1y = _compute_amount_dist(claims_1y)

    return {
        "全部": {
            "保单数": len(nodes),
            "被保人数": len(all_insured_ids),
            "案件数": len(claims),
            "传播边数": len(edges),
            "赔付金额": total_amount_all,
            "分层统计": _pack_level(level_stats_all),
            "关联类型分布": relation_dist,
            "集中度": concentration_all,
            "层级集中度": concentration_level_all,
            "就诊类型分布": visit_type_all,
            "金额分布": amount_dist_all,
        },
        "近一年": {
            "时间窗口": f"{one_year_ago.isoformat()} 至 {report_date.isoformat()}",
            "保单数": len(policies_1y),
            "被保人数": len(insured_1y),
            "案件数": case_cnt_1y,
            "赔付金额": total_amount_1y,
            "分层统计": _pack_level(level_stats_1y),
            "集中度": concentration_1y,
            "层级集中度": concentration_level_1y,
            "就诊类型分布": visit_type_1y,
            "金额分布": amount_dist_1y,
        },
        "产品字段": claim_product_field or node_product_field,
    }


def render_section_3(raw: Optional[Dict[str, Any]], mo_scores: Dict[str, float], report_date: datetime.date) -> str:
    lines: List[str] = []
    lines.append("## 三、风险传播整体概述")
    lines.append("")

    if raw is None:
        lines.append("- 未提供原始输入 JSON，无法输出整体传播统计。")
        lines.append("")
        return "\n".join(lines)

    stats = _stats_all_and_last_year(raw, report_date)
    all_s = stats.get("全部", {})
    y1_s = stats.get("近一年", {})

    lines.append("### 3.1 全量传播概览")
    lines.append(f"- 总保单数：{all_s.get('保单数', 0)}")
    lines.append(f"- 涉及被保人数：{all_s.get('被保人数', 0)}")
    lines.append(f"- 涉及赔案数：{all_s.get('案件数', 0)}")
    lines.append(f"- 总传播边数：{all_s.get('传播边数', 0)}")
    lines.append(f"- 总赔付金额：{fmt_amount(all_s.get('赔付金额', 0.0))}")

    dist_high = sum(1 for v in mo_scores.values() if _safe_float(v) >= HIGH_SIM_THRESHOLD)
    dist_med = sum(1 for v in mo_scores.values() if MEDIUM_SIM_THRESHOLD <= _safe_float(v) < HIGH_SIM_THRESHOLD)
    dist_low = sum(1 for v in mo_scores.values() if _safe_float(v) < MEDIUM_SIM_THRESHOLD)
    lines.append(f"- MO相似度分布：高相似 {dist_high} 件，中相似 {dist_med} 件，低相似 {dist_low} 件")

    rel = all_s.get("关联类型分布", {})
    if rel:
        lines.append("- 全局关联类型分布（按保单号去重）：")
        for k, item in sorted(rel.items(), key=lambda x: x[1].get("涉及保单数(去重)", 0), reverse=True):
            lines.append(
                f"  - {k}：涉及保单{item.get('涉及保单数(去重)', 0)}张，唯一关联ID{item.get('唯一关联ID数', 0)}个"
            )

    vt_all = all_s.get("就诊类型分布") or []
    if vt_all:
        lines.append("- 就诊类型分布（全量）：")
        for row in vt_all:
            lines.append(
                f"  - {row.get('就诊类型')}：{row.get('案件数', 0)}件"
                f"（{fmt_score(row.get('案件占比', 0.0) * 100, 1)}%），"
                f"赔付{fmt_amount(row.get('赔付金额', 0.0))}"
                f"（{fmt_score(row.get('赔付占比', 0.0) * 100, 1)}%）"
            )

    amt_dist_all = all_s.get("金额分布") or []
    if amt_dist_all:
        lines.append("- 赔付金额分布（全量）：")
        for row in amt_dist_all:
            lines.append(
                f"  - {row.get('区间')}：{row.get('案件数', 0)}件"
                f"（{fmt_score(row.get('案件占比', 0.0) * 100, 1)}%），"
                f"赔付{fmt_amount(row.get('赔付金额', 0.0))}"
                f"（{fmt_score(row.get('赔付占比', 0.0) * 100, 1)}%）"
            )

    conc_all = all_s.get("集中度", {})
    if conc_all:
        lines.append("- 案件集中度（全量）：")
        _render_top3_block(lines, "医院（已过滤药房/药店）", conc_all.get("医院", {}))
        _render_top3_block(lines, "疾病（按输入粒度）", conc_all.get("疾病", {}))
        if conc_all.get("产品"):
            _render_top3_block(lines, "产品", conc_all.get("产品", {}))

    lines.append("- 分传播层级（全量）：")
    for lv, item in (all_s.get("分层统计") or {}).items():
        lines.append(
            f"  - 层级 {lv}：保单 {item.get('保单数', 0)}，案件 {item.get('案件数', 0)}，赔付 {fmt_amount(item.get('赔付金额', 0.0))}，平均风险分 {fmt_score(item.get('平均风险分'), 4)}"
        )
        lv_conc = (all_s.get("层级集中度") or {}).get(lv) or {}
        if lv_conc:
            hosp_conc = (lv_conc.get("医院") or {}).get("top3案件集中度", 0.0)
            dis_conc = (lv_conc.get("疾病") or {}).get("top3案件集中度", 0.0)
            amt_conc = (lv_conc.get("金额") or {}).get("Top3赔案金额集中度", 0.0)
            lines.append(
                f"    - 层级集中度：医院Top3 {fmt_score(hosp_conc * 100, 1)}%，疾病Top3 {fmt_score(dis_conc * 100, 1)}%，金额Top3 {fmt_score(amt_conc * 100, 1)}%"
            )
            if lv_conc.get("产品"):
                prod_conc = (lv_conc.get("产品") or {}).get("top3案件集中度", 0.0)
                lines.append(f"    - 层级产品Top3集中度：{fmt_score(prod_conc * 100, 1)}%")

    lines.append("")
    lines.append("### 3.2 近一年传播概览")
    lines.append(f"- 时间窗口：{y1_s.get('时间窗口', '')}")
    lines.append(f"- 近一年保单数：{y1_s.get('保单数', 0)}")
    lines.append(f"- 近一年涉及被保人数：{y1_s.get('被保人数', 0)}")
    lines.append(f"- 近一年赔案数：{y1_s.get('案件数', 0)}")
    lines.append(f"- 近一年赔付金额：{fmt_amount(y1_s.get('赔付金额', 0.0))}")

    vt_1y = y1_s.get("就诊类型分布") or []
    if vt_1y:
        lines.append("- 就诊类型分布（近一年）：")
        for row in vt_1y:
            lines.append(
                f"  - {row.get('就诊类型')}：{row.get('案件数', 0)}件"
                f"（{fmt_score(row.get('案件占比', 0.0) * 100, 1)}%），"
                f"赔付{fmt_amount(row.get('赔付金额', 0.0))}"
                f"（{fmt_score(row.get('赔付占比', 0.0) * 100, 1)}%）"
            )

    amt_dist_1y = y1_s.get("金额分布") or []
    if amt_dist_1y:
        lines.append("- 赔付金额分布（近一年）：")
        for row in amt_dist_1y:
            lines.append(
                f"  - {row.get('区间')}：{row.get('案件数', 0)}件"
                f"（{fmt_score(row.get('案件占比', 0.0) * 100, 1)}%），"
                f"赔付{fmt_amount(row.get('赔付金额', 0.0))}"
                f"（{fmt_score(row.get('赔付占比', 0.0) * 100, 1)}%）"
            )

    conc_1y = y1_s.get("集中度", {})
    if conc_1y:
        lines.append("- 案件集中度（近一年）：")
        _render_top3_block(lines, "医院（已过滤药房/药店）", conc_1y.get("医院", {}))
        _render_top3_block(lines, "疾病（按输入粒度）", conc_1y.get("疾病", {}))
        if conc_1y.get("产品"):
            _render_top3_block(lines, "产品", conc_1y.get("产品", {}))

    lines.append("- 分传播层级（近一年）：")
    level_1y = y1_s.get("分层统计") or {}
    if not level_1y:
        lines.append("  - 近一年无可用层级数据")
    else:
        for lv, item in level_1y.items():
            lines.append(
                f"  - 层级 {lv}：保单 {item.get('保单数', 0)}，案件 {item.get('案件数', 0)}，赔付 {fmt_amount(item.get('赔付金额', 0.0))}，平均风险分 {fmt_score(item.get('平均风险分'), 4)}"
            )
            lv_conc_1y = (y1_s.get("层级集中度") or {}).get(lv) or {}
            if lv_conc_1y:
                hosp_conc = (lv_conc_1y.get("医院") or {}).get("top3案件集中度", 0.0)
                dis_conc = (lv_conc_1y.get("疾病") or {}).get("top3案件集中度", 0.0)
                amt_conc = (lv_conc_1y.get("金额") or {}).get("Top3赔案金额集中度", 0.0)
                lines.append(
                    f"    - 层级集中度：医院Top3 {fmt_score(hosp_conc * 100, 1)}%，疾病Top3 {fmt_score(dis_conc * 100, 1)}%，金额Top3 {fmt_score(amt_conc * 100, 1)}%"
                )
                if lv_conc_1y.get("产品"):
                    prod_conc = (lv_conc_1y.get("产品") or {}).get("top3案件集中度", 0.0)
                    lines.append(f"    - 层级产品Top3集中度：{fmt_score(prod_conc * 100, 1)}%")

    lines.append("")
    return "\n".join(lines)


def render_section_4_placeholder() -> str:
    """
    生成 Section 4 占位符。
    由 Claude（Stage 3）完成解读后，将 <!--INTERPRETATION_PLACEHOLDER-->
    替换为实际的智能解读内容。
    """
    lines: List[str] = []
    lines.append("## 四、智能解读结论")
    lines.append("")
    lines.append("> 本节由 LLM（Claude Stage 3）自动填充，包含社群关联机制解读、跨社群共性信号及 P0/P1/P2 调查建议。")
    lines.append("> 以下内容待 Stage 3 解读后写入，禁止手动修改占位符标记。")
    lines.append("")
    lines.append("<!--INTERPRETATION_PLACEHOLDER-->")
    lines.append("")
    return "\n".join(lines)


def main():
    args = parse_args()
    with open(args.input, "r", encoding="utf-8") as f:
        j = json.load(f)

    clusters = j.get("clusters", []) or []
    leiden = j.get("leiden_社群", []) or []
    mo_scores = j.get("mo_scores", {}) or {}

    raw = None
    if args.raw_input:
        with open(args.raw_input, "r", encoding="utf-8") as f:
            raw = json.load(f)

    report_dt = datetime.now().date()
    seed_case_id = _find_seed_case_id(raw)
    _claim_by_id, claim_by_policy = _build_claim_maps(raw)

    # 社群筛选规则：至少1件中相似（MO>=0.4）
    similar_communities: List[Dict[str, Any]] = []
    suspicious_by_comm: Dict[str, List[Dict[str, Any]]] = {}

    for comm in leiden:
        comm_id = str(comm.get("社群编号") or "")
        suspicious = _extract_similar_claims_for_community(comm, claim_by_policy, mo_scores, seed_case_id)
        if len(suspicious) >= 1:
            similar_communities.append(comm)
            suspicious_by_comm[comm_id] = suspicious

    similar_claim_total = len({
        c["案件号"]
        for lst in suspicious_by_comm.values()
        for c in lst
    })

    lines: List[str] = []
    lines.append("# 欺诈关联分析报告（v3）")
    lines.append(f"生成时间：{datetime.now().isoformat()}")
    if seed_case_id:
        lines.append(f"种子案件：{seed_case_id}")
    lines.append("")

    lines.append(render_section_1(clusters, similar_communities, seed_case_id, similar_claim_total))
    lines.append(render_section_2(similar_communities, suspicious_by_comm, args.viz_dir))
    lines.append(render_section_3(raw, mo_scores, report_dt))
    lines.append(render_section_4_placeholder())

    with open(args.report, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")

    print("Wrote report to", args.report)


if __name__ == '__main__':
    main()
