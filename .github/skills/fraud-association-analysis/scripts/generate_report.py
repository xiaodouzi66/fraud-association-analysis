#!/usr/bin/env python3
"""
根据 cluster_analysis v2 输出生成三段式中文报告。

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
from typing import Any, Dict, List, Optional, Tuple


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


def risk_level_cn(score: Optional[float]) -> str:
    if score is None:
        return "未知"
    if score >= 0.7:
        return "🔴 高风险"
    if score >= 0.4:
        return "🟡 中风险"
    return "🟢 低风险"


def confidence_level(score: float) -> str:
    if score >= 0.8:
        return "高"
    if score >= 0.5:
        return "中"
    return "低"


def fmt_score(x: Any, ndigits: int = 4) -> str:
    if x is None:
        return "N/A"
    try:
        return f"{float(x):.{ndigits}f}"
    except Exception:
        return "N/A"


def fmt_amount(x: Any) -> str:
    val = _safe_float(x, 0.0)
    if val >= 10000:
        return f"{val / 10000:.2f}万元"
    return f"{val:.2f}元"


def _dedup_ordered(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item and item not in seen:
            out.append(item)
            seen.add(item)
    return out


def _risk_conclusion_text(score: float, has_blacklist: bool) -> str:
    if has_blacklist or score >= 0.7:
        return "存在较强团伙欺诈信号，建议立即人工复核并补充证据链。"
    if score >= 0.4:
        return "存在关联风险信号，建议进入重点核查并持续追踪。"
    return "暂未识别显著团伙特征，建议常规监控。"


def _collect_summary_flags(clusters: List[Dict[str, Any]], leiden: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    has_blacklist = False
    reasons: List[str] = []
    for c in clusters:
        reasons.extend([str(x) for x in (c.get("confidence_reasons") or []) if x])
        if "黑名单" in " ".join([str(x) for x in c.get("confidence_reasons") or []]):
            has_blacklist = True
    for s in leiden:
        reasons.extend([str(x) for x in (s.get("置信度原因") or []) if x])
        for high_case in s.get("高相似案件列表") or []:
            if bool(high_case.get("是否黑名单", False)):
                has_blacklist = True
    return has_blacklist, _dedup_ordered(reasons)


def render_section_1(
    clusters: List[Dict[str, Any]],
    leiden: List[Dict[str, Any]],
    mo_scores: Dict[str, float],
    seed_case_id: str,
) -> str:
    max_cluster = max((_safe_float(c.get("gang_score"), 0.0) for c in clusters), default=0.0)
    max_leiden = max((_safe_float(s.get("社群风险分"), 0.0) for s in leiden), default=0.0)
    top_score = max(max_cluster, max_leiden)
    has_blacklist, reasons = _collect_summary_flags(clusters, leiden)

    mo_vals = [float(v) for v in mo_scores.values()] if mo_scores else []
    mo_avg = sum(mo_vals) / len(mo_vals) if mo_vals else 0.0

    lines: List[str] = []
    lines.append("## 一、综合结论")
    lines.append("")
    lines.append(f"- 种子案件：{seed_case_id or '未知'}")
    lines.append(f"- 风险等级：{risk_level_cn(top_score)}")
    lines.append(f"- 结论：{_risk_conclusion_text(top_score, has_blacklist)}")
    lines.append(f"- 置信度：{confidence_level(top_score)}")
    lines.append(f"- 最高团伙分（连通分量）：{fmt_score(max_cluster)}")
    lines.append(f"- 最高社群分（Leiden）：{fmt_score(max_leiden)}")
    lines.append(f"- 关联案件 MO 平均相似度：{fmt_score(mo_avg)}")

    if reasons:
        lines.append("- 关键依据：")
        for r in reasons[:6]:
            lines.append(f"  - {r}")
    else:
        lines.append("- 关键依据：暂无显著规则命中")
    lines.append("")
    return "\n".join(lines)


def render_cluster_block(c: Dict[str, Any], idx: int) -> str:
    lines: List[str] = []
    score = _safe_float(c.get("gang_score"), 0.0)
    metrics = c.get("metrics") or {}
    members = c.get("members") or []
    reasons = c.get("confidence_reasons") or []

    lines.append(f"### 2.1.{idx} 关联簇 {c.get('cluster_id', '未知')}")
    lines.append("")
    lines.append(f"- 风险等级：{risk_level_cn(score)}（gang_score={fmt_score(score)}）")
    lines.append(f"- 来源：{c.get('source', 'unknown')}，推荐动作：{c.get('recommended_action', 'monitor')}")
    lines.append(f"- 规模：{_safe_int(metrics.get('N'), 0)} 节点，边密度：{fmt_score(metrics.get('density'), 3)}")
    lines.append(f"- 高风险占比 H：{fmt_score(metrics.get('H'), 3)}，突发性 B：{fmt_score(metrics.get('B'), 3)}")
    lines.append(f"- 疾病集中度 C_dc：{fmt_score(metrics.get('C_dc'), 3)}，重复索赔率 RCR：{fmt_score(metrics.get('RCR'), 3)}")
    lines.append(f"- MO均值（保单聚合）：{fmt_score(c.get('mo_similarity_avg'), 4)}")
    lines.append(f"- 赔付金额总计：{fmt_amount(c.get('claim_amount_total'))}")

    rel_dist = c.get("relation_distribution") or {}
    if rel_dist:
        rel_text = "，".join([f"{k}:{v}" for k, v in sorted(rel_dist.items(), key=lambda x: x[1], reverse=True)])
        lines.append(f"- 关联类型分布：{rel_text}")

    lines.append("- 成员保单（最多展示10个）：")
    for m in members[:10]:
        lines.append(f"  - {m}")

    if reasons:
        lines.append("- 置信理由：")
        for r in reasons[:5]:
            lines.append(f"  - {r}")
    lines.append("")
    return "\n".join(lines)


def _viz_path_for_comm(viz_dir: Optional[str], comm_id: str) -> Optional[str]:
    if not viz_dir:
        return None
    filename = f"community_{comm_id}.png"
    path = os.path.join(viz_dir, filename)
    return path if os.path.exists(path) else None


def render_leiden_block(s: Dict[str, Any], idx: int, viz_dir: Optional[str]) -> str:
    lines: List[str] = []
    comm_id = str(s.get("社群编号") or f"comm_{idx}")

    lines.append(f"### 2.2.{idx} 子社群 {comm_id}")
    lines.append("")
    lines.append(f"- 风险等级：{risk_level_cn(_safe_float(s.get('社群风险分'), 0.0))}（社群风险分={fmt_score(s.get('社群风险分'))}）")
    lines.append(f"- 是否包含种子节点：{bool(s.get('包含种子节点', False))}")
    lines.append(f"- 社群规模：{_safe_int(s.get('规模'), 0)} 张保单，{_safe_int(s.get('边数'), 0)} 条关联边")
    lines.append(f"- 涉及案件数：{_safe_int(s.get('涉及案件数'), 0)}，涉及赔付金额：{fmt_amount(s.get('涉及赔付金额'))}")
    lines.append(f"- 传播路径描述：{s.get('传播路径描述', '暂无')} ")

    ext = s.get("扩展指标") or {}
    lines.append(
        "- 扩展指标："
        f"mo_similarity_avg={fmt_score(ext.get('mo_similarity_avg'))}，"
        f"mo_similarity_high={fmt_score(ext.get('mo_similarity_high'))}，"
        f"hospital_conc={fmt_score(ext.get('hospital_conc'))}，"
        f"amount_cluster={fmt_score(ext.get('amount_cluster'))}"
    )

    rel_dist = s.get("关联类型分布") or {}
    if rel_dist:
        rel_text = "，".join([f"{k}:{v}" for k, v in sorted(rel_dist.items(), key=lambda x: x[1], reverse=True)])
        lines.append(f"- 关联类型分布：{rel_text}")

    bridges = s.get("桥接节点列表") or []
    if bridges:
        lines.append("- 桥接节点（Top5）：")
        for b in bridges[:5]:
            lines.append(
                f"  - {b.get('保单号')}｜层级{b.get('传播层级')}｜"
                f"风险{fmt_score(b.get('传播风险分'), 3)}｜中介中心性{fmt_score(b.get('中介中心性'), 4)}"
            )

    high_claims = s.get("高相似案件列表") or []
    if high_claims:
        lines.append("- 高相似案件（最多10条）：")
        lines.append("  - 案件号 | 保单号 | 医院 | 疾病 | 住院天数 | 赔付金额 | MO")
        for c in high_claims[:10]:
            dis = c.get("疾病名称") or []
            dis_txt = "、".join([str(x) for x in dis]) if isinstance(dis, list) else str(dis)
            lines.append(
                f"  - {c.get('案件号')} | {c.get('保单号')} | {c.get('医院名称')} | "
                f"{dis_txt} | {c.get('住院天数')} | {fmt_amount(c.get('赔付金额'))} | {fmt_score(c.get('mo_score'))}"
            )

    reasons = s.get("置信度原因") or []
    if reasons:
        lines.append("- 置信理由：")
        for r in reasons[:5]:
            lines.append(f"  - {r}")

    viz = _viz_path_for_comm(viz_dir, comm_id)
    if viz:
        lines.append(f"- 社群图：{viz}")

    lines.append("")
    return "\n".join(lines)


def render_section_2(
    clusters: List[Dict[str, Any]],
    leiden: List[Dict[str, Any]],
    viz_dir: Optional[str],
) -> str:
    lines: List[str] = []
    lines.append("## 二、风险详情")
    lines.append("")

    if clusters:
        for idx, c in enumerate(sorted(clusters, key=lambda x: _safe_float(x.get("gang_score"), 0.0), reverse=True), start=1):
            lines.append(render_cluster_block(c, idx))
    else:
        lines.append("- 未识别到连通分量风险簇。")
        lines.append("")

    if leiden:
        for idx, s in enumerate(sorted(leiden, key=lambda x: _safe_float(x.get("社群风险分"), 0.0), reverse=True), start=1):
            lines.append(render_leiden_block(s, idx, viz_dir))
    else:
        lines.append("### 2.2 Leiden 子社群细分")
        lines.append("")
        lines.append("- 未触发或未输出 Leiden 子社群结果。")
        lines.append("")

    return "\n".join(lines)


def _global_stats_from_raw(raw: Dict[str, Any]) -> Dict[str, Any]:
    nodes = raw.get("保单节点列表") or []
    claims = raw.get("关联案件列表") or []
    edges = raw.get("传播边列表") or []

    level_counter = Counter()
    level_score = defaultdict(list)
    relation_counter = Counter()
    total_amount = 0.0

    for n in nodes:
        lv = _safe_int(n.get("传播层级"), 0)
        level_counter[lv] += 1
        level_score[lv].append(_safe_float(n.get("传播风险分"), 0.0))

    for e in edges:
        relation_counter[str(e.get("关联类型") or "未知")] += 1

    for c in claims:
        total_amount += _safe_float(c.get("赔付金额"), 0.0)

    return {
        "保单总数": len(nodes),
        "案件总数": len(claims),
        "传播边总数": len(edges),
        "赔付总金额": total_amount,
        "分层统计": {
            lv: {
                "保单数": cnt,
                "平均风险分": (sum(level_score[lv]) / len(level_score[lv])) if level_score[lv] else 0.0,
            }
            for lv, cnt in sorted(level_counter.items(), key=lambda x: x[0])
        },
        "关联类型分布": dict(relation_counter),
    }


def _mo_distribution(mo_scores: Dict[str, float]) -> Dict[str, int]:
    high = medium = low = 0
    for _, v in mo_scores.items():
        x = _safe_float(v, 0.0)
        if x >= 0.6:
            high += 1
        elif x >= 0.4:
            medium += 1
        else:
            low += 1
    return {"高相似(>=0.6)": high, "中相似([0.4,0.6))": medium, "低相似(<0.4)": low}


def render_section_3(
    raw: Optional[Dict[str, Any]],
    clusters: List[Dict[str, Any]],
    mo_scores: Dict[str, float],
) -> str:
    lines: List[str] = []
    lines.append("## 三、传播全局概述")
    lines.append("")

    if raw is not None:
        stats = _global_stats_from_raw(raw)
        lines.append(f"- 总保单数：{stats['保单总数']}")
        lines.append(f"- 总案件数：{stats['案件总数']}")
        lines.append(f"- 总传播边数：{stats['传播边总数']}")
        lines.append(f"- 总赔付金额：{fmt_amount(stats['赔付总金额'])}")

        lines.append("- 分层统计：")
        for lv, item in (stats.get("分层统计") or {}).items():
            lines.append(
                f"  - 层级 {lv}：保单 {item.get('保单数', 0)}，平均风险分 {fmt_score(item.get('平均风险分'), 4)}"
            )

        rel_dist = stats.get("关联类型分布") or {}
        if rel_dist:
            rel_text = "，".join([f"{k}:{v}" for k, v in sorted(rel_dist.items(), key=lambda x: x[1], reverse=True)])
            lines.append(f"- 全局关联类型分布：{rel_text}")
    else:
        lines.append("- 未提供原始输入 JSON，以下为基于 cluster 输出的简化概述。")
        member_union = set()
        for c in clusters:
            for m in c.get("members") or []:
                member_union.add(str(m))
        lines.append(f"- 参与保单（去重）：{len(member_union)}")
        lines.append(f"- 识别风险簇数量：{len(clusters)}")

    dist = _mo_distribution(mo_scores)
    lines.append(
        f"- MO 相似度分布：高相似 {dist['高相似(>=0.6)']} 件，"
        f"中相似 {dist['中相似([0.4,0.6))']} 件，低相似 {dist['低相似(<0.4)']} 件"
    )

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

    seed_case_id = ""
    if raw is not None:
        seed_case_id = str((raw.get("种子案件") or {}).get("案件号") or "")

    lines: List[str] = []
    lines.append("# 欺诈关联分析报告（v2）")
    lines.append(f"生成时间：{datetime.utcnow().isoformat()}")
    if seed_case_id:
        lines.append(f"种子案件：{seed_case_id}")
    lines.append("")

    lines.append(render_section_1(clusters, leiden, mo_scores, seed_case_id))
    lines.append(render_section_2(clusters, leiden, args.viz_dir))
    lines.append(render_section_3(raw, clusters, mo_scores))

    with open(args.report, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")

    print("Wrote report to", args.report)

if __name__ == '__main__':
    main()
