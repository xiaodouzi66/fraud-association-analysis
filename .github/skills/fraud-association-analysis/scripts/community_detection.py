#!/usr/bin/env python3
"""
社区细分分析（Leiden / fallback）模块。

目标：
1) 基于保单粒度图进行社群划分
2) 聚焦种子保单所在子社群
3) 在案件粒度汇总 MO 相似性与聚集性指标
4) 输出结构化结果，供 cluster_analysis.py 和报告模块调用
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx


DEFAULT_MIN_EDGE_WEIGHT = 0.05
DEFAULT_LEIDEN_RESOLUTION = 1.0
DEFAULT_TOP_BRIDGE_K = 5
HIGH_SIMILARITY_THRESHOLD = 0.6

# 扩展指标权重（已按用户要求提升 mo_similarity_avg 权重）
EXTENDED_WEIGHTS = {
    "mo_similarity_avg": 0.20,
    "mo_similarity_high": 0.05,
    "hospital_conc": 0.08,
    "amount_cluster": 0.07,
}
BASE_SCALE = 0.60  # 基础8项指标整体缩放

# 与 cluster_analysis.py 中一致的基础指标权重
BASE_WEIGHTS = {
    "N": 0.12,
    "D": 0.12,
    "S_avg": 0.18,
    "H": 0.12,
    "B": 0.14,
    "M": 0.08,
    "RCR": 0.10,
    "C_dc": 0.14,
}


@dataclass
class SubCommunityResult:
    社群编号: str
    包含种子节点: bool
    规模: int
    边数: int
    涉及案件数: int
    涉及赔付金额: float
    基础指标: Dict[str, float]
    扩展指标: Dict[str, float]
    社群风险分: float
    桥接节点列表: List[Dict[str, Any]]
    关联类型分布: Dict[str, int]
    传播路径描述: str
    高相似案件列表: List[Dict[str, Any]]
    置信度原因: List[str]


def normalize_metric(x: float, p10: float, p90: float) -> float:
    if p90 - p10 <= 0:
        return 0.0
    return max(0.0, min(1.0, (x - p10) / (p90 - p10)))


def gini_concentration(items: List[str]) -> float:
    if not items:
        return 0.0
    total = len(items)
    freq = Counter(items)
    k = len(freq)
    if k == 1:
        return 1.0
    entropy = -sum((v / total) * math.log2(v / total) for v in freq.values())
    max_entropy = math.log2(k)
    return max(0.0, min(1.0, 1.0 - (entropy / max_entropy))) if max_entropy > 0 else 0.0


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


def build_policy_graph(
    节点列表: List[Dict[str, Any]],
    边列表: List[Dict[str, Any]],
    min_edge_weight: float = DEFAULT_MIN_EDGE_WEIGHT,
) -> nx.Graph:
    G = nx.Graph()
    for node in 节点列表:
        pid = str(node.get("保单号") or "").strip()
        if not pid:
            continue
        G.add_node(
            pid,
            保单号=pid,
            传播层级=int(node.get("传播层级", 0) or 0),
            传播风险分=float(node.get("传播风险分", 0.0) or 0.0),
            是否种子=bool(node.get("是否种子", False)),
            被保人ID=node.get("被保人ID"),
            投保日期=node.get("投保日期"),
            claims=[],
        )

    for edge in 边列表:
        src = str(edge.get("源保单号") or "").strip()
        dst = str(edge.get("目标保单号") or "").strip()
        if not src or not dst or src == dst:
            continue
        if src not in G or dst not in G:
            continue
        w = float(edge.get("边权重", 0.0) or 0.0)
        if w < min_edge_weight:
            continue
        G.add_edge(
            src,
            dst,
            weight=w,
            关联类型=str(edge.get("关联类型") or "未知"),
            关联ID值=str(edge.get("关联ID值") or "未知"),
        )
    return G


def attach_claims_to_graph(
    G: nx.Graph,
    案件列表: List[Dict[str, Any]],
    mo_scores: Dict[str, float],
) -> None:
    # 支持 mo_scores key=案件号（新版）或 key=保单号（兼容）
    policy_level_scores: Dict[str, List[float]] = defaultdict(list)

    for claim in 案件列表:
        policy_id = str(claim.get("保单号") or "").strip()
        if not policy_id or policy_id not in G:
            continue
        claim_id = str(claim.get("案件号") or "").strip()

        if claim_id and claim_id in mo_scores:
            mo = float(mo_scores[claim_id])
        else:
            mo = float(mo_scores.get(policy_id, 0.0))

        enriched = dict(claim)
        enriched["mo_score"] = mo
        G.nodes[policy_id]["claims"].append(enriched)
        policy_level_scores[policy_id].append(mo)

    for policy_id in G.nodes():
        scores = policy_level_scores.get(policy_id, [])
        G.nodes[policy_id]["节点MO均值"] = sum(scores) / len(scores) if scores else 0.0


def should_run_leiden(G: nx.Graph, force: bool = False) -> bool:
    if force:
        return True
    n = G.number_of_nodes()
    if n == 0:
        return False
    avg_risk = sum(float(G.nodes[x].get("传播风险分", 0.0)) for x in G.nodes()) / n
    if n >= 500:
        return True
    if n >= 200 and avg_risk < 0.6:
        return True
    return False


def run_leiden(
    G: nx.Graph,
    resolution: float = DEFAULT_LEIDEN_RESOLUTION,
    weight_attr: str = "weight",
) -> Tuple[Dict[str, str], str]:
    if G.number_of_nodes() == 0:
        return {}, "empty"

    try:
        import igraph as ig  # type: ignore
        import leidenalg  # type: ignore

        nodes = list(G.nodes())
        index = {node: i for i, node in enumerate(nodes)}
        edges = [(index[u], index[v]) for u, v in G.edges()]
        weights = [float(G[u][v].get(weight_attr, 1.0)) for u, v in G.edges()]

        g = ig.Graph(n=len(nodes), edges=edges, directed=False)
        if weights:
            g.es["weight"] = weights

        part = leidenalg.find_partition(
            g,
            leidenalg.RBConfigurationVertexPartition,
            weights=g.es["weight"] if weights else None,
            resolution_parameter=resolution,
        )

        node_to_comm: Dict[str, str] = {}
        for comm_idx, members in enumerate(part):
            cid = f"comm_{comm_idx}"
            for idx in members:
                node_to_comm[nodes[idx]] = cid
        return node_to_comm, "leidenalg"

    except Exception:
        comms = nx.community.greedy_modularity_communities(G, weight=weight_attr)
        node_to_comm = {}
        for comm_idx, members in enumerate(comms):
            cid = f"comm_{comm_idx}"
            for node in members:
                node_to_comm[node] = cid
        return node_to_comm, "louvain-fallback"


def find_seed_communities(node_to_comm: Dict[str, str], seed_policy_ids: Set[str]) -> Set[str]:
    cids: Set[str] = set()
    for sid in seed_policy_ids:
        if sid in node_to_comm:
            cids.add(node_to_comm[sid])
    return cids


def component_metrics_from_claims(sub: nx.Graph) -> Dict[str, float]:
    N = sub.number_of_nodes()
    E = sub.number_of_edges()
    density = (2 * E) / (N * (N - 1)) if N > 1 else 0.0

    # 传播风险分作为节点风险代理
    scores = [float(sub.nodes[n].get("传播风险分", 0.0)) for n in sub.nodes()]
    S_avg = sum(scores) / len(scores) if scores else 0.0
    H = sum(1 for s in scores if s >= 0.6) / N if N > 0 else 0.0

    # burst 近似：按报案日期聚合高风险案件
    claim_dates = []
    for n in sub.nodes():
        for c in sub.nodes[n].get("claims", []):
            d = str(c.get("报案日期") or "")
            if d and float(c.get("mo_score", 0.0)) >= HIGH_SIMILARITY_THRESHOLD:
                claim_dates.append(d)
    claim_dates.sort()
    max_cnt = 0
    for i in range(len(claim_dates)):
        j = i
        while j < len(claim_dates) and claim_dates[j] == claim_dates[i]:
            j += 1
        max_cnt = max(max_cnt, j - i)
    B = min(max_cnt / 4.0, 1.0)

    diag_items = []
    for n in sub.nodes():
        for c in sub.nodes[n].get("claims", []):
            for d in (c.get("疾病名称") or []):
                diag_items.append(str(d))
    C_dc = gini_concentration(diag_items)

    # RCR：按医院+报案日期近似重复率
    pair_count = Counter()
    for n in sub.nodes():
        for c in sub.nodes[n].get("claims", []):
            key = (str(c.get("医院名称") or "未知"), str(c.get("报案日期") or "未知"))
            pair_count[key] += 1
    max_repeat = max(pair_count.values()) if pair_count else 1
    RCR = min((max_repeat - 1) / 4.0, 1.0) if max_repeat > 1 else 0.0

    triangles = sum(nx.triangles(sub).values()) / 3 if N > 0 else 0
    M = min(triangles, 5) / 5.0

    return {
        "N": float(N),
        "E": float(E),
        "density": density,
        "S_avg": S_avg,
        "H": H,
        "B": B,
        "C_dc": C_dc,
        "RCR": RCR,
        "M": M,
    }


def compute_extended_metrics(sub: nx.Graph) -> Dict[str, float]:
    mo_values: List[float] = []
    hospitals: List[str] = []
    amount_labels: List[str] = []

    for n in sub.nodes():
        for claim in sub.nodes[n].get("claims", []):
            mo = float(claim.get("mo_score", 0.0) or 0.0)
            mo_values.append(mo)

            hosp = str(claim.get("医院名称") or "").strip()
            if hosp:
                hospitals.append(hosp)

            amount_labels.append(map_amount_to_label(claim.get("赔付金额")))

    if mo_values:
        mo_similarity_avg = sum(mo_values) / len(mo_values)
        mo_similarity_high = sum(1 for x in mo_values if x >= HIGH_SIMILARITY_THRESHOLD) / len(mo_values)
    else:
        mo_similarity_avg = 0.0
        mo_similarity_high = 0.0

    return {
        "mo_similarity_avg": mo_similarity_avg,
        "mo_similarity_high": mo_similarity_high,
        "hospital_conc": gini_concentration(hospitals),
        "amount_cluster": gini_concentration(amount_labels),
    }


def relation_type_distribution(sub: nx.Graph) -> Dict[str, int]:
    c = Counter()
    for _, _, d in sub.edges(data=True):
        c[str(d.get("关联类型") or "未知")] += 1
    return dict(c)


def compute_bridge_nodes(sub: nx.Graph, top_k: int = DEFAULT_TOP_BRIDGE_K) -> List[Dict[str, Any]]:
    if sub.number_of_nodes() == 0:
        return []
    bc = nx.betweenness_centrality(sub, weight="weight", normalized=True)
    ranked = sorted(bc.items(), key=lambda x: x[1], reverse=True)[:top_k]

    result: List[Dict[str, Any]] = []
    for node, score in ranked:
        rel_types = set()
        rel_ids = set()
        for nb in sub.neighbors(node):
            edge = sub[node][nb]
            rel_types.add(str(edge.get("关联类型") or "未知"))
            rel_ids.add(str(edge.get("关联ID值") or "未知"))

        result.append(
            {
                "保单号": node,
                "传播层级": int(sub.nodes[node].get("传播层级", 0) or 0),
                "传播风险分": round(float(sub.nodes[node].get("传播风险分", 0.0) or 0.0), 4),
                "中介中心性": round(float(score), 6),
                "关联类型": sorted(rel_types),
                "关联ID值": sorted(rel_ids),
            }
        )
    return result


def describe_propagation(sub: nx.Graph, bridge_nodes: List[Dict[str, Any]], rel_dist: Dict[str, int]) -> str:
    n = sub.number_of_nodes()
    e = sub.number_of_edges()
    if not rel_dist:
        return f"该子社群包含{n}张保单、{e}条关联边，未识别出显著关联类型。"

    top_rel = sorted(rel_dist.items(), key=lambda x: x[1], reverse=True)
    head = "、".join([f"{k}({v}条)" for k, v in top_rel[:2]])

    if bridge_nodes:
        b = bridge_nodes[0]
        bridge_desc = f"核心桥接保单为{b['保单号']}（中介中心性{b['中介中心性']:.4f}）"
    else:
        bridge_desc = "未识别明显桥接保单"

    return f"该子社群包含{n}张保单、{e}条关联边，主要关联类型为{head}；{bridge_desc}。"


def compute_community_risk_score(base_metrics: Dict[str, float], ext_metrics: Dict[str, float]) -> Tuple[float, List[str]]:
    # 基础指标标准化（与 cluster_analysis 同口径）
    Nn = normalize_metric(base_metrics.get("N", 0.0), 1, 20)
    Dn = normalize_metric(base_metrics.get("density", 0.0), 0.0, 0.5)

    base_score = (
        BASE_WEIGHTS["N"] * Nn
        + BASE_WEIGHTS["D"] * Dn
        + BASE_WEIGHTS["S_avg"] * float(base_metrics.get("S_avg", 0.0))
        + BASE_WEIGHTS["H"] * float(base_metrics.get("H", 0.0))
        + BASE_WEIGHTS["B"] * float(base_metrics.get("B", 0.0))
        + BASE_WEIGHTS["M"] * float(base_metrics.get("M", 0.0))
        + BASE_WEIGHTS["RCR"] * float(base_metrics.get("RCR", 0.0))
        + BASE_WEIGHTS["C_dc"] * float(base_metrics.get("C_dc", 0.0))
    )

    ext_score = (
        EXTENDED_WEIGHTS["mo_similarity_avg"] * float(ext_metrics.get("mo_similarity_avg", 0.0))
        + EXTENDED_WEIGHTS["mo_similarity_high"] * float(ext_metrics.get("mo_similarity_high", 0.0))
        + EXTENDED_WEIGHTS["hospital_conc"] * float(ext_metrics.get("hospital_conc", 0.0))
        + EXTENDED_WEIGHTS["amount_cluster"] * float(ext_metrics.get("amount_cluster", 0.0))
    )

    score = BASE_SCALE * base_score + ext_score
    score = max(0.0, min(1.0, score))

    reasons: List[str] = []
    if ext_metrics.get("mo_similarity_avg", 0.0) >= 0.6:
        reasons.append("社群案件与种子案件作案手法平均相似度较高")
    if ext_metrics.get("hospital_conc", 0.0) >= 0.6:
        reasons.append("社群内医院集中度较高")
    if ext_metrics.get("amount_cluster", 0.0) >= 0.6:
        reasons.append("社群内赔付金额区间聚集明显")
    if base_metrics.get("H", 0.0) >= 0.3:
        reasons.append("高风险保单占比较高")

    return round(score, 6), reasons


def _claims_in_subgraph(sub: nx.Graph) -> List[Dict[str, Any]]:
    claims: List[Dict[str, Any]] = []
    for n in sub.nodes():
        claims.extend(sub.nodes[n].get("claims", []))
    return claims


def analyze_seed_communities(
    data: Dict[str, Any],
    mo_scores: Dict[str, float],
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[List[SubCommunityResult], str]:
    cfg = config or {}
    resolution = float(cfg.get("resolution", DEFAULT_LEIDEN_RESOLUTION))
    min_edge_weight = float(cfg.get("min_edge_weight", DEFAULT_MIN_EDGE_WEIGHT))
    top_k = int(cfg.get("top_bridge_k", DEFAULT_TOP_BRIDGE_K))
    force = bool(cfg.get("force_leiden", False))

    nodes = data.get("保单节点列表") or []
    edges = data.get("传播边列表") or []
    claims = data.get("关联案件列表") or []

    G = build_policy_graph(nodes, edges, min_edge_weight=min_edge_weight)
    attach_claims_to_graph(G, claims, mo_scores)

    if not should_run_leiden(G, force=force):
        return [], "skipped"

    node_to_comm, algo = run_leiden(G, resolution=resolution)

    seed_policy_ids = {
        str(n.get("保单号"))
        for n in nodes
        if bool(n.get("是否种子", False)) and str(n.get("保单号") or "").strip()
    }
    if not seed_policy_ids and data.get("种子案件", {}).get("保单号"):
        seed_policy_ids.add(str(data["种子案件"]["保单号"]))

    seed_comm_ids = find_seed_communities(node_to_comm, seed_policy_ids)

    results: List[SubCommunityResult] = []
    for cid in sorted(seed_comm_ids):
        members = [n for n, c in node_to_comm.items() if c == cid]
        sub = G.subgraph(members).copy()

        base = component_metrics_from_claims(sub)
        ext = compute_extended_metrics(sub)
        score, reasons = compute_community_risk_score(base, ext)

        rel_dist = relation_type_distribution(sub)
        bridges = compute_bridge_nodes(sub, top_k=top_k)
        desc = describe_propagation(sub, bridges, rel_dist)

        sub_claims = _claims_in_subgraph(sub)

        # 获取种子案件号，用于剔除自比较条目
        seed_case_id = str((data.get("种子案件") or {}).get("案件号") or "").strip()

        high_claims = [
            {
                "案件号": c.get("案件号"),
                "保单号": c.get("保单号"),
                "报案日期": c.get("报案日期"),
                "医院名称": c.get("医院名称"),
                "疾病名称": c.get("疾病名称"),
                "住院天数": c.get("住院天数"),
                "赔付金额": c.get("赔付金额"),
                "mo_score": round(float(c.get("mo_score", 0.0) or 0.0), 6),
            }
            for c in sub_claims
            if float(c.get("mo_score", 0.0) or 0.0) >= HIGH_SIMILARITY_THRESHOLD
            and str(c.get("案件号") or "").strip() != seed_case_id
        ]
        high_claims.sort(key=lambda x: x["mo_score"], reverse=True)

        total_amount = 0.0
        for c in sub_claims:
            try:
                total_amount += float(c.get("赔付金额", 0.0) or 0.0)
            except Exception:
                pass

        results.append(
            SubCommunityResult(
                社群编号=cid,
                包含种子节点=bool(seed_policy_ids & set(members)),
                规模=sub.number_of_nodes(),
                边数=sub.number_of_edges(),
                涉及案件数=len(sub_claims),
                涉及赔付金额=round(total_amount, 2),
                基础指标={k: round(float(v), 6) for k, v in base.items()},
                扩展指标={k: round(float(v), 6) for k, v in ext.items()},
                社群风险分=score,
                桥接节点列表=bridges,
                关联类型分布=rel_dist,
                传播路径描述=desc,
                高相似案件列表=high_claims,
                置信度原因=reasons,
            )
        )

    results.sort(key=lambda x: x.社群风险分, reverse=True)
    return results, algo


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="保单传播图社群细分分析")
    p.add_argument("input", help="三层输入 JSON 文件")
    p.add_argument("--mo-scores", required=True, help="MO 评分文件（由 modus_operandi.py 输出）")
    p.add_argument("--output", default="leiden_results.json", help="输出文件路径")
    p.add_argument("--resolution", type=float, default=DEFAULT_LEIDEN_RESOLUTION)
    p.add_argument("--min-edge-weight", type=float, default=DEFAULT_MIN_EDGE_WEIGHT)
    p.add_argument("--top-bridge-k", type=int, default=DEFAULT_TOP_BRIDGE_K)
    p.add_argument("--force-leiden", action="store_true")
    return p.parse_args()


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    args = parse_args()
    data = _load_json(args.input)
    mo_json = _load_json(args.mo_scores)
    mo_scores = mo_json.get("节点MO评分", {}) if isinstance(mo_json, dict) else {}

    results, algo = analyze_seed_communities(
        data,
        mo_scores,
        config={
            "resolution": args.resolution,
            "min_edge_weight": args.min_edge_weight,
            "top_bridge_k": args.top_bridge_k,
            "force_leiden": args.force_leiden,
        },
    )

    out = {
        "算法": algo,
        "分辨率": args.resolution,
        "社群总数": len(results),
        "种子所在社群": [asdict(x) for x in results],
        "参数": {
            "最小边权重": args.min_edge_weight,
            "桥接节点TopK": args.top_bridge_k,
            "高相似阈值": HIGH_SIMILARITY_THRESHOLD,
            "扩展指标权重": EXTENDED_WEIGHTS,
            "基础指标缩放": BASE_SCALE,
        },
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("Wrote", args.output)


if __name__ == "__main__":
    main()
