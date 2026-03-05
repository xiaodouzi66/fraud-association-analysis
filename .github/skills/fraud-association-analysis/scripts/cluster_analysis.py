#!/usr/bin/env python3
"""
社区聚类主流程（v2）。

能力：
1) 读取三层输入（种子案件 + 保单节点列表 + 传播边列表 + 关联案件列表）
2) 基于保单图做连通分量分析（基础 gang_score）
3) 调用 modus_operandi 做 MO 相似度批量评分
4) 调用 community_detection 按需执行 Leiden 细分
5) 输出 clusters + leiden_社群 + mo_scores 的统一结果
"""

import argparse
import json
import time
from collections import defaultdict, Counter
from datetime import datetime
from dataclasses import asdict
from typing import Any, Dict, List, Set, Tuple

import networkx as nx

from modus_operandi import load_seed_mo, batch_score
from community_detection import analyze_seed_communities

# Configuration defaults
DEFAULT_MIN_EDGE_SCORE = 0.05
DEFAULT_MAX_NODES = 5000
SCORE_WEIGHTS = {
    "N": 0.12,
    "D": 0.12,
    "S_avg": 0.18,
    "H": 0.12,
    "B": 0.14,
    "M": 0.08,
    "RCR": 0.10,
    "C_dc": 0.14,
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("input", help="三层输入 JSON 文件")
    p.add_argument("--output", default="output.json")
    p.add_argument("--min-edge-score", type=float, default=DEFAULT_MIN_EDGE_SCORE)
    p.add_argument("--max-nodes", type=int, default=DEFAULT_MAX_NODES)
    p.add_argument("--force-leiden", action="store_true")
    p.add_argument("--leiden-resolution", type=float, default=1.0)
    p.add_argument("--top-bridge-k", type=int, default=5)
    return p.parse_args()


def read_input(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_graph_v2(data: Dict[str, Any], min_edge_score: float) -> nx.Graph:
    """按三层输入中的保单节点表和传播边表构建同构保单图。"""
    G = nx.Graph()
    for node in data.get("保单节点列表") or []:
        policy_id = str(node.get("保单号") or "").strip()
        if not policy_id:
            continue
        G.add_node(
            policy_id,
            保单号=policy_id,
            传播层级=int(node.get("传播层级", 0) or 0),
            传播风险分=float(node.get("传播风险分", 0.0) or 0.0),
            是否种子=bool(node.get("是否种子", False)),
            被保人ID=node.get("被保人ID"),
            投保日期=node.get("投保日期"),
            claims=[],
        )

    for edge in data.get("传播边列表") or []:
        src = str(edge.get("源保单号") or "").strip()
        dst = str(edge.get("目标保单号") or "").strip()
        if not src or not dst or src == dst:
            continue
        if src not in G or dst not in G:
            continue
        weight = float(edge.get("边权重", 0.0) or 0.0)
        if weight < min_edge_score:
            continue
        relation_type = str(edge.get("关联类型") or "未知")
        relation_value = str(edge.get("关联ID值") or "未知")
        if G.has_edge(src, dst):
            old_w = float(G[src][dst].get("weight", 0.0) or 0.0)
            G[src][dst]["weight"] = max(old_w, weight)
            G[src][dst]["关联类型"] = relation_type
            G[src][dst]["关联ID值"] = relation_value
        else:
            G.add_edge(
                src,
                dst,
                weight=weight,
                关联类型=relation_type,
                关联ID值=relation_value,
            )

    attach_claims_to_graph(G, data.get("关联案件列表") or [])
    return G


def attach_claims_to_graph(G: nx.Graph, claims: List[Dict[str, Any]]) -> None:
    """将案件列表挂载到保单节点上，供指标计算使用。"""
    for claim in claims:
        policy_id = str(claim.get("保单号") or "").strip()
        if not policy_id or policy_id not in G:
            continue
        G.nodes[policy_id]["claims"].append(dict(claim))


def component_metrics(G, comp_nodes):
    sub = G.subgraph(comp_nodes)
    N = sub.number_of_nodes()
    E = sub.number_of_edges()
    density = (2*E)/(N*(N-1)) if N>1 else 0.0

    scores = [float(sub.nodes[n].get("传播风险分", 0.0)) for n in sub.nodes()]
    S_avg = sum(scores)/len(scores) if scores else 0.0
    H = sum(1 for s in scores if s>=0.6)/N if N>0 else 0.0

    # burst: find max high-risk nodes in 24h sliding window
    ts_list = []
    for n in sub.nodes():
        ts = None
        claims = sub.nodes[n].get("claims", [])
        if claims:
            ts = claims[0].get("报案日期")
        try:
            ts_list.append((n, datetime.fromisoformat(ts)))
        except Exception:
            pass
    ts_list.sort(key=lambda x: x[1] if x[1] else datetime.min)
    max_count = 0
    for i in range(len(ts_list)):
        j=i
        while j < len(ts_list) and (ts_list[j][1] - ts_list[i][1]).total_seconds() <= 24*3600:
            j+=1
        window_nodes = ts_list[i:j]
        # count high-risk among window
        cnt = sum(1 for n,t in window_nodes if float(sub.nodes[n].get("传播风险分",0.0))>=0.6)
        max_count = max(max_count, cnt)
    B = min(max_count/4.0, 1.0)

    # disease concentration (Gini on diag_codes)
    diag_all = []
    for n in sub.nodes():
        for c in sub.nodes[n].get("claims", []):
            diag_all.extend(c.get("疾病名称") or [])
    C_dc = gini_concentration(diag_all)

    # repeat claim rate: agent/payee repeats within 7 days
    RCR = compute_rcr(sub)

    # motif score: triangles
    triangles = sum(nx.triangles(sub).values())/3 if N>0 else 0
    M = min(triangles,5)/5.0
    return {"N":N, "E":E, "density":density, "S_avg":S_avg, "H":H, "B":B, "C_dc":C_dc, "RCR":RCR, "M":M}


def gini_concentration(list_items):
    """Compute disease concentration as 1 - normalized_entropy.

    Returns 1.0 when all codes are identical (max concentration),
    0.0 when perfectly uniformly distributed.
    This fixes the Gini edge-case where a single unique code
    would incorrectly return 0.
    """
    if not list_items:
        return 0.0
    total = len(list_items)
    freq = Counter(list_items)
    k = len(freq)  # number of unique codes
    if k == 1:
        return 1.0  # all claims share the same code → maximum concentration
    import math
    entropy = -sum((v / total) * math.log2(v / total) for v in freq.values())
    max_entropy = math.log2(k)
    concentration = 1.0 - (entropy / max_entropy) if max_entropy > 0 else 0.0
    return max(0.0, min(1.0, concentration))


def compute_rcr(sub):
    # group by 医院名称 and count repeats in 7-day windows
    by_agent = defaultdict(list)
    for n in sub.nodes():
        for claim in sub.nodes[n].get("claims", []):
            agent = claim.get("医院名称")
            payee = claim.get("医院名称")
            ts = claim.get("报案日期")
            if ts:
                try:
                    t = datetime.fromisoformat(ts)
                except Exception:
                    t = None
            else:
                t = None
            if agent:
                by_agent[("agent", agent)].append((n, t))
            if payee:
                by_agent[("payee", payee)].append((n, t))

    max_repeat = 0
    for k, lst in by_agent.items():
        lst = [t for n,t in lst if t]
        lst.sort()
        for i in range(len(lst)):
            j=i
            while j < len(lst) and (lst[j] - lst[i]).days <= 7:
                j+=1
            max_repeat = max(max_repeat, j-i)
    if max_repeat <= 1:
        return 0.0
    return min((max_repeat-1)/4.0, 1.0)


def compute_mo_scores(data: Dict[str, Any]) -> Dict[str, float]:
    """计算关联案件相对种子案件的 MO 相似度评分。

    种子案件自身（通过案件号识别）会被自动从评分结果中剔除，
    避免自比较导致高相似统计虚高。
    """
    seed = data.get("种子案件") or {}
    seed_mo_raw = seed.get("MO特征") or seed
    seed_mo = load_seed_mo(seed_mo_raw)

    # 收集所有需要剔除的 ID：种子案件号，以及种子保单号
    exclude_ids: Set[str] = set()
    seed_case_id = str(seed.get("案件号") or "").strip()
    seed_policy_id = str(seed.get("保单号") or "").strip()
    if seed_case_id:
        exclude_ids.add(seed_case_id)
    if seed_policy_id:
        exclude_ids.add(seed_policy_id)

    claims = data.get("关联案件列表") or []
    return batch_score(seed_mo, claims, exclude_ids=exclude_ids)


def policy_level_mo_scores(data: Dict[str, Any], mo_scores: Dict[str, float]) -> Dict[str, float]:
    """按保单聚合案件 MO 均值，便于 clusters 输出解释。"""
    by_policy: Dict[str, List[float]] = defaultdict(list)
    for c in data.get("关联案件列表") or []:
        policy_id = str(c.get("保单号") or "").strip()
        claim_id = str(c.get("案件号") or "").strip()
        if not policy_id or not claim_id:
            continue
        if claim_id in mo_scores:
            by_policy[policy_id].append(float(mo_scores[claim_id]))

    out: Dict[str, float] = {}
    for policy_id, scores in by_policy.items():
        if scores:
            out[policy_id] = round(sum(scores) / len(scores), 6)
    return out


def apply_mo_enhancement(base_score: float, metrics: Dict[str, float], policy_ids: List[str], policy_mo: Dict[str, float]) -> Tuple[float, List[str]]:
    """在基础分上叠加 MO 解释性加权。"""
    mo_vals = [policy_mo[p] for p in policy_ids if p in policy_mo]
    if not mo_vals:
        return base_score, []

    mo_avg = sum(mo_vals) / len(mo_vals)
    mo_high = sum(1 for x in mo_vals if x >= 0.6) / len(mo_vals)
    enhanced = min(1.0, base_score + 0.20 * mo_avg + 0.05 * mo_high)

    reasons: List[str] = []
    if mo_avg >= 0.6:
        reasons.append("簇内保单对应案件与种子案件 MO 平均相似度较高")
    if mo_high >= 0.4:
        reasons.append("簇内存在较高比例的高 MO 相似保单")
    if metrics.get("H", 0.0) >= 0.3:
        reasons.append("簇内高风险保单占比较高")
    return enhanced, reasons


def _extract_seed_policy_ids(data: Dict[str, Any]) -> List[str]:
    ids = [
        str(n.get("保单号"))
        for n in (data.get("保单节点列表") or [])
        if bool(n.get("是否种子", False)) and str(n.get("保单号") or "").strip()
    ]
    if not ids:
        seed_policy = str((data.get("种子案件") or {}).get("保单号") or "").strip()
        if seed_policy:
            ids = [seed_policy]
    return ids


def _cluster_relation_summary(sub: nx.Graph) -> Dict[str, int]:
    dist = Counter()
    for _, _, edge in sub.edges(data=True):
        dist[str(edge.get("关联类型") or "未知")] += 1
    return dict(dist)


def _cluster_claim_amount(sub: nx.Graph) -> float:
    total = 0.0
    for n in sub.nodes():
        for claim in sub.nodes[n].get("claims", []):
            try:
                total += float(claim.get("赔付金额", 0.0) or 0.0)
            except Exception:
                continue
    return round(total, 2)


def normalize_metric(x, p10, p90):
    if p90 - p10 <= 0:
        return 0.0
    return max(0.0, min(1.0, (x - p10) / (p90 - p10)))


def compute_gang_score(metrics, weights=SCORE_WEIGHTS):
    """Compute weighted gang_score from normalized component metrics.

    N, density are normalized by empirical p10/p90.
    M is already in [0,1].  S_avg, H, B, RCR, C_dc are already in [0,1].
    In production replace p10/p90 constants with historical percentiles.
    """
    p10 = {"N": 1, "density": 0.0}
    p90 = {"N": 20, "density": 0.5}
    Nn = normalize_metric(metrics["N"], p10["N"], p90["N"]) if "N" in metrics else 0.0
    Dn = normalize_metric(metrics.get("density", 0.0), p10["density"], p90["density"])
    # B5 fix: M is already in [0,1] — no need to scale
    Mn = float(metrics.get("M", 0.0))
    S_avg = float(metrics.get("S_avg", 0.0))
    H = float(metrics.get("H", 0.0))
    B = float(metrics.get("B", 0.0))
    RCR = float(metrics.get("RCR", 0.0))
    C_dc = float(metrics.get("C_dc", 0.0))
    score = (weights["N"] * Nn + weights["D"] * Dn + weights["S_avg"] * S_avg
             + weights["H"] * H + weights["B"] * B + weights["M"] * Mn
             + weights["RCR"] * RCR + weights["C_dc"] * C_dc)
    contrib = {"N": Nn, "D": Dn, "S_avg": S_avg, "H": H,
               "B": B, "M": Mn, "RCR": RCR, "C_dc": C_dc}
    return min(score, 1.0), contrib


def apply_strong_rules(metrics, sub):
    # S-01: any blacklisted and H>=0.25
    for n in sub.nodes():
        claims = sub.nodes[n].get("claims", [])
        if any(bool(c.get("是否黑名单", False)) for c in claims) and metrics.get("H",0) >= 0.25:
            return True, 0.8, "黑名单命中且高风险节点占比>=25%"
    # S-02: same payee >=3 in 7 days and disease_conc>=0.6
    # compute by payee
    payee_counts = defaultdict(list)
    for n in sub.nodes():
        for c in sub.nodes[n].get("claims", []):
            p = c.get("医院名称")
            ts = c.get("报案日期")
            if p and ts:
                try:
                    payee_counts[p].append(datetime.fromisoformat(ts))
                except Exception:
                    pass
    for p, times in payee_counts.items():
        times.sort()
        for i in range(len(times)):
            j=i
            while j < len(times) and (times[j]-times[i]).days <= 7:
                j+=1
            if j-i >= 3 and metrics.get("C_dc",0) >= 0.6:
                return True, 0.75, "同一收款7天内>=3次且诊疗高度集中"
    # S-03: burst >=3 and shared agent/payee
    if metrics.get("B",0) >= 0.75 and metrics.get("N",0) >= 4:
        # check shared hospital among claims
        hospitals = Counter()
        for n in sub.nodes():
            for c in sub.nodes[n].get("claims", []):
                h = c.get("医院名称")
                if h:
                    hospitals[h] += 1
        if any(v>=2 for v in hospitals.values()):
            return True, 0.75, "短时突增且共享代理/收款"
    # S-04: photo hash reuse (not implemented here) -> placeholder
    return False, None, None


def ego_split_and_evaluate(G, super_node, min_edge_score):
    """Extract 2-hop ego graph, prune weak edges, then detect local clusters.

    B2 fix: nx.ego_graph returns a SubGraph VIEW for undirected graphs;
    calling remove_edges_from on a view mutates the original G.
    We .copy() first to avoid side effects.
    """
    ego = nx.ego_graph(G, super_node, radius=2).copy()  # B2 fix: copy, not view
    # prune weak edges on the copy
    edges_to_remove = [
        (u, v) for u, v, d in ego.edges(data=True)
        if d.get("weight", 0.0) < min_edge_score
    ]
    ego.remove_edges_from(edges_to_remove)
    # remove isolated nodes created by pruning
    ego.remove_nodes_from(list(nx.isolates(ego)))
    comps = list(nx.connected_components(ego))
    results = []
    for c in comps:
        if len(c) < 2:  # skip trivial single-node components
            continue
        metrics = component_metrics(ego, c)
        score, contrib = compute_gang_score(metrics)
        if score >= 0.4:
            results.append((c, metrics, score, contrib))
    return results


def _recommended_action(score):
    if score >= 0.7:
        return "manual_review"
    elif score >= 0.4:
        return "collect_more_evidence"
    return "monitor"


def analyze_v2(data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    G = build_graph_v2(data, config.get("min_edge_score", DEFAULT_MIN_EDGE_SCORE))

    if G.number_of_nodes() > config.get("max_nodes", DEFAULT_MAX_NODES):
        return {"error": "too_many_nodes", "count": G.number_of_nodes()}

    mo_scores = compute_mo_scores(data)
    mo_scores_by_policy = policy_level_mo_scores(data, mo_scores)

    seed_policy_ids = set(_extract_seed_policy_ids(data))

    clusters = []
    for comp in nx.connected_components(G):
        metrics = component_metrics(G, comp)
        score, contrib = compute_gang_score(metrics)
        comp_list = list(comp)
        score, mo_reasons = apply_mo_enhancement(score, metrics, comp_list, mo_scores_by_policy)
        confidence_reasons = []

        # B3 fix: capture strong-rule reason and store it
        hit, forced_score, reason = apply_strong_rules(metrics, G.subgraph(comp))
        if hit:
            score = max(score, forced_score)
            confidence_reasons.append(reason)  # B3 fix: no longer discarded
        confidence_reasons.extend(mo_reasons)

        # B7 fix: compute max_deg once, not in every iteration
        degrees = [G.degree(n) for n in comp_list]
        max_deg = max(degrees) if degrees else 0

        ego_split_done = False
        if max_deg > 50:  # in prod use percentile of the full degree distribution
            # B7 fix: find super_node without recomputing max
            super_node = comp_list[degrees.index(max_deg)]
            splits = ego_split_and_evaluate(
                G, super_node, config.get("min_edge_score", DEFAULT_MIN_EDGE_SCORE)
            )
            for c_nodes, c_metrics, c_score, c_contrib in splits:
                c_node_list = list(c_nodes)
                c_score, c_mo_reasons = apply_mo_enhancement(
                    c_score,
                    c_metrics,
                    c_node_list,
                    mo_scores_by_policy,
                )
                c_action = _recommended_action(c_score)
                c_sub = G.subgraph(c_nodes)
                clusters.append({
                    "cluster_id": f"ego-{super_node}-{hash(frozenset(c_nodes))}",
                    "source": "ego_split",  # B6 fix: tag origin so callers can distinguish
                    "parent_cluster_id": str(hash(frozenset(comp))),
                    "members": c_node_list,
                    "metrics": c_metrics,
                    "gang_score": round(c_score, 4),
                    "contrib": c_contrib,
                    "mo_similarity_avg": round(sum(mo_scores_by_policy.get(n, 0.0) for n in c_node_list) / len(c_node_list), 6) if c_node_list else 0.0,
                    "relation_distribution": _cluster_relation_summary(c_sub),
                    "claim_amount_total": _cluster_claim_amount(c_sub),
                    "recommended_action": c_action,  # B4 fix
                    "confidence_reasons": c_mo_reasons,
                })
            ego_split_done = True

        action = _recommended_action(score)
        subgraph = G.subgraph(comp)
        clusters.append({
            "cluster_id": str(hash(frozenset(comp))),
            "source": "full_component",  # B6 fix: tag origin
            "has_ego_splits": ego_split_done,
            "members": comp_list,
            "metrics": metrics,
            "gang_score": round(score, 4),
            "contrib": contrib,
            "mo_similarity_avg": round(sum(mo_scores_by_policy.get(n, 0.0) for n in comp_list) / len(comp_list), 6) if comp_list else 0.0,
            "relation_distribution": _cluster_relation_summary(subgraph),
            "claim_amount_total": _cluster_claim_amount(subgraph),
            "包含种子节点": bool(seed_policy_ids & set(comp_list)),
            "recommended_action": action,   # B4 fix
            "confidence_reasons": confidence_reasons,  # B3/B4 fix
        })

    leiden_cfg = {
        "resolution": float(config.get("leiden_resolution", 1.0)),
        "min_edge_weight": float(config.get("min_edge_score", DEFAULT_MIN_EDGE_SCORE)),
        "top_bridge_k": int(config.get("top_bridge_k", 5)),
        "force_leiden": bool(config.get("force_leiden", False)),
    }

    leiden_results, leiden_algo = analyze_seed_communities(data, mo_scores, config=leiden_cfg)

    if isinstance(leiden_results, list):
        leiden_json = [asdict(x) for x in leiden_results]
    else:
        leiden_json = []

    clusters.sort(key=lambda x: x.get("gang_score", 0.0), reverse=True)

    return {
        "clusters": clusters,
        "leiden_算法": leiden_algo,
        "leiden_社群": leiden_json,
        "mo_scores": mo_scores,
    }


def main():
    args = parse_args()
    data = read_input(args.input)

    config = {
        "min_edge_score": args.min_edge_score,
        "max_nodes": args.max_nodes,
        "force_leiden": args.force_leiden,
        "leiden_resolution": args.leiden_resolution,
        "top_bridge_k": args.top_bridge_k,
    }

    t0 = time.time()
    res = analyze_v2(data, config)
    res["diagnostics"] = {"processing_time_ms": int((time.time()-t0)*1000), "params":config}
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    print("Wrote", args.output)


if __name__ == "__main__":
    main()
