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
from collections import defaultdict
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
    size_map = _extract_comm_size_map(leiden_results, node_to_comm)

    comm_ids = sorted(size_map.keys())
    if not comm_ids:
        return ""

    min_risk = min(risk_map.get(cid, 0.0) for cid in comm_ids)
    max_risk = max(risk_map.get(cid, 0.0) for cid in comm_ids)

    nodes = []
    for cid in comm_ids:
        size_val = size_map.get(cid, 1)
        risk_val = risk_map.get(cid, 0.0)
        label = _community_label_by_degree(raw_edges, cid, node_to_comm)
        nodes.append(
            {
                "name": cid,
                "symbolSize": max(10, min(60, 8 + int(math.log(max(size_val, 1) + 1, 1.5)))),
                "value": size_val,
                "itemStyle": {"color": _risk_to_red(risk_val, min_risk, max_risk)},
                "tooltip": {
                    "formatter": f"社群: {cid}<br/>标签: {label}<br/>保单规模: {size_val}<br/>风险分: {risk_val:.4f}"
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
    return output_path


def _id_node_color(node_attrs: Dict[str, Any]) -> str:
    rel_type = str(node_attrs.get("关联类型") or "unknown")
    return NODE_COLOR_MAP.get(rel_type, NODE_COLOR_MAP["unknown"])


def _node_title(node_id: str, attrs: Dict[str, Any], degree: int) -> str:
    if attrs.get("node_type") == "policy":
        return (
            f"保单号: {node_id}<br/>"
            f"节点类型: 保单<br/>"
            f"传播层级: {attrs.get('传播层级', 0)}<br/>"
            f"传播风险分: {_safe_float(attrs.get('传播风险分', 0.0)):.4f}<br/>"
            f"连接度: {degree}"
        )
    return (
        f"节点: {node_id}<br/>"
        f"节点类型: ID<br/>"
        f"关联类型: {attrs.get('关联类型', '未知')}<br/>"
        f"关联ID值: {attrs.get('关联ID值', '')}<br/>"
        f"连接保单数: {degree}"
    )


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
) -> str:
    sub = _filtered_subgraph_if_needed(G, max_nodes_before_filter=max_nodes_before_filter)

    net = Network(
        height="900px",
        width="100%",
        select_menu=True,
        filter_menu=True,
        cdn_resources="remote",
        directed=False,
        notebook=False,
    )

    for node, attrs in sub.nodes(data=True):
        degree = sub.degree(node)
        node_type = attrs.get("node_type", "policy")
        if node_type == "policy":
            color = NODE_COLOR_MAP["policy"]
            size = 8 + min(30, degree)
            label = str(node)
        else:
            color = _id_node_color(attrs)
            size = 8 + min(35, degree)
            rel_type = str(attrs.get("关联类型") or "ID")
            rel_val = str(attrs.get("关联ID值") or "")
            short_val = rel_val[:12] + "..." if len(rel_val) > 12 else rel_val
            label = f"{rel_type}:{short_val}"

        net.add_node(
            str(node),
            label=label,
            size=size,
            color=color,
            title=_node_title(str(node), attrs, degree),
            group=node_type,
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
        }
        """
    )

    dir_part = os.path.dirname(output_path)
    if dir_part:
        os.makedirs(dir_part, exist_ok=True)
    net.show(output_path, notebook=False)

    with open(output_path, "a", encoding="utf-8") as f:
        legend_html = (
            "<div style=\"position:absolute; top:50px; right:10px; background:rgba(255,255,255,0.92); "
            "padding:12px; border:1px solid #ccc; border-radius:6px; font-family:Arial; font-size:12px;\">"
            f"<h4 style='margin:0 0 8px 0;'>{title}</h4>"
            "<p style='margin:4px 0;'><span style='display:inline-block;width:12px;height:12px;background:#4e79a7;margin-right:6px;'></span>保单号</p>"
            "<p style='margin:4px 0;'><span style='display:inline-block;width:12px;height:12px;background:#f28e2b;margin-right:6px;'></span>手机号</p>"
            "<p style='margin:4px 0;'><span style='display:inline-block;width:12px;height:12px;background:#e15759;margin-right:6px;'></span>身份证号</p>"
            "<p style='margin:4px 0;'><span style='display:inline-block;width:12px;height:12px;background:#76b7b2;margin-right:6px;'></span>邮箱</p>"
            "<p style='margin:4px 0;'><span style='display:inline-block;width:12px;height:12px;background:#59a14f;margin-right:6px;'></span>代理人ID</p>"
            "<p style='margin:4px 0;'><span style='display:inline-block;width:12px;height:12px;background:#edc948;margin-right:6px;'></span>银行卡号</p>"
            "</div>"
        )
        f.write(legend_html)

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
            sub_path = os.path.join(output_dir, f"subgraph_{cid}_rank{idx}.html")
            render_pyvis_graph(
                sub,
                output_path=sub_path,
                title=f"Top{idx} 风险子社群 {cid}",
                max_nodes_before_filter=max_nodes_before_filter,
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
