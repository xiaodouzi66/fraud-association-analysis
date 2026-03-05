#!/usr/bin/env python3
"""
社群可视化模块：基于 networkx + matplotlib 输出 PNG。

输入：
1) 三层输入 JSON（包含保单节点列表、传播边列表）
2) community_detection.py 产出的 leiden_results.json

输出：
- 每个种子社群一张 PNG
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import networkx as nx


RELATION_COLOR_MAP = {
    "共用代理人ID": "#9B59B6",
    "共用手机号": "#27AE60",
    "共用身份证号": "#E6A817",
    "共用银行卡号": "#2980B9",
}
RELATION_DEFAULT_COLOR = "#AAAAAA"

NODE_COLOR_SEED = "#E74C3C"      # 红
NODE_COLOR_HIGH_SIM = "#F39C12"  # 橙
NODE_COLOR_NORMAL = "#4A90E2"    # 蓝
NODE_COLOR_NO_CLAIM = "#BFC5CD"  # 灰


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _set_chinese_font() -> None:
    candidates = [
        "PingFang SC",
        "Heiti SC",
        "STHeiti",
        "Arial Unicode MS",
        "Noto Sans CJK SC",
        "WenQuanYi Zen Hei",
    ]
    plt.rcParams["font.sans-serif"] = candidates + plt.rcParams.get("font.sans-serif", [])
    plt.rcParams["axes.unicode_minus"] = False


def build_policy_graph(data: Dict[str, Any], min_edge_weight: float = 0.05) -> nx.Graph:
    nodes = data.get("保单节点列表") or []
    edges = data.get("传播边列表") or []

    G = nx.Graph()

    for n in nodes:
        pid = str(n.get("保单号") or "").strip()
        if not pid:
            continue
        G.add_node(
            pid,
            保单号=pid,
            传播层级=int(n.get("传播层级", 0) or 0),
            传播风险分=float(n.get("传播风险分", 0.0) or 0.0),
            是否种子=bool(n.get("是否种子", False)),
        )

    for e in edges:
        src = str(e.get("源保单号") or "").strip()
        dst = str(e.get("目标保单号") or "").strip()
        if not src or not dst or src == dst:
            continue
        if src not in G or dst not in G:
            continue

        weight = float(e.get("边权重", 0.0) or 0.0)
        if weight < min_edge_weight:
            continue

        G.add_edge(
            src,
            dst,
            weight=weight,
            关联类型=str(e.get("关联类型") or "未知"),
            关联ID值=str(e.get("关联ID值") or "未知"),
        )

    return G


def run_leiden_or_fallback(G: nx.Graph, resolution: float = 1.0) -> Dict[str, str]:
    if G.number_of_nodes() == 0:
        return {}

    try:
        import igraph as ig  # type: ignore
        import leidenalg  # type: ignore

        nodes = list(G.nodes())
        index = {node: i for i, node in enumerate(nodes)}
        edges = [(index[u], index[v]) for u, v in G.edges()]
        weights = [float(G[u][v].get("weight", 1.0)) for u, v in G.edges()]

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
        return node_to_comm

    except Exception:
        comms = nx.community.greedy_modularity_communities(G, weight="weight")
        node_to_comm: Dict[str, str] = {}
        for comm_idx, members in enumerate(comms):
            cid = f"comm_{comm_idx}"
            for node in members:
                node_to_comm[node] = cid
        return node_to_comm


def choose_layout(sub: nx.Graph) -> Dict[str, Tuple[float, float]]:
    n = sub.number_of_nodes()
    if n <= 1:
        return {node: (0.0, 0.0) for node in sub.nodes()}
    if n < 20:
        return nx.spring_layout(sub, seed=42, k=0.8)
    if n <= 50:
        return nx.kamada_kawai_layout(sub)

    # 大图：按传播层级做 shell 分圈
    level_to_nodes: Dict[int, List[str]] = defaultdict(list)
    for node in sub.nodes():
        level = int(sub.nodes[node].get("传播层级", 0) or 0)
        level_to_nodes[level].append(node)

    shells = [level_to_nodes[k] for k in sorted(level_to_nodes.keys()) if level_to_nodes[k]]
    if not shells:
        return nx.spring_layout(sub, seed=42)
    return nx.shell_layout(sub, nlist=shells)


def _node_visuals(
    sub: nx.Graph,
    seed_ids: Set[str],
    high_sim_ids: Set[str],
    no_claim_ids: Set[str],
    blacklisted_ids: Set[str],
    bridge_ids: Set[str],
) -> Tuple[List[str], List[float], List[str], List[float]]:
    colors: List[str] = []
    sizes: List[float] = []
    edge_colors: List[str] = []
    edge_widths: List[float] = []

    for node in sub.nodes():
        risk = float(sub.nodes[node].get("传播风险分", 0.0) or 0.0)
        size = 300 + risk * 1200
        sizes.append(size)

        if node in seed_ids:
            colors.append(NODE_COLOR_SEED)
        elif node in high_sim_ids:
            colors.append(NODE_COLOR_HIGH_SIM)
        elif node in no_claim_ids:
            colors.append(NODE_COLOR_NO_CLAIM)
        else:
            colors.append(NODE_COLOR_NORMAL)

        if node in blacklisted_ids:
            edge_colors.append("#8B0000")
            edge_widths.append(2.6)
        elif node in bridge_ids:
            edge_colors.append("#DAA520")
            edge_widths.append(2.2)
        else:
            edge_colors.append("#FFFFFF")
            edge_widths.append(0.8)

    return colors, sizes, edge_colors, edge_widths


def _edge_visuals(sub: nx.Graph) -> Tuple[List[str], List[float]]:
    colors: List[str] = []
    widths: List[float] = []
    for u, v, d in sub.edges(data=True):
        rel = str(d.get("关联类型") or "未知")
        colors.append(RELATION_COLOR_MAP.get(rel, RELATION_DEFAULT_COLOR))
        w = float(d.get("weight", 0.0) or 0.0)
        widths.append(0.5 + 2.5 * max(0.0, min(1.0, w)))
    return colors, widths


def _short_label(node: str, layer: int) -> str:
    if len(node) > 14:
        node = node[:11] + "..."
    return f"{node}\nL{layer}"


def _draw_legend(ax: plt.Axes) -> None:
    node_handles = [
        Line2D([0], [0], marker="o", color="w", label="种子保单", markerfacecolor=NODE_COLOR_SEED, markersize=10),
        Line2D([0], [0], marker="o", color="w", label="高相似案件保单", markerfacecolor=NODE_COLOR_HIGH_SIM, markersize=10),
        Line2D([0], [0], marker="o", color="w", label="普通传播保单", markerfacecolor=NODE_COLOR_NORMAL, markersize=10),
        Line2D([0], [0], marker="o", color="w", label="无案件保单", markerfacecolor=NODE_COLOR_NO_CLAIM, markersize=10),
        Patch(facecolor="#FFFFFF", edgecolor="#DAA520", label="桥接节点（金边）"),
        Patch(facecolor="#FFFFFF", edgecolor="#8B0000", label="黑名单关联节点（红边）"),
    ]

    edge_handles = [
        Line2D([0], [0], color=RELATION_COLOR_MAP["共用代理人ID"], lw=2, label="共用代理人ID"),
        Line2D([0], [0], color=RELATION_COLOR_MAP["共用手机号"], lw=2, label="共用手机号"),
        Line2D([0], [0], color=RELATION_COLOR_MAP["共用身份证号"], lw=2, label="共用身份证号"),
        Line2D([0], [0], color=RELATION_DEFAULT_COLOR, lw=2, label="其他关联"),
    ]

    handles = node_handles + edge_handles
    ax.legend(handles=handles, loc="lower left", frameon=True, fontsize=9)


def _draw_summary_box(ax: plt.Axes, result: Dict[str, Any], sub: nx.Graph) -> None:
    text = (
        f"社群编号: {result.get('社群编号', 'N/A')}\n"
        f"保单数: {sub.number_of_nodes()}\n"
        f"关联边数: {sub.number_of_edges()}\n"
        f"案件数: {result.get('涉及案件数', 0)}\n"
        f"风险分: {result.get('社群风险分', 0):.4f}"
    )
    ax.text(
        0.99,
        0.99,
        text,
        transform=ax.transAxes,
        va="top",
        ha="right",
        fontsize=10,
        bbox={"facecolor": "white", "alpha": 0.85, "edgecolor": "#CCCCCC"},
    )


def draw_community(
    sub: nx.Graph,
    result: Dict[str, Any],
    output_path: str,
    show_edge_labels: bool = False,
    figsize: Tuple[int, int] = (14, 10),
    dpi: int = 150,
) -> str:
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    ax.set_axis_off()

    seed_ids = {
        n for n in sub.nodes() if bool(sub.nodes[n].get("是否种子", False))
    }

    high_sim_ids = {
        str(x.get("保单号"))
        for x in (result.get("高相似案件列表") or [])
        if str(x.get("保单号") or "").strip()
    }

    # 无案件节点：不在高相似列表，且在结果中没有明细可认定时，用输入 claims 判断较稳定
    # visualize 脚本没有直接挂 claims，这里采用“高相似外 + 非种子 + 低风险”近似灰显
    no_claim_ids = {
        n for n in sub.nodes()
        if n not in high_sim_ids and n not in seed_ids and float(sub.nodes[n].get("传播风险分", 0.0)) < 0.45
    }

    blacklisted_ids = set()
    for x in (result.get("高相似案件列表") or []):
        if bool(x.get("是否黑名单", False)):
            pid = str(x.get("保单号") or "").strip()
            if pid:
                blacklisted_ids.add(pid)

    bridge_ids = {
        str(x.get("保单号"))
        for x in (result.get("桥接节点列表") or [])
        if str(x.get("保单号") or "").strip()
    }

    pos = choose_layout(sub)
    node_colors, node_sizes, node_edge_colors, node_edge_widths = _node_visuals(
        sub,
        seed_ids=seed_ids,
        high_sim_ids=high_sim_ids,
        no_claim_ids=no_claim_ids,
        blacklisted_ids=blacklisted_ids,
        bridge_ids=bridge_ids,
    )

    edge_colors, edge_widths = _edge_visuals(sub)

    nx.draw_networkx_edges(
        sub,
        pos,
        ax=ax,
        edge_color=edge_colors,
        width=edge_widths,
        alpha=0.8,
    )

    nx.draw_networkx_nodes(
        sub,
        pos,
        ax=ax,
        node_color=node_colors,
        node_size=node_sizes,
        edgecolors=node_edge_colors,
        linewidths=node_edge_widths,
        alpha=0.95,
    )

    labels = {
        n: _short_label(str(n), int(sub.nodes[n].get("传播层级", 0) or 0))
        for n in sub.nodes()
    }
    nx.draw_networkx_labels(sub, pos, labels=labels, font_size=8, ax=ax)

    if show_edge_labels and sub.number_of_edges() <= 40:
        edge_labels = {
            (u, v): str(d.get("关联ID值") or "")
            for u, v, d in sub.edges(data=True)
        }
        nx.draw_networkx_edge_labels(
            sub,
            pos,
            edge_labels=edge_labels,
            font_size=7,
            rotate=False,
            ax=ax,
        )

    _draw_legend(ax)
    _draw_summary_box(ax, result, sub)

    ax.set_title(f"可疑社群可视化 - {result.get('社群编号', 'N/A')}", fontsize=14)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    return output_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="社群可视化 PNG 生成")
    p.add_argument("input", help="三层输入 JSON 文件")
    p.add_argument("--leiden-results", required=True, help="community_detection 输出的 JSON 文件")
    p.add_argument("--output-dir", default="output_png", help="输出目录")
    p.add_argument("--resolution", type=float, default=1.0, help="Leiden 分辨率（用于重建社群映射）")
    p.add_argument("--min-edge-weight", type=float, default=0.05, help="最小边权重")
    p.add_argument("--show-edge-labels", action="store_true", help="显示边关联ID标签")
    p.add_argument("--dpi", type=int, default=150)
    return p.parse_args()


def main() -> None:
    _set_chinese_font()
    args = parse_args()

    data = _load_json(args.input)
    leiden_json = _load_json(args.leiden_results)
    results = leiden_json.get("种子所在社群", []) if isinstance(leiden_json, dict) else []

    G = build_policy_graph(data, min_edge_weight=args.min_edge_weight)
    node_to_comm = run_leiden_or_fallback(G, resolution=args.resolution)

    # 反向索引：社群ID -> 成员保单
    comm_to_nodes: Dict[str, List[str]] = defaultdict(list)
    for node, cid in node_to_comm.items():
        comm_to_nodes[cid].append(node)

    os.makedirs(args.output_dir, exist_ok=True)
    generated: List[str] = []

    for result in results:
        cid = str(result.get("社群编号") or "").strip()
        if not cid:
            continue
        members = comm_to_nodes.get(cid, [])
        if not members:
            continue

        sub = G.subgraph(members).copy()
        filename = f"community_{cid}.png"
        output_path = os.path.join(args.output_dir, filename)

        draw_community(
            sub,
            result=result,
            output_path=output_path,
            show_edge_labels=args.show_edge_labels,
            dpi=args.dpi,
        )
        generated.append(output_path)

    print(json.dumps({"生成图片数": len(generated), "图片路径": generated}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
