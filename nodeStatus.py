#!/usr/bin/env python3
"""
nodeStatus.py — Generate an HTML node-status report from sinfo output.
Usage: python3 nodeStatus.py [-o output.html]
"""

import argparse
import csv
import re
import subprocess
import sys
from datetime import datetime
from collections import defaultdict
from pathlib import Path

# --- State classification ---

CATEGORY_ORDER = ["nonfunctional", "suspicious", "ok", "unknown"]
CATEGORY_PRIORITY = {c: i for i, c in enumerate(CATEGORY_ORDER)}  # lower = worse


def classify_state(state: str) -> str:
    s = state.lower().rstrip("*")
    if s in ("drain", "drained", "drng", "draining", "down"):
        return "nonfunctional"
    if s in ("idle", "unk", "unknown", "comp", "completing"):
        return "suspicious"
    if s in ("mix", "mixed", "alloc", "allocated"):
        return "ok"
    return "unknown"


# --- Node range expansion ---

def expand_nodelist(nodelist: str) -> list:
    if nodelist in ("(null)", "N/A", ""):
        return []
    nodes = []
    for part in _split_nodelist(nodelist):
        nodes.extend(_expand_node(part))
    return nodes


def _split_nodelist(nodelist: str) -> list:
    """Split comma-separated nodelist respecting bracket groups."""
    parts = []
    depth = 0
    current = []
    for ch in nodelist:
        if ch == "[":
            depth += 1
            current.append(ch)
        elif ch == "]":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _expand_node(node: str) -> list:
    """Expand a single node spec like dn[003,028-029] or hn008."""
    m = re.match(r"^([^\[]+)\[([^\]]+)\]$", node)
    if not m:
        return [node]

    prefix = m.group(1)
    range_str = m.group(2)

    result = []
    for token in range_str.split(","):
        if "-" in token:
            start, end = token.split("-", 1)
            width = len(start)
            for i in range(int(start), int(end) + 1):
                result.append(f"{prefix}{i:0{width}d}")
        else:
            result.append(f"{prefix}{token}")
    return result


# --- sinfo parsing ---

def get_sinfo_lines() -> list:
    try:
        result = subprocess.run(
            ["sinfo", "-h", "--format=%P|%a|%l|%D|%T|%N"],
            capture_output=True, text=True,
        )
    except FileNotFoundError:
        print("Error: sinfo not found. Is this a SLURM cluster?", file=sys.stderr)
        sys.exit(1)
    if result.returncode != 0:
        print(f"sinfo failed: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip().splitlines()


def parse_sinfo(lines: list) -> dict:
    """
    Returns OrderedDict-like: {partition: {category: sorted_list_of_nodes}}
    A node appearing under multiple states takes the worst category.
    """
    partition_order = []
    # partition → node → category string
    partition_node_cat: dict = defaultdict(dict)

    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 6:
            continue

        partition_raw, _, _, _, state, nodelist = (
            parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
        )

        partition = partition_raw.rstrip("*")
        if partition not in partition_node_cat:
            partition_order.append(partition)

        category = classify_state(state)
        nodes = expand_nodelist(nodelist)

        for node in nodes:
            existing = partition_node_cat[partition].get(node)
            if existing is None or CATEGORY_PRIORITY[category] < CATEGORY_PRIORITY[existing]:
                partition_node_cat[partition][node] = category

    result = {}
    for partition in partition_order:
        by_cat: dict = {c: [] for c in CATEGORY_ORDER}
        for node, cat in partition_node_cat[partition].items():
            by_cat[cat].append(node)
        for cat in CATEGORY_ORDER:
            by_cat[cat].sort()
        result[partition] = by_cat

    return result


# --- Historical CSV ---

def append_history(data: dict, timestamp: str, csv_path: str) -> None:
    path = Path(csv_path)
    write_header = not path.exists() or path.stat().st_size == 0
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["timestamp", "node", "partition", "category"])
        for partition, by_cat in data.items():
            for category, nodes in by_cat.items():
                for node in nodes:
                    writer.writerow([timestamp, node, partition, category])


# --- HTML generation ---

CATEGORY_META = {
    "nonfunctional": {
        "label": "Nonfunctional",
        "color": "#dc2626",
        "bg": "#fef2f2",
        "border": "#fca5a5",
        "tag_bg": "#dc2626",
        "tag_fg": "#fff",
    },
    "suspicious": {
        "label": "Suspicious",
        "color": "#b45309",
        "bg": "#fffbeb",
        "border": "#fcd34d",
        "tag_bg": "#d97706",
        "tag_fg": "#fff",
    },
    "ok": {
        "label": "OK",
        "color": "#15803d",
        "bg": "#f0fdf4",
        "border": "#86efac",
        "tag_bg": "#16a34a",
        "tag_fg": "#fff",
    },
    "unknown": {
        "label": "Unknown",
        "color": "#4b5563",
        "bg": "#f9fafb",
        "border": "#d1d5db",
        "tag_bg": "#6b7280",
        "tag_fg": "#fff",
    },
}


def _node_pills(nodes: list, meta: dict) -> str:
    if not nodes:
        return '<span style="color:#9ca3af;font-style:italic">none</span>'
    pills = []
    for node in nodes:
        pills.append(
            f'<span style="display:inline-block;margin:2px 3px;padding:2px 8px;'
            f'background:{meta["bg"]};border:1px solid {meta["border"]};'
            f'border-radius:4px;font-family:monospace;font-size:0.82em;'
            f'color:{meta["color"]}">{node}</span>'
        )
    return "".join(pills)


def _partition_section(partition: str, by_cat: dict) -> str:
    # Count badges in header
    badge_html_parts = []
    for cat in ["nonfunctional", "suspicious", "ok"]:
        n = len(by_cat[cat])
        if n:
            m = CATEGORY_META[cat]
            badge_html_parts.append(
                f'<span style="margin-left:8px;padding:1px 9px;'
                f'background:{m["tag_bg"]};border-radius:12px;'
                f'font-size:0.72em;color:{m["tag_fg"]};font-weight:600;'
                f'vertical-align:middle">{n} {m["label"]}</span>'
            )
    badge_html = "".join(badge_html_parts)

    sub_sections = []
    for cat in CATEGORY_ORDER:
        nodes = by_cat[cat]
        meta = CATEGORY_META[cat]
        if not nodes and cat in ("ok", "unknown"):
            continue

        heading = (
            f'<div style="font-weight:600;font-size:0.92em;color:{meta["color"]};'
            f'margin-bottom:5px">{meta["label"]} ({len(nodes)})</div>'
        )
        pills = _node_pills(nodes, meta)

        if cat == "ok":
            sub_sections.append(
                f'<details style="margin-top:12px">'
                f'<summary style="cursor:pointer;font-weight:600;font-size:0.92em;'
                f'color:{meta["color"]};user-select:none;list-style:none;'
                f'display:flex;align-items:center;gap:4px">'
                f'<span>▸</span><span>OK ({len(nodes)})</span></summary>'
                f'<div style="margin-top:6px">{pills}</div>'
                f'</details>'
            )
        else:
            sub_sections.append(
                f'<div style="margin-top:12px">{heading}<div>{pills}</div></div>'
            )

    body = "".join(sub_sections)

    return (
        f'<details open style="background:#fff;border:1px solid #e5e7eb;'
        f'border-radius:10px;margin-bottom:14px;padding:14px 18px">'
        f'<summary style="cursor:pointer;list-style:none;user-select:none;'
        f'display:flex;align-items:center">'
        f'<span style="font-size:1.05em;font-weight:700;margin-right:2px">{partition}</span>'
        f'{badge_html}</summary>'
        f'<div style="margin-top:4px">{body}</div>'
        f'</details>'
    )


def generate_html(data: dict, generated_at: str) -> str:
    totals = {c: 0 for c in CATEGORY_ORDER}
    for by_cat in data.values():
        for cat in CATEGORY_ORDER:
            totals[cat] += len(by_cat[cat])

    summary_cards = []
    for cat in ["nonfunctional", "suspicious", "ok", "unknown"]:
        meta = CATEGORY_META[cat]
        n = totals[cat]
        summary_cards.append(
            f'<div style="padding:14px 22px;background:{meta["bg"]};'
            f'border:1px solid {meta["border"]};border-radius:10px;'
            f'text-align:center;min-width:110px">'
            f'<div style="font-size:2.2em;font-weight:800;color:{meta["color"]};'
            f'line-height:1">{n}</div>'
            f'<div style="font-size:0.8em;color:#374151;margin-top:4px">{meta["label"]}</div>'
            f'</div>'
        )
    summary_html = "".join(summary_cards)

    partitions_html = "".join(
        _partition_section(partition, by_cat)
        for partition, by_cat in data.items()
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Node Status</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    margin: 0; padding: 24px; background: #f3f4f6; color: #111827; font-size: 14px;
  }}
  h1 {{ margin: 0 0 4px; font-size: 1.6em; color: #111827; }}
  .ts {{ color: #6b7280; font-size: 0.85em; margin-bottom: 22px; }}
  .summary {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 26px; }}
  details > summary::-webkit-details-marker {{ display: none; }}
  details[open] > summary > span:first-child {{ /* no-op, handled per-element */ }}
</style>
</head>
<body>
<h1>Node Status</h1>
<div class="ts">Generated {generated_at}</div>
<div class="summary">{summary_html}</div>
{partitions_html}
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(
        description="Generate node-status HTML from sinfo"
    )
    parser.add_argument(
        "-o", "--output",
        default=str(Path(__file__).parent / "index.html"),
        help="Output HTML path (default: index.html next to this script)",
    )
    parser.add_argument(
        "--csv",
        default=str(Path(__file__).parent / "nodeStatus_history.csv"),
        help="History CSV path (default: nodeStatus_history.csv next to this script)",
    )
    parser.add_argument(
        "--no-csv", action="store_true",
        help="Skip appending to the history CSV",
    )
    args = parser.parse_args()

    lines = get_sinfo_lines()
    data = parse_sinfo(lines)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not args.no_csv:
        append_history(data, generated_at, args.csv)
        print(f"History appended to {args.csv}")

    html = generate_html(data, generated_at)
    out_path = args.output
    with open(out_path, "w") as f:
        f.write(html)

    print(f"Written to {out_path}")


if __name__ == "__main__":
    main()
