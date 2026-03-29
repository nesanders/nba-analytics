"""
Converts SQL query results + LLM chart spec → Plotly figure dict.

The figure dict is returned to the frontend and passed directly to:
    Plotly.react(div, figure.data, figure.layout)
"""

from __future__ import annotations

from typing import Any

import pandas as pd


ChartSpec = dict[str, Any]
Rows = list[dict[str, Any]]


PLOTLY_TEMPLATE = "plotly_dark"
DEFAULT_HEIGHT = 420


def build_figure(rows: Rows, spec: ChartSpec) -> dict | None:
    """Build a Plotly figure dict from query rows and LLM chart spec."""
    if not rows:
        return None

    df = pd.DataFrame(rows)
    chart_type = (spec.get("type") or "table").lower()

    builders = {
        "bar":       _bar,
        "line":      _line,
        "scatter":   _scatter,
        "bubble":    _bubble,
        "radar":     _radar,
        "histogram": _histogram,
        "box":       _box,
        "pie":       _pie,
        "heatmap":   _heatmap,
        "table":     _table,
    }

    builder = builders.get(chart_type, _table)
    try:
        return builder(df, spec)
    except Exception as e:
        # Fallback to table on any rendering error
        try:
            return _table(df, spec)
        except Exception:
            return None


def _layout(spec: ChartSpec, **overrides) -> dict:
    return {
        "title": {"text": spec.get("title", ""), "x": 0.5},
        "template": PLOTLY_TEMPLATE,
        "height": DEFAULT_HEIGHT,
        "margin": {"l": 50, "r": 20, "t": 50, "b": 50},
        "xaxis": {"title": spec.get("x", "")},
        "yaxis": {"title": spec.get("y", "") if isinstance(spec.get("y"), str) else ""},
        **overrides,
    }


def _resolve_columns(df: pd.DataFrame, spec: ChartSpec) -> tuple[str | None, list[str]]:
    """Return (x_col, [y_cols]) resolving against actual dataframe columns."""
    x = spec.get("x")
    y = spec.get("y")

    # Normalise column names to case-insensitive match
    col_map = {c.lower(): c for c in df.columns}
    x = col_map.get((x or "").lower(), x)

    if isinstance(y, list):
        y_cols = [col_map.get(c.lower(), c) for c in y]
    elif y:
        y_cols = [col_map.get(y.lower(), y)]
    else:
        # Guess: first numeric column that isn't x
        numeric = df.select_dtypes("number").columns.tolist()
        y_cols = [c for c in numeric if c != x][:1]

    return x, y_cols


def _bar(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    color_col = spec.get("color")
    data = []
    if color_col and color_col in df.columns:
        for group, gdf in df.groupby(color_col):
            for y_col in y_cols:
                data.append({"type": "bar", "name": str(group), "x": gdf[x].tolist(), "y": gdf[y_col].tolist()})
    else:
        for y_col in y_cols:
            data.append({"type": "bar", "name": y_col, "x": df[x].tolist(), "y": df[y_col].tolist()})
    return {"data": data, "layout": _layout(spec, barmode="group")}


def _line(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    color_col = spec.get("color")
    data = []
    if color_col and color_col in df.columns:
        for group, gdf in df.groupby(color_col):
            gdf = gdf.sort_values(x) if x else gdf
            for y_col in y_cols:
                data.append({
                    "type": "scatter", "mode": "lines+markers",
                    "name": str(group),
                    "x": gdf[x].tolist(), "y": gdf[y_col].tolist(),
                })
    else:
        df_sorted = df.sort_values(x) if x else df
        for y_col in y_cols:
            data.append({
                "type": "scatter", "mode": "lines+markers",
                "name": y_col,
                "x": df_sorted[x].tolist(), "y": df_sorted[y_col].tolist(),
            })
    return {"data": data, "layout": _layout(spec)}


def _scatter(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    color_col = spec.get("color")
    text_col = _find_col(df, ["PLAYER_NAME", "player_name", "TEAM_NAME", "team_name"])
    data = []
    for y_col in y_cols:
        trace: dict = {
            "type": "scatter", "mode": "markers",
            "x": df[x].tolist(), "y": df[y_col].tolist(),
            "name": y_col,
        }
        if text_col:
            trace["text"] = df[text_col].tolist()
            trace["hovertemplate"] = "%{text}<br>x=%{x}<br>y=%{y}<extra></extra>"
        if color_col and color_col in df.columns:
            trace["marker"] = {"color": df[color_col].tolist(), "showscale": True}
        data.append(trace)
    return {"data": data, "layout": _layout(spec)}


def _bubble(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    size_col = spec.get("size")
    text_col = _find_col(df, ["PLAYER_NAME", "player_name", "TEAM_NAME", "team_name"])
    data = []
    for y_col in y_cols:
        trace: dict = {
            "type": "scatter", "mode": "markers",
            "x": df[x].tolist(), "y": df[y_col].tolist(),
        }
        if size_col and size_col in df.columns:
            sizes = df[size_col].fillna(0).tolist()
            max_s = max(sizes) or 1
            trace["marker"] = {"size": [max(4, 40 * s / max_s) for s in sizes], "sizemode": "diameter"}
        if text_col:
            trace["text"] = df[text_col].tolist()
        data.append(trace)
    return {"data": data, "layout": _layout(spec)}


def _radar(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    # x = category column, y_cols = value columns, or rows = players
    if x and len(y_cols) > 1:
        # Multiple metrics per entity
        name_col = _find_col(df, ["PLAYER_NAME", "player_name", "TEAM_NAME", "team_name"])
        categories = y_cols
        data = []
        for _, row in df.iterrows():
            vals = [row.get(c, 0) for c in categories]
            data.append({
                "type": "scatterpolar", "fill": "toself",
                "name": str(row[name_col]) if name_col else "Series",
                "r": vals + [vals[0]],
                "theta": categories + [categories[0]],
            })
    else:
        categories = df[x].tolist() if x else list(range(len(df)))
        data = []
        for y_col in y_cols:
            vals = df[y_col].tolist()
            data.append({
                "type": "scatterpolar", "fill": "toself",
                "name": y_col, "r": vals + [vals[0]], "theta": categories + [categories[0]],
            })
    layout = _layout(spec)
    layout["polar"] = {"radialaxis": {"visible": True}}
    return {"data": data, "layout": layout}


def _histogram(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    col = x or (y_cols[0] if y_cols else df.columns[0])
    data = [{"type": "histogram", "x": df[col].dropna().tolist(), "name": col}]
    return {"data": data, "layout": _layout(spec)}


def _box(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    data = []
    if x and y_cols:
        for group in df[x].unique():
            gdf = df[df[x] == group]
            for y_col in y_cols:
                data.append({"type": "box", "name": str(group), "y": gdf[y_col].dropna().tolist()})
    else:
        for y_col in y_cols:
            data.append({"type": "box", "name": y_col, "y": df[y_col].dropna().tolist()})
    return {"data": data, "layout": _layout(spec)}


def _pie(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    labels = df[x].tolist() if x else list(range(len(df)))
    values = df[y_cols[0]].tolist() if y_cols else [1] * len(df)
    data = [{"type": "pie", "labels": labels, "values": values, "textinfo": "label+percent"}]
    layout = _layout(spec)
    layout.pop("xaxis", None)
    layout.pop("yaxis", None)
    return {"data": data, "layout": layout}


def _heatmap(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    y_ax = spec.get("y") if isinstance(spec.get("y"), str) else (y_cols[0] if y_cols else None)
    color_col = spec.get("color") or (y_cols[0] if y_cols else None)

    if x and y_ax and color_col:
        pivot = df.pivot_table(index=y_ax, columns=x, values=color_col, aggfunc="mean")
        data = [{
            "type": "heatmap",
            "z": pivot.values.tolist(),
            "x": pivot.columns.tolist(),
            "y": pivot.index.tolist(),
            "colorscale": "RdYlGn",
        }]
    else:
        numeric = df.select_dtypes("number")
        data = [{"type": "heatmap", "z": numeric.values.tolist(),
                 "x": numeric.columns.tolist(), "colorscale": "RdYlGn"}]
    layout = _layout(spec)
    layout.pop("xaxis", None)
    layout.pop("yaxis", None)
    return {"data": data, "layout": layout}


def _table(df: pd.DataFrame, spec: ChartSpec) -> dict:
    # Limit display columns for readability
    display_df = df.head(50)
    data = [{
        "type": "table",
        "header": {
            "values": list(display_df.columns),
            "fill": {"color": "#1a1a2e"},
            "font": {"color": "white", "size": 12},
            "align": "left",
        },
        "cells": {
            "values": [display_df[c].tolist() for c in display_df.columns],
            "fill": {"color": ["#16213e", "#0f3460"] * (len(display_df.columns) // 2 + 1)},
            "font": {"color": "white", "size": 11},
            "align": "left",
        },
    }]
    layout = {
        "title": {"text": spec.get("title", ""), "x": 0.5},
        "template": PLOTLY_TEMPLATE,
        "height": min(600, 80 + 30 * len(display_df)),
        "margin": {"l": 10, "r": 10, "t": 50, "b": 10},
    }
    return {"data": data, "layout": layout}


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None
