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

_LABEL_MAP = {
    "PLAYER_NAME": "Player", "PLAYER_ID": "Player ID",
    "TEAM_NAME": "Team", "TEAM_ABBREVIATION": "Team",
    "season": "Season", "GAME_DATE": "Date",
    "GP": "Games", "MIN": "Minutes",
    "PTS": "Points", "FGM": "FG Made", "FGA": "FG Att.",
    "FG_PCT": "FG%", "FG3M": "3P Made", "FG3A": "3P Att.", "FG3_PCT": "3PT%",
    "FTM": "FT Made", "FTA": "FT Att.", "FT_PCT": "FT%",
    "REB": "Rebounds", "OREB": "Off Reb", "DREB": "Def Reb",
    "AST": "Assists", "STL": "Steals", "BLK": "Blocks",
    "TOV": "Turnovers", "PF": "Fouls", "PLUS_MINUS": "+/-",
    "NET_RATING": "Net Rating", "OFF_RATING": "Off Rating", "DEF_RATING": "Def Rating",
    "E_NET_RATING": "Est. Net Rating",
    "TS_PCT": "True Shooting %", "USG_PCT": "Usage %", "EFG_PCT": "eFG%",
    "AST_PCT": "Assist %", "REB_PCT": "Reb %", "PIE": "PIE",
    "W_PCT": "Win %", "W": "Wins", "L": "Losses",
    "pts_per36": "Pts / 36 Min", "ast_per36": "Ast / 36 Min",
    "reb_per36": "Reb / 36 Min", "blk_per36": "Blk / 36 Min",
    "stl_per36": "Stl / 36 Min",
    "avg_pts": "Avg Points", "avg_ast": "Avg Assists", "avg_reb": "Avg Rebounds",
    "total_min": "Total Minutes", "total_pts": "Total Points",
    "total_gp": "Total Games",
}


def _pretty_label(col: str | None) -> str:
    """Convert a raw column name to a human-readable axis label."""
    if not col:
        return ""
    if col in _LABEL_MAP:
        return _LABEL_MAP[col]
    return col.replace("_", " ").title()


def _data_footnote(df: pd.DataFrame) -> str:
    """
    Build a short data-availability note for chart annotations.

    If the dataframe has a 'season' column with YYYY-YY strings, derive the
    actual range from the data.  Otherwise fall back to known source ranges.
    """
    import re
    if "season" in df.columns:
        seasons = df["season"].dropna().astype(str)
        valid = [s for s in seasons if re.match(r"^\d{4}-\d{2}$", s)]
        if valid:
            first = min(valid)
            last = max(valid)
            end_year = int(last[:4]) + 1
            return f"Data: {first[:4]}–{end_year} · Source: NBA Stats API / Kaggle"
    # Fallback: player/team season stats coverage
    return "Player/team season stats: 1996–2025 · Game records: 1946–2025 · Source: NBA Stats API / Kaggle"




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


def _layout(spec: ChartSpec, df: pd.DataFrame | None = None, **overrides) -> dict:
    y_spec = spec.get("y")
    title_text = spec.get("title", "")
    if df is not None:
        note = _data_footnote(df)
        # Embed footnote as a subtitle line so it's never clipped by margins
        title_text = f"{title_text}<br><sup style='color:#666688'>{note}</sup>"
    return {
        "title": {"text": title_text, "x": 0.5},
        "template": PLOTLY_TEMPLATE,
        "height": DEFAULT_HEIGHT,
        "margin": {"l": 50, "r": 20, "t": 65, "b": 50},
        "xaxis": {"title": _pretty_label(spec.get("x") or "")},
        "yaxis": {"title": _pretty_label(y_spec) if isinstance(y_spec, str) else ""},
        **overrides,
    }


def _is_season_col(series: pd.Series) -> bool:
    """Return True if the column contains NBA season strings like '2009-10'."""
    import re
    sample = series.dropna().astype(str).head(5)
    return all(re.match(r"^\d{4}-\d{2}$", v) for v in sample) if len(sample) > 0 else False


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

    # Use horizontal bars when x-axis has string labels (names) to avoid cutoff
    use_horizontal = x and df[x].dtype == object and len(df) >= 5

    data = []
    if color_col and color_col in df.columns:
        for group, gdf in df.groupby(color_col):
            for y_col in y_cols:
                trace = {"type": "bar", "name": str(group)}
                if use_horizontal:
                    trace["y"] = gdf[x].tolist()
                    trace["x"] = gdf[y_col].tolist()
                    trace["orientation"] = "h"
                else:
                    trace["x"] = gdf[x].tolist()
                    trace["y"] = gdf[y_col].tolist()
                data.append(trace)
    else:
        for y_col in y_cols:
            trace = {"type": "bar", "name": y_col}
            if use_horizontal:
                trace["y"] = df[x].tolist()
                trace["x"] = df[y_col].tolist()
                trace["orientation"] = "h"
            else:
                trace["x"] = df[x].tolist()
                trace["y"] = df[y_col].tolist()
            data.append(trace)

    if use_horizontal:
        n = len(df)
        height = max(DEFAULT_HEIGHT, 40 * n + 80)
        y_spec = spec.get("y")
        layout = _layout(spec, df, barmode="group",
                         height=height,
                         margin={"l": 160, "r": 20, "t": 65, "b": 50},
                         yaxis={"title": "", "autorange": "reversed"},
                         xaxis={"title": _pretty_label(y_spec) if isinstance(y_spec, str) else ""})
    else:
        layout = _layout(spec, df, barmode="group",
                         xaxis={"title": _pretty_label(spec.get("x") or ""), "tickangle": -35,
                                 "automargin": True})
    return {"data": data, "layout": layout}


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
    # Force categorical axis for season strings (e.g. "2009-10") so Plotly
    # doesn't misparse them as dates and space them unevenly.
    # Also set categoryorder so multi-series charts (where groupby produces
    # traces in alphabetical player order) don't append missing seasons to the
    # end of the axis instead of placing them chronologically.
    xaxis_override = {"title": _pretty_label(spec.get("x") or "")}
    if x and _is_season_col(df[x]):
        xaxis_override["type"] = "category"
        xaxis_override["categoryorder"] = "category ascending"
    return {"data": data, "layout": _layout(spec, df, xaxis=xaxis_override)}


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
    return {"data": data, "layout": _layout(spec, df)}


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
    return {"data": data, "layout": _layout(spec, df)}


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
    layout = _layout(spec, df)
    layout["polar"] = {"radialaxis": {"visible": True}}
    return {"data": data, "layout": layout}


def _histogram(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    col = x or (y_cols[0] if y_cols else df.columns[0])
    data = [{"type": "histogram", "x": df[col].dropna().tolist(), "name": col}]
    return {"data": data, "layout": _layout(spec, df)}


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
    return {"data": data, "layout": _layout(spec, df)}


def _pie(df: pd.DataFrame, spec: ChartSpec) -> dict:
    x, y_cols = _resolve_columns(df, spec)
    labels = df[x].tolist() if x else list(range(len(df)))
    values = df[y_cols[0]].tolist() if y_cols else [1] * len(df)
    data = [{"type": "pie", "labels": labels, "values": values, "textinfo": "label+percent"}]
    layout = _layout(spec, df)
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
    layout = _layout(spec, df)
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
