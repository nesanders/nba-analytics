"""
Shot chart endpoint: fetches shot data from nba_api on-demand and
builds a Plotly scatter figure overlaid on a basketball court SVG.

Endpoint: GET /shot_chart?player_id=...&season=...&season_type=Regular+Season
"""

from __future__ import annotations

import time
from functools import lru_cache

from fastapi import APIRouter, HTTPException, Query
from nba_api.stats.endpoints import shotchartdetail

router = APIRouter()


@router.get("/shot_chart")
def shot_chart(
    player_id: int = Query(..., description="NBA player ID"),
    season: str = Query(..., description="Season string e.g. '2005-06'"),
    season_type: str = Query("Regular Season", description="Regular Season | Playoffs"),
):
    """Fetch shot chart data for a player/season and return a Plotly figure dict."""
    try:
        data = _fetch_shots(player_id, season, season_type)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"nba_api error: {e}")

    if not data:
        raise HTTPException(status_code=404, detail="No shot data found for this player/season.")

    figure = _build_shot_chart(data, player_id, season)
    return {"figure": figure}


@lru_cache(maxsize=128)
def _fetch_shots(player_id: int, season: str, season_type: str) -> tuple[dict, ...]:
    """Cached wrapper around ShotChartDetail API call."""
    time.sleep(0.6)  # be polite
    result = shotchartdetail.ShotChartDetail(
        team_id=0,
        player_id=player_id,
        season_nullable=season,
        season_type_all_star=season_type,
        context_measure_simple="FGA",
        timeout=30,
    )
    df = result.get_data_frames()[0]
    # Return as tuple of dicts for lru_cache (must be hashable)
    return tuple(df.to_dict("records"))


def _build_shot_chart(shots: tuple[dict, ...], player_id: int, season: str) -> dict:
    """Build Plotly figure with court background SVG and shot scatter plot."""
    made = [s for s in shots if s.get("SHOT_MADE_FLAG") == 1]
    missed = [s for s in shots if s.get("SHOT_MADE_FLAG") == 0]

    data = [
        {
            "type": "scatter",
            "mode": "markers",
            "name": "Made",
            "x": [s["LOC_X"] for s in made],
            "y": [s["LOC_Y"] for s in made],
            "marker": {
                "color": "rgba(0, 200, 100, 0.7)",
                "size": 6,
                "symbol": "circle",
            },
            "hovertemplate": (
                "%{customdata[0]}<br>"
                "%{customdata[1]}<br>"
                "Q%{customdata[2]} %{customdata[3]}<extra></extra>"
            ),
            "customdata": [
                [s.get("ACTION_TYPE", ""), s.get("SHOT_ZONE_BASIC", ""),
                 s.get("PERIOD", ""), s.get("MINUTES_REMAINING", "")]
                for s in made
            ],
        },
        {
            "type": "scatter",
            "mode": "markers",
            "name": "Missed",
            "x": [s["LOC_X"] for s in missed],
            "y": [s["LOC_Y"] for s in missed],
            "marker": {
                "color": "rgba(220, 60, 60, 0.5)",
                "size": 6,
                "symbol": "x",
            },
            "hovertemplate": (
                "%{customdata[0]}<br>"
                "%{customdata[1]}<br>"
                "Q%{customdata[2]} %{customdata[3]}<extra></extra>"
            ),
            "customdata": [
                [s.get("ACTION_TYPE", ""), s.get("SHOT_ZONE_BASIC", ""),
                 s.get("PERIOD", ""), s.get("MINUTES_REMAINING", "")]
                for s in missed
            ],
        },
    ]

    made_count = len(made)
    total = len(shots)
    pct = f"{100 * made_count / total:.1f}" if total else "0.0"

    layout = {
        "title": {
            "text": f"Shot Chart — Season {season}  ({made_count}/{total}, {pct}% FG)",
            "x": 0.5,
        },
        "template": "plotly_dark",
        "height": 520,
        "width": 500,
        "margin": {"l": 10, "r": 10, "t": 50, "b": 10},
        "xaxis": {
            "range": [-260, 260],
            "showgrid": False,
            "zeroline": False,
            "showticklabels": False,
        },
        "yaxis": {
            "range": [-50, 470],
            "showgrid": False,
            "zeroline": False,
            "showticklabels": False,
            "scaleanchor": "x",
            "scaleratio": 1,
        },
        "shapes": _court_shapes(),
        "legend": {"x": 0.8, "y": 0.05},
    }

    return {"data": data, "layout": layout}


def _court_shapes() -> list[dict]:
    """
    Returns Plotly shape definitions that draw an NBA half-court.
    Coordinates match the LOC_X / LOC_Y system used by nba_api
    (origin at basket, units in tenths of a foot, y increases toward half-court).
    """
    line_color = "rgba(180,140,80,0.8)"
    lw = 2

    def rect(x0, y0, x1, y1):
        return {"type": "rect", "x0": x0, "y0": y0, "x1": x1, "y1": y1,
                "line": {"color": line_color, "width": lw}, "fillcolor": "rgba(0,0,0,0)"}

    def circle(cx, cy, r):
        return {"type": "circle", "x0": cx - r, "y0": cy - r, "x1": cx + r, "y1": cy + r,
                "line": {"color": line_color, "width": lw}, "fillcolor": "rgba(0,0,0,0)"}

    def line(x0, y0, x1, y1):
        return {"type": "line", "x0": x0, "y0": y0, "x1": x1, "y1": y1,
                "line": {"color": line_color, "width": lw}}

    return [
        # Court boundary (half court)
        rect(-250, -47.5, 250, 422.5),
        # Basket
        circle(0, 0, 7.5),
        # Backboard
        line(-30, -7.5, 30, -7.5),
        # Paint (key)
        rect(-80, -47.5, 80, 142.5),
        rect(-60, -47.5, 60, 142.5),
        # Free throw circle
        circle(0, 142.5, 60),
        # Restricted area arc
        {"type": "path",
         "path": "M -40 0 A 40 40 0 0 1 40 0",
         "line": {"color": line_color, "width": lw}},
        # Three point arc
        {"type": "path",
         "path": "M -220 -47.5 A 237.5 237.5 0 0 1 220 -47.5",
         "line": {"color": line_color, "width": lw}},
        # Three point lines (corners)
        line(-220, -47.5, -220, 92.5),
        line(220, -47.5, 220, 92.5),
        # Half court circle
        circle(0, 422.5, 60),
        # Center court line
        line(-250, 422.5, 250, 422.5),
    ]
