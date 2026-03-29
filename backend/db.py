"""
DuckDB connection that reads Parquet files from GCS.
The connection is created once at startup and reused across requests.
"""

import os
import duckdb

_conn: duckdb.DuckDBPyConnection | None = None

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "nba-analytics-data-2026")
# For local dev, point at local parquet dir instead of GCS
LOCAL_PARQUET_DIR = os.getenv("LOCAL_PARQUET_DIR", "")


def init_db() -> None:
    global _conn
    _conn = duckdb.connect(database=":memory:")

    if LOCAL_PARQUET_DIR:
        _register_local(_conn)
    else:
        _register_gcs(_conn)


def _register_gcs(conn: duckdb.DuckDBPyConnection) -> None:
    """Register GCS Parquet files as DuckDB views using the httpfs extension."""
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("SET gcs_base_url = 'https://storage.googleapis.com';")

    tables = _parquet_tables()
    for name, path in tables.items():
        gcs_url = f"gs://{GCS_BUCKET}/parquet/{path}"
        conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{gcs_url}')")

    print(f"[db] Registered {len(tables)} views from GCS bucket: {GCS_BUCKET}")


def _register_local(conn: duckdb.DuckDBPyConnection) -> None:
    """Register local Parquet files as DuckDB views (for local dev)."""
    import pathlib

    base = pathlib.Path(LOCAL_PARQUET_DIR)
    tables = _parquet_tables()
    registered = 0
    for name, path in tables.items():
        full = base / path
        if full.exists():
            conn.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{full}')"
            )
            registered += 1
        else:
            print(f"[db] WARNING: {full} not found, skipping view {name}")

    print(f"[db] Registered {registered}/{len(tables)} views from local dir: {LOCAL_PARQUET_DIR}")


def _parquet_tables() -> dict[str, str]:
    """Map view name → parquet filename."""
    return {
        # From SQLite
        "game":                         "game.parquet",
        "common_player_info":           "common_player_info.parquet",
        "player":                       "player.parquet",
        "draft_history":                "draft_history.parquet",
        "draft_combine_stats":          "draft_combine_stats.parquet",
        "line_score":                   "line_score.parquet",
        "other_stats":                  "other_stats.parquet",
        "game_info":                    "game_info.parquet",
        "game_summary":                 "game_summary.parquet",
        "team":                         "team.parquet",
        "team_details":                 "team_details.parquet",
        "officials":                    "officials.parquet",
        # From nba_api
        "player_season_stats":          "player_season_stats_traditional.parquet",
        "player_season_stats_advanced": "player_season_stats_advanced.parquet",
        "team_season_stats":            "team_season_stats_traditional.parquet",
        "team_season_stats_advanced":   "team_season_stats_advanced.parquet",
        "player_game_logs":             "player_game_logs.parquet",
    }


def get_conn() -> duckdb.DuckDBPyConnection:
    if _conn is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _conn


def run_query(sql: str) -> list[dict]:
    """Execute SQL and return results as a list of dicts."""
    conn = get_conn()
    rel = conn.execute(sql)
    columns = [desc[0] for desc in rel.description]
    rows = rel.fetchall()
    return [dict(zip(columns, row)) for row in rows]
