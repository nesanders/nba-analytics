"""
DuckDB connection that reads Parquet files.

On Cloud Run: uses DuckDB's httpfs extension with GCS HMAC keys to register
gs:// views directly — no download, no startup delay, no memory overhead
proportional to dataset size. Credentials come from GCS_HMAC_KEY_ID and
GCS_HMAC_SECRET env vars (set on the Cloud Run service).

For local dev: set LOCAL_PARQUET_DIR to read from a local directory instead.

Adding a new table: add one entry to _parquet_tables(). If the file is already
on GCS, no other changes are needed — the view is registered automatically.
"""

import os
import pathlib
import duckdb

_conn: duckdb.DuckDBPyConnection | None = None

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "nba-analytics-data-2026")
LOCAL_PARQUET_DIR = os.getenv("LOCAL_PARQUET_DIR", "")
GCS_HMAC_KEY_ID = os.getenv("GCS_HMAC_KEY_ID", "")
GCS_HMAC_SECRET = os.getenv("GCS_HMAC_SECRET", "")


def init_db() -> None:
    global _conn
    _conn = duckdb.connect(database=":memory:")

    if LOCAL_PARQUET_DIR:
        _register_local(_conn, pathlib.Path(LOCAL_PARQUET_DIR))
    else:
        _register_gcs(_conn)


def _register_gcs(conn: duckdb.DuckDBPyConnection) -> None:
    """Register all tables as GCS views using httpfs + HMAC credentials."""
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        CREATE SECRET gcs_hmac (
            TYPE GCS,
            KEY_ID '{GCS_HMAC_KEY_ID}',
            SECRET '{GCS_HMAC_SECRET}'
        );
    """)

    tables = _parquet_tables()
    for name, filename in tables.items():
        url = f"gs://{GCS_BUCKET}/parquet/{filename}"
        conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{url}')")

    print(f"[db] Registered {len(tables)} GCS views from gs://{GCS_BUCKET}/parquet/")


def _register_local(conn: duckdb.DuckDBPyConnection, base: pathlib.Path) -> None:
    """Register Parquet files as views from a local directory (local dev)."""
    tables = _parquet_tables()
    registered = 0
    for name, filename in tables.items():
        full = base / filename
        if full.exists():
            conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{full}')")
            registered += 1
        else:
            print(f"[db] WARNING: {full} not found, skipping view {name}")

    print(f"[db] Registered {registered}/{len(tables)} views from {base}")


def _parquet_tables() -> dict[str, str]:
    """Map DuckDB view name → GCS parquet filename."""
    return {
        # From wyattowalsh/basketball (Kaggle SQLite)
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
        # From nba_api (LeagueDash* endpoints, 1996-97 to present)
        "player_season_stats":          "player_season_stats_traditional.parquet",
        "player_season_stats_advanced": "player_season_stats_advanced.parquet",
        "team_season_stats":            "team_season_stats_traditional.parquet",
        "team_season_stats_advanced":   "team_season_stats_advanced.parquet",
        "player_game_logs":             "player_game_logs.parquet",
        # Large tables — registered as views but only scanned when queried
        "play_by_play":                 "play_by_play.parquet",
        "inactive_players":             "inactive_players.parquet",
    }


def get_conn() -> duckdb.DuckDBPyConnection:
    if _conn is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _conn


def run_query(sql: str) -> list[dict]:
    """Execute SQL and return results as a list of row dicts."""
    conn = get_conn()
    rel = conn.execute(sql)
    columns = [desc[0] for desc in rel.description]
    rows = rel.fetchall()
    return [dict(zip(columns, row)) for row in rows]
