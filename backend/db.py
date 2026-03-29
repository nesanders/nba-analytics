"""
DuckDB connection that reads Parquet files.

On Cloud Run: downloads Parquet files from GCS at startup into /tmp, then
registers them as local DuckDB views. This avoids httpfs auth complexity
and keeps queries fast after the one-time ~1s download.

For local dev: set LOCAL_PARQUET_DIR to skip GCS entirely.
"""

import os
import pathlib
import tempfile
import duckdb

_conn: duckdb.DuckDBPyConnection | None = None
_parquet_dir: pathlib.Path | None = None

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "nba-analytics-data-2026")
LOCAL_PARQUET_DIR = os.getenv("LOCAL_PARQUET_DIR", "")


def init_db() -> None:
    global _conn, _parquet_dir
    _conn = duckdb.connect(database=":memory:")

    if LOCAL_PARQUET_DIR:
        _parquet_dir = pathlib.Path(LOCAL_PARQUET_DIR)
    else:
        _parquet_dir = _download_from_gcs()

    _register_local(_conn, _parquet_dir)


def _download_from_gcs() -> pathlib.Path:
    """Download all Parquet files from GCS into a temp directory."""
    from google.cloud import storage

    tmp = pathlib.Path(tempfile.mkdtemp(prefix="nba_parquet_"))
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    tables = _parquet_tables()
    downloaded = 0
    for filename in tables.values():
        blob = bucket.blob(f"parquet/{filename}")
        dest = tmp / filename
        if not dest.exists():
            blob.download_to_filename(str(dest))
            downloaded += 1

    print(f"[db] Downloaded {downloaded} Parquet files from gs://{GCS_BUCKET} → {tmp}")
    return tmp


def _register_local(conn: duckdb.DuckDBPyConnection, base: pathlib.Path) -> None:
    """Register Parquet files as DuckDB views from a local directory."""
    tables = _parquet_tables()
    registered = 0
    for name, filename in tables.items():
        full = base / filename
        if full.exists():
            conn.execute(
                f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{full}')"
            )
            registered += 1
        else:
            print(f"[db] WARNING: {full} not found, skipping view {name}")

    print(f"[db] Registered {registered}/{len(tables)} views from {base}")


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
