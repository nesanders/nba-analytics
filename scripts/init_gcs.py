#!/usr/bin/env python3
"""
One-time init script: converts NBA SQLite dataset to Parquet files and uploads to GCS.
Also pulls player season stats from nba_api for all available seasons.

Usage:
    conda activate nba-analytics
    python scripts/init_gcs.py

Requires:
    - data/nba.sqlite (downloaded from Kaggle wyattowalsh/basketball)
    - GCS_BUCKET_NAME env var (or passed via --bucket)
    - Application default credentials or GOOGLE_APPLICATION_CREDENTIALS
"""

import argparse
import os
import sys
import time
import sqlite3
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
SQLITE_PATH = DATA_DIR / "nba.sqlite"

# Tables to convert directly from SQLite
SQLITE_TABLES = [
    "game",               # team box scores per game (home/away)
    "common_player_info", # player bio/metadata
    "player",             # player name reference
    "draft_history",      # draft picks
    "draft_combine_stats",# combine measurements
    "line_score",         # quarter-by-quarter scores
    "other_stats",        # paint pts, 2nd chance pts, etc.
    "game_info",          # arena, attendance
    "game_summary",       # game metadata, TV info
    "team",               # team reference
    "team_details",       # team details
    "officials",          # game officials
    "play_by_play",       # 13.5M rows — ~500MB parquet, skip if memory-constrained
    "inactive_players",   # players inactive per game
]

# Season range for nba_api pulls (format: "1996-97")
# LeagueDashPlayerStats is available from 1996-97 onward
FIRST_SEASON_YEAR = 1996
# 2024-25 is the current season as of 2025-26
LAST_SEASON_YEAR = 2024


def season_str(start_year: int) -> str:
    """Convert start year to NBA season string, e.g. 1996 → '1996-97'."""
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def sqlite_to_parquet():
    """Convert selected SQLite tables to Parquet files."""
    print(f"\n{'='*60}")
    print("Converting SQLite tables to Parquet")
    print(f"{'='*60}")

    if not SQLITE_PATH.exists():
        print(f"ERROR: SQLite file not found at {SQLITE_PATH}", file=sys.stderr)
        sys.exit(1)

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH)

    for table in SQLITE_TABLES:
        out_path = PARQUET_DIR / f"{table}.parquet"
        print(f"  {table}...", end=" ", flush=True)
        try:
            df = pd.read_sql(f'SELECT * FROM "{table}"', conn)
            df.to_parquet(out_path, index=False, compression="snappy")
            print(f"{len(df):,} rows → {out_path.stat().st_size // 1024:,} KB")
        except Exception as e:
            print(f"FAILED: {e}")

    conn.close()


def fetch_player_season_stats():
    """Pull player season stats from nba_api for all seasons."""
    from nba_api.stats.endpoints import leaguedashplayerstats

    print(f"\n{'='*60}")
    print("Fetching player season stats via nba_api")
    print(f"{'='*60}")

    all_traditional = []
    all_advanced = []

    seasons = [season_str(y) for y in range(FIRST_SEASON_YEAR, LAST_SEASON_YEAR + 1)]

    for season in seasons:
        for measure_type, collector in [
            ("Base", all_traditional),
            ("Advanced", all_advanced),
        ]:
            print(f"  {season} {measure_type}...", end=" ", flush=True)
            try:
                stats = leaguedashplayerstats.LeagueDashPlayerStats(
                    season=season,
                    measure_type_detailed_defense=measure_type,
                    per_mode_detailed="PerGame",
                    timeout=60,
                )
                df = stats.get_data_frames()[0]
                df["season"] = season
                collector.append(df)
                print(f"{len(df):,} players")
            except Exception as e:
                print(f"FAILED: {e}")
            # Be polite to the NBA stats API
            time.sleep(0.6)

    # Combine and save
    if all_traditional:
        df_trad = pd.concat(all_traditional, ignore_index=True)
        out = PARQUET_DIR / "player_season_stats_traditional.parquet"
        df_trad.to_parquet(out, index=False, compression="snappy")
        print(f"\n  Saved traditional: {len(df_trad):,} rows → {out.stat().st_size // 1024:,} KB")

    if all_advanced:
        df_adv = pd.concat(all_advanced, ignore_index=True)
        out = PARQUET_DIR / "player_season_stats_advanced.parquet"
        df_adv.to_parquet(out, index=False, compression="snappy")
        print(f"  Saved advanced:     {len(df_adv):,} rows → {out.stat().st_size // 1024:,} KB")


def fetch_team_season_stats():
    """Pull team season stats from nba_api for all seasons."""
    from nba_api.stats.endpoints import leaguedashteamstats

    print(f"\n{'='*60}")
    print("Fetching team season stats via nba_api")
    print(f"{'='*60}")

    all_traditional = []
    all_advanced = []

    seasons = [season_str(y) for y in range(FIRST_SEASON_YEAR, LAST_SEASON_YEAR + 1)]

    for season in seasons:
        for measure_type, collector in [
            ("Base", all_traditional),
            ("Advanced", all_advanced),
        ]:
            print(f"  {season} {measure_type}...", end=" ", flush=True)
            try:
                stats = leaguedashteamstats.LeagueDashTeamStats(
                    season=season,
                    measure_type_detailed_defense=measure_type,
                    per_mode_detailed="PerGame",
                    timeout=60,
                )
                df = stats.get_data_frames()[0]
                df["season"] = season
                collector.append(df)
                print(f"{len(df):,} teams")
            except Exception as e:
                print(f"FAILED: {e}")
            time.sleep(0.6)

    if all_traditional:
        df_trad = pd.concat(all_traditional, ignore_index=True)
        out = PARQUET_DIR / "team_season_stats_traditional.parquet"
        df_trad.to_parquet(out, index=False, compression="snappy")
        print(f"\n  Saved traditional: {len(df_trad):,} rows → {out.stat().st_size // 1024:,} KB")

    if all_advanced:
        df_adv = pd.concat(all_advanced, ignore_index=True)
        out = PARQUET_DIR / "team_season_stats_advanced.parquet"
        df_adv.to_parquet(out, index=False, compression="snappy")
        print(f"  Saved advanced:     {len(df_adv):,} rows → {out.stat().st_size // 1024:,} KB")


def fetch_player_game_logs_recent():
    """Pull player game logs for recent seasons (last 5) — full history is too large."""
    from nba_api.stats.endpoints import playergamelogs

    print(f"\n{'='*60}")
    print("Fetching player game logs (recent 5 seasons) via nba_api")
    print(f"{'='*60}")

    all_logs = []
    recent_seasons = [season_str(y) for y in range(LAST_SEASON_YEAR - 4, LAST_SEASON_YEAR + 1)]

    for season in recent_seasons:
        print(f"  {season}...", end=" ", flush=True)
        try:
            logs = playergamelogs.PlayerGameLogs(
                season_nullable=season,
                timeout=120,
            )
            df = logs.get_data_frames()[0]
            df["season"] = season
            all_logs.append(df)
            print(f"{len(df):,} rows")
        except Exception as e:
            print(f"FAILED: {e}")
        time.sleep(1.0)

    if all_logs:
        df_logs = pd.concat(all_logs, ignore_index=True)
        out = PARQUET_DIR / "player_game_logs.parquet"
        df_logs.to_parquet(out, index=False, compression="snappy")
        print(f"\n  Saved: {len(df_logs):,} rows → {out.stat().st_size // 1024:,} KB")


def upload_to_gcs(bucket_name: str):
    """Upload all Parquet files to GCS."""
    print(f"\n{'='*60}")
    print(f"Uploading Parquet files to gs://{bucket_name}/parquet/")
    print(f"{'='*60}")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    parquet_files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not parquet_files:
        print("No Parquet files found to upload.", file=sys.stderr)
        return

    for local_path in parquet_files:
        blob_name = f"parquet/{local_path.name}"
        blob = bucket.blob(blob_name)
        size_mb = local_path.stat().st_size / (1024 * 1024)
        print(f"  {local_path.name} ({size_mb:.1f} MB)...", end=" ", flush=True)
        blob.upload_from_filename(str(local_path))
        print("done")

    print(f"\nUploaded {len(parquet_files)} files.")


def main():
    parser = argparse.ArgumentParser(description="Initialize GCS with NBA Parquet data")
    parser.add_argument("--bucket", default=os.getenv("GCS_BUCKET_NAME"), help="GCS bucket name")
    parser.add_argument("--skip-sqlite", action="store_true", help="Skip SQLite conversion")
    parser.add_argument("--skip-player-stats", action="store_true", help="Skip player season stats fetch")
    parser.add_argument("--skip-team-stats", action="store_true", help="Skip team season stats fetch")
    parser.add_argument("--skip-game-logs", action="store_true", help="Skip player game logs fetch")
    parser.add_argument("--skip-upload", action="store_true", help="Skip GCS upload (local only)")
    args = parser.parse_args()

    if not args.skip_sqlite:
        sqlite_to_parquet()

    if not args.skip_player_stats:
        fetch_player_season_stats()

    if not args.skip_team_stats:
        fetch_team_season_stats()

    if not args.skip_game_logs:
        fetch_player_game_logs_recent()

    if not args.skip_upload:
        if not args.bucket:
            print("ERROR: GCS_BUCKET_NAME not set. Use --bucket or set env var.", file=sys.stderr)
            sys.exit(1)
        upload_to_gcs(args.bucket)

    print("\nDone.")


if __name__ == "__main__":
    main()
