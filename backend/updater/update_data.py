#!/usr/bin/env python3
"""
Cloud Run Job: pulls recent NBA game data via nba_api and updates Parquet files on GCS.

Runs daily Oct–Jun (after each game day) and weekly Jul–Sep.
Appends new rows to player_game_logs and player_season_stats; replaces team_season_stats.

Usage:
    python updater/update_data.py [--season 2024-25] [--days-back 2]
"""

import argparse
import io
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "nba-analytics-data-2026")


def season_str(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def current_season() -> str:
    today = date.today()
    # NBA season starts in October; if before October use prior year
    start_year = today.year if today.month >= 10 else today.year - 1
    return season_str(start_year)


def gcs_client() -> storage.Client:
    return storage.Client()


def download_parquet(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame | None:
    """Download a Parquet file from GCS into a DataFrame, or return None if not found."""
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    data = blob.download_as_bytes()
    return pd.read_parquet(io.BytesIO(data))


def upload_parquet(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)
    bucket.blob(blob_name).upload_from_file(buf, content_type="application/octet-stream")
    print(f"  Uploaded {blob_name} ({len(df):,} rows)")


def update_player_game_logs(bucket: storage.Bucket, season: str, days_back: int) -> None:
    """Append new player game log rows from the last N days."""
    from nba_api.stats.endpoints import playergamelogs

    date_from = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    print(f"  Fetching player game logs from {date_from}...")

    try:
        logs = playergamelogs.PlayerGameLogs(
            season_nullable=season,
            date_from_nullable=date_from,
            timeout=120,
        )
        new_df = logs.get_data_frames()[0]
        new_df["season"] = season
    except Exception as e:
        print(f"  ERROR fetching game logs: {e}", file=sys.stderr)
        return

    if new_df.empty:
        print("  No new game log rows.")
        return

    blob_name = "parquet/player_game_logs.parquet"
    existing = download_parquet(bucket, blob_name)

    if existing is not None:
        # Deduplicate on GAME_ID + PLAYER_ID
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["GAME_ID", "PLAYER_ID"], keep="last")
    else:
        combined = new_df

    upload_parquet(bucket, blob_name, combined)
    print(f"  Added {len(new_df):,} new rows (total: {len(combined):,})")


def update_player_season_stats(bucket: storage.Bucket, season: str) -> None:
    """Refresh player season stats (averages update with each game)."""
    from nba_api.stats.endpoints import leaguedashplayerstats

    for measure_type, blob_suffix in [("Base", "traditional"), ("Advanced", "advanced")]:
        blob_name = f"parquet/player_season_stats_{blob_suffix}.parquet"
        print(f"  Fetching player season stats ({measure_type}) for {season}...")
        try:
            stats = leaguedashplayerstats.LeagueDashPlayerStats(
                season=season,
                measure_type_detailed_defense=measure_type,
                per_mode_detailed="PerGame",
                timeout=60,
            )
            new_df = stats.get_data_frames()[0]
            new_df["season"] = season
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            time.sleep(1)
            continue

        existing = download_parquet(bucket, blob_name)
        if existing is not None:
            # Replace rows for this season, keep all other seasons
            other_seasons = existing[existing["season"] != season]
            combined = pd.concat([other_seasons, new_df], ignore_index=True)
        else:
            combined = new_df

        upload_parquet(bucket, blob_name, combined)
        time.sleep(0.6)


def update_team_season_stats(bucket: storage.Bucket, season: str) -> None:
    """Refresh team season stats."""
    from nba_api.stats.endpoints import leaguedashteamstats

    for measure_type, blob_suffix in [("Base", "traditional"), ("Advanced", "advanced")]:
        blob_name = f"parquet/team_season_stats_{blob_suffix}.parquet"
        print(f"  Fetching team season stats ({measure_type}) for {season}...")
        try:
            stats = leaguedashteamstats.LeagueDashTeamStats(
                season=season,
                measure_type_detailed_defense=measure_type,
                per_mode_detailed="PerGame",
                timeout=60,
            )
            new_df = stats.get_data_frames()[0]
            new_df["season"] = season
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            time.sleep(1)
            continue

        existing = download_parquet(bucket, blob_name)
        if existing is not None:
            other_seasons = existing[existing["season"] != season]
            combined = pd.concat([other_seasons, new_df], ignore_index=True)
        else:
            combined = new_df

        upload_parquet(bucket, blob_name, combined)
        time.sleep(0.6)


def main():
    parser = argparse.ArgumentParser(description="Update NBA Parquet data on GCS")
    parser.add_argument("--season", default=None, help="Season string e.g. '2024-25' (default: current)")
    parser.add_argument("--days-back", type=int, default=2, help="Days back for game log fetch (default: 2)")
    parser.add_argument("--skip-game-logs", action="store_true")
    parser.add_argument("--skip-player-stats", action="store_true")
    parser.add_argument("--skip-team-stats", action="store_true")
    args = parser.parse_args()

    season = args.season or current_season()
    print(f"Updating data for season: {season}")

    client = gcs_client()
    bucket = client.bucket(GCS_BUCKET)

    if not args.skip_game_logs:
        update_player_game_logs(bucket, season, args.days_back)

    if not args.skip_player_stats:
        update_player_season_stats(bucket, season)

    if not args.skip_team_stats:
        update_team_season_stats(bucket, season)

    print("Done.")


if __name__ == "__main__":
    main()
