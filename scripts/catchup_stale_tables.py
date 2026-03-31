#!/usr/bin/env python3
"""
One-time catch-up script: backfills stale Kaggle-snapshot tables with data
from ~early 2023 to present using nba_api.

Tables updated:
  game            — full-season fetch for each catch-up season (LeagueGameLog)
  line_score      — BoxScoreSummaryV2 for every game_id not already in table
  game_summary    — same
  officials       — same
  common_player_info — full PlayerIndex refresh + CommonPlayerInfo for new players
  draft_history   — 2023, 2024, 2025 draft classes
  play_by_play    — PlayByPlayV2 per missing game_id (slow; opt-in with --include-pbp)

The Kaggle snapshot covers up to ~early 2023, so 2022-23 (second half),
2023-24, and 2024-25 need to be appended. player_season_stats, team_season_stats,
and player_game_logs are already kept current by the daily updater and are NOT
touched here.

Usage:
    conda activate nba-analytics
    export GCS_BUCKET_NAME=nba-analytics-data-2026

    # Full catch-up (everything except play-by-play)
    python scripts/catchup_stale_tables.py

    # Include play-by-play (slow — ~1 hour per season, downloads ~500 MB file)
    python scripts/catchup_stale_tables.py --include-pbp

    # Skip sections you've already done
    python scripts/catchup_stale_tables.py --skip-game --skip-player-info

Requires:
    - GCS_BUCKET_NAME env var
    - Application default credentials (gcloud auth application-default login)
      OR GOOGLE_APPLICATION_CREDENTIALS pointing to a service account key
"""

import argparse
import io
import os
import sys
import time
from pathlib import Path

import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

# Allow importing from the backend updater
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "backend" / "updater"))

from update_data import (
    download_parquet,
    upload_parquet,
    update_common_player_info,
    update_draft_history,
    NBA_HEADERS,
)

load_dotenv()
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "nba-analytics-data-2026")

# Seasons where game/play-by-play data is stale.
# 2022-23: Kaggle snapshot ends ~Feb 2023; fetch full season to overwrite/dedup.
# 2023-24: entirely missing.
# 2024-25: current season, partially missing.
CATCHUP_SEASONS = ["2022-23", "2023-24", "2024-25"]

# Draft years missing from the Kaggle snapshot
CATCHUP_DRAFT_YEARS = [2023, 2024, 2025]


# ---------------------------------------------------------------------------
# Game table (full-season fetch, no date filter)
# ---------------------------------------------------------------------------

def _pivot_game_long_to_wide(long_df: pd.DataFrame, season_type: str) -> pd.DataFrame:
    """Convert LeagueGameLog long format to wide home/away format."""
    STAT_COLS = [
        "PTS", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
        "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB",
        "AST", "STL", "BLK", "TOV", "PF", "PLUS_MINUS",
    ]
    df = long_df.copy()
    df["_is_home"] = df["MATCHUP"].str.contains(" vs\\. ", regex=True)

    home = df[df["_is_home"]].set_index("GAME_ID")
    away = df[~df["_is_home"]].set_index("GAME_ID")

    shared_ids = home.index.intersection(away.index)
    if shared_ids.empty:
        return pd.DataFrame()

    home = home.loc[shared_ids]
    away = away.loc[shared_ids]

    wide = pd.DataFrame({"game_id": shared_ids})
    wide["game_date"] = pd.to_datetime(home["GAME_DATE"].values).dt.strftime("%Y-%m-%d")
    wide["season_id"] = home["SEASON_ID"].values
    wide["season_type"] = season_type
    wide["team_id_home"] = home["TEAM_ID"].values
    wide["team_abbreviation_home"] = home["TEAM_ABBREVIATION"].values
    wide["team_name_home"] = home["TEAM_NAME"].values
    wide["wl_home"] = home["WL"].values
    wide["team_id_away"] = away["TEAM_ID"].values
    wide["team_abbreviation_away"] = away["TEAM_ABBREVIATION"].values
    wide["team_name_away"] = away["TEAM_NAME"].values
    wide["wl_away"] = away["WL"].values
    for col in STAT_COLS:
        wide[f"{col.lower()}_home"] = home[col].values
        wide[f"{col.lower()}_away"] = away[col].values

    return wide.reset_index(drop=True)


def catchup_game_table(bucket: storage.Bucket) -> None:
    """Fetch full seasons for CATCHUP_SEASONS and merge into game table."""
    from nba_api.stats.endpoints import leaguegamelog

    blob_name = "parquet/game.parquet"
    existing = download_parquet(bucket, blob_name)

    all_new = []
    for season in CATCHUP_SEASONS:
        for season_type in ["Regular Season", "Playoffs", "PlayIn"]:
            print(f"  {season} / {season_type}...")
            try:
                logs = leaguegamelog.LeagueGameLog(
                    season=season,
                    season_type_all_star=season_type,
                    headers=NBA_HEADERS,
                    timeout=120,
                )
                df = logs.get_data_frames()[0]
                if not df.empty:
                    wide = _pivot_game_long_to_wide(df, season_type)
                    if not wide.empty:
                        all_new.append(wide)
                        print(f"    {len(wide)} games")
            except Exception as e:
                print(f"  WARNING ({season} / {season_type}): {e}", file=sys.stderr)
            time.sleep(0.8)

    if not all_new:
        print("  No new game rows fetched.")
        return

    new_df = pd.concat(all_new, ignore_index=True)

    if existing is not None:
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["game_id"], keep="last")
    else:
        combined = new_df

    upload_parquet(bucket, blob_name, combined)
    print(f"  game table: {len(new_df):,} new rows fetched, {len(combined):,} total")


# ---------------------------------------------------------------------------
# Game details: line_score, game_summary, officials
# ---------------------------------------------------------------------------

def _missing_game_ids(bucket: storage.Bucket, detail_blob: str, detail_id_col: str) -> list[str]:
    """Return game_ids present in the game table but absent from a detail table."""
    game_df = download_parquet(bucket, "parquet/game.parquet")
    if game_df is None or game_df.empty:
        print("  game table is empty — run game catch-up first.")
        return []

    # Only care about catch-up seasons
    catchup_season_ids = set()
    for season in CATCHUP_SEASONS:
        start_year = int(season[:4])
        # NBA season_id format: e.g. "22022" for 2022-23 regular season
        # Just filter by game_date range instead
    cutoff_date = "2023-01-01"
    recent_game_ids = set(
        game_df[game_df["game_date"] >= cutoff_date]["game_id"].astype(str).unique()
    )

    detail_df = download_parquet(bucket, detail_blob)
    if detail_df is not None:
        existing_ids = set(detail_df[detail_id_col].astype(str).unique())
    else:
        existing_ids = set()

    missing = sorted(recent_game_ids - existing_ids)
    print(f"  {len(missing)} game_ids missing from {detail_blob} (of {len(recent_game_ids)} since 2023-01-01)")
    return missing


def catchup_game_details(bucket: storage.Bucket) -> None:
    """Fetch BoxScoreSummaryV2 for game_ids missing from line_score, game_summary, officials."""
    from nba_api.stats.endpoints import boxscoresummaryv2

    # Use line_score as the reference — if it's missing a game_id, fetch all three
    missing_ids = _missing_game_ids(bucket, "parquet/line_score.parquet", "GAME_ID")
    if not missing_ids:
        return

    line_scores, game_summaries, officials_rows = [], [], []

    for i, gid in enumerate(missing_ids):
        try:
            summary = boxscoresummaryv2.BoxScoreSummaryV2(game_id=gid, headers=NBA_HEADERS, timeout=30)
            dfs = summary.get_data_frames()
            # Index 0: GameSummary, 2: Officials, 5: LineScore
            if len(dfs) > 5 and not dfs[5].empty:
                ls = dfs[5].copy()
                ls["game_id"] = gid
                line_scores.append(ls)
            if len(dfs) > 0 and not dfs[0].empty:
                game_summaries.append(dfs[0].copy())
            if len(dfs) > 2 and not dfs[2].empty:
                of = dfs[2].copy()
                of["game_id"] = gid
                officials_rows.append(of)
        except Exception as e:
            print(f"  WARNING (game {gid}): {e}", file=sys.stderr)

        if (i + 1) % 50 == 0:
            print(f"  ...{i + 1}/{len(missing_ids)} games processed")
        time.sleep(0.6)

    def _merge_upload(rows, blob_name, dedup_col):
        if not rows:
            return
        new_df = pd.concat(rows, ignore_index=True)
        existing = download_parquet(bucket, blob_name)
        if existing is not None:
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=[dedup_col], keep="last")
        else:
            combined = new_df
        upload_parquet(bucket, blob_name, combined)

    _merge_upload(line_scores, "parquet/line_score.parquet", "game_id")
    _merge_upload(game_summaries, "parquet/game_summary.parquet", "GAME_ID")
    _merge_upload(officials_rows, "parquet/officials.parquet", "official_id")
    print(f"  Updated line_score, game_summary, officials for {len(missing_ids)} games")


# ---------------------------------------------------------------------------
# Play-by-play (slow)
# ---------------------------------------------------------------------------

def catchup_play_by_play(bucket: storage.Bucket) -> None:
    """
    Fetch PlayByPlayV2 for all game_ids since 2023-01-01 not already in
    play_by_play. Downloads the full ~500 MB parquet to dedup.

    Estimated time: ~0.6s/game × ~3600 games = 35–60 min.
    Run this separately and expect it to take a while.
    """
    from nba_api.stats.endpoints import playbyplayv2

    game_df = download_parquet(bucket, "parquet/game.parquet")
    if game_df is None or game_df.empty:
        print("  game table is empty — run game catch-up first.")
        return

    recent_ids = set(
        game_df[game_df["game_date"] >= "2023-01-01"]["game_id"].astype(str).unique()
    )

    pbp_blob = "parquet/play_by_play.parquet"
    existing = download_parquet(bucket, pbp_blob)
    if existing is not None:
        existing_ids = set(existing["game_id"].astype(str).unique())
        missing_ids = sorted(recent_ids - existing_ids)
    else:
        existing_ids = set()
        missing_ids = sorted(recent_ids)

    print(f"  {len(missing_ids)} game_ids missing from play_by_play")
    if not missing_ids:
        return

    new_rows = []
    for i, gid in enumerate(missing_ids):
        try:
            pbp = playbyplayv2.PlayByPlayV2(game_id=gid, headers=NBA_HEADERS, timeout=60)
            df = pbp.get_data_frames()[0]
            if not df.empty:
                new_rows.append(df)
        except Exception as e:
            print(f"  WARNING (game {gid}): {e}", file=sys.stderr)

        if (i + 1) % 20 == 0:
            print(f"  ...{i + 1}/{len(missing_ids)} games fetched")
        time.sleep(0.6)

    if not new_rows:
        print("  No new play-by-play rows.")
        return

    new_df = pd.concat(new_rows, ignore_index=True)
    if existing is not None:
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["game_id", "eventnum"], keep="last")
    else:
        combined = new_df

    upload_parquet(bucket, pbp_blob, combined)
    print(f"  play_by_play: {len(new_df):,} new rows added ({len(combined):,} total)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="One-time catch-up for stale Kaggle-snapshot tables"
    )
    parser.add_argument("--skip-game", action="store_true",
                        help="Skip game table catch-up")
    parser.add_argument("--skip-player-info", action="store_true",
                        help="Skip common_player_info refresh")
    parser.add_argument("--skip-draft-history", action="store_true",
                        help="Skip draft_history refresh")
    parser.add_argument("--skip-game-details", action="store_true",
                        help="Skip line_score / game_summary / officials")
    parser.add_argument("--include-pbp", action="store_true",
                        help="Include play-by-play catch-up (slow, ~1hr/season)")
    args = parser.parse_args()

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    # Step 1: game table — must run before game_details and play_by_play
    if not args.skip_game:
        print("\n[1/5] game table (LeagueGameLog, full seasons)")
        catchup_game_table(bucket)
    else:
        print("\n[1/5] game table — SKIPPED")

    # Step 2: common_player_info — independent
    if not args.skip_player_info:
        print("\n[2/5] common_player_info (PlayerIndex + CommonPlayerInfo for new players)")
        update_common_player_info(bucket)
    else:
        print("\n[2/5] common_player_info — SKIPPED")

    # Step 3: draft_history — independent
    if not args.skip_draft_history:
        print("\n[3/5] draft_history (2023–2025)")
        update_draft_history(bucket, seasons=CATCHUP_DRAFT_YEARS)
    else:
        print("\n[3/5] draft_history — SKIPPED")

    # Step 4: line_score, game_summary, officials — requires game table
    if not args.skip_game_details:
        print("\n[4/5] line_score / game_summary / officials (BoxScoreSummaryV2)")
        catchup_game_details(bucket)
    else:
        print("\n[4/5] game details — SKIPPED")

    # Step 5: play-by-play — requires game table, slow
    if args.include_pbp:
        print("\n[5/5] play_by_play (PlayByPlayV2) — this will take ~1 hour per season")
        catchup_play_by_play(bucket)
    else:
        print("\n[5/5] play_by_play — SKIPPED (use --include-pbp to enable)")

    print("\nCatch-up complete.")


if __name__ == "__main__":
    main()
