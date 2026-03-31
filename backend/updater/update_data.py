#!/usr/bin/env python3
"""
Cloud Run Job: pulls recent NBA game data via nba_api and updates Parquet files on GCS.

Runs daily Oct–Jun (after each game day) and weekly Jul–Sep.

Tables updated:
  - player_game_logs         — append new rows (by date range)
  - player_season_stats      — replace current-season rows
  - team_season_stats        — replace current-season rows
  - game                     — append new games (pivot LeagueGameLog long→wide)
  - common_player_info       — full PlayerIndex refresh + CommonPlayerInfo for new players
  - draft_history            — append recent draft seasons
  - line_score               — fetch BoxScoreSummaryV2 for new game_ids
  - game_summary             — fetch BoxScoreSummaryV2 for new game_ids
  - officials                — fetch BoxScoreSummaryV2 for new game_ids
  - play_by_play             — append PlayByPlayV2 for new game_ids (slow, run separately)

Usage:
    python updater/update_data.py [--season 2024-25] [--days-back 2]
"""

import argparse
import io
import os
import sys
import time
from datetime import date, timedelta

import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "nba-analytics-data-2026")

# stats.nba.com requires browser-like headers or it drops/times out requests.
NBA_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Connection": "keep-alive",
    "Referer": "https://stats.nba.com/",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

# Stat columns in LeagueGameLog that map directly to the game table
_GAME_STAT_COLS = [
    "PTS", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB",
    "AST", "STL", "BLK", "TOV", "PF", "PLUS_MINUS",
]


def season_str(start_year: int) -> str:
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def current_season() -> str:
    today = date.today()
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


# ---------------------------------------------------------------------------
# Player game logs
# ---------------------------------------------------------------------------

def update_player_game_logs(bucket: storage.Bucket, season: str, days_back: int) -> None:
    """Append new player game log rows from the last N days."""
    from nba_api.stats.endpoints import playergamelogs

    date_from = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    print(f"  Fetching player game logs from {date_from}...")

    try:
        logs = playergamelogs.PlayerGameLogs(
            season_nullable=season,
            date_from_nullable=date_from,
            headers=NBA_HEADERS,
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
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["GAME_ID", "PLAYER_ID"], keep="last")
    else:
        combined = new_df

    upload_parquet(bucket, blob_name, combined)
    print(f"  Added {len(new_df):,} new rows (total: {len(combined):,})")


# ---------------------------------------------------------------------------
# Player / team season stats
# ---------------------------------------------------------------------------

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
                headers=NBA_HEADERS,
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
                headers=NBA_HEADERS,
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


# ---------------------------------------------------------------------------
# Game table (team box scores, wide home/away format)
# ---------------------------------------------------------------------------

def _pivot_game_long_to_wide(long_df: pd.DataFrame, season_type: str) -> pd.DataFrame:
    """
    Convert LeagueGameLog long format (one row per team per game) to the wide
    home/away format used in the game table.

    LeagueGameLog MATCHUP examples:
      "LAL vs. GSW"  →  LAL is home
      "GSW @ LAL"    →  GSW is away
    """
    df = long_df.copy()
    df["_is_home"] = df["MATCHUP"].str.contains(" vs\\. ", regex=True)

    home = df[df["_is_home"]].set_index("GAME_ID")
    away = df[~df["_is_home"]].set_index("GAME_ID")

    # Only keep game_ids where both teams are present
    shared_ids = home.index.intersection(away.index)
    if shared_ids.empty:
        return pd.DataFrame()

    home = home.loc[shared_ids]
    away = away.loc[shared_ids]

    wide = pd.DataFrame({"game_id": shared_ids})
    wide["game_date"] = pd.to_datetime(home["GAME_DATE"].values).strftime("%Y-%m-%d")
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

    for col in _GAME_STAT_COLS:
        wide[f"{col.lower()}_home"] = home[col].values
        wide[f"{col.lower()}_away"] = away[col].values

    return wide.reset_index(drop=True)


def update_game_table(bucket: storage.Bucket, season: str, days_back: int) -> None:
    """Append new team box score rows using LeagueGameLog, pivoted to wide home/away format."""
    from nba_api.stats.endpoints import leaguegamelog

    date_from = (date.today() - timedelta(days=days_back)).strftime("%m/%d/%Y")
    print(f"  Fetching game table rows from {date_from}...")

    all_new = []
    for season_type in ["Regular Season", "Playoffs", "PlayIn"]:
        try:
            logs = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star=season_type,
                date_from_nullable=date_from,
                headers=NBA_HEADERS,
                timeout=120,
            )
            df = logs.get_data_frames()[0]
            if not df.empty:
                wide = _pivot_game_long_to_wide(df, season_type)
                if not wide.empty:
                    all_new.append(wide)
                    print(f"    {season_type}: {len(wide)} games")
        except Exception as e:
            print(f"  WARNING ({season_type}): {e}", file=sys.stderr)
        time.sleep(0.6)

    if not all_new:
        print("  No new game rows.")
        return

    new_df = pd.concat(all_new, ignore_index=True)

    blob_name = "parquet/game.parquet"
    existing = download_parquet(bucket, blob_name)
    if existing is not None:
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["game_id"], keep="last")
    else:
        combined = new_df

    upload_parquet(bucket, blob_name, combined)
    print(f"  Added {len(new_df):,} new game rows (total: {len(combined):,})")


# ---------------------------------------------------------------------------
# Common player info
# ---------------------------------------------------------------------------

def update_common_player_info(bucket: storage.Bucket) -> None:
    """
    Refresh common_player_info using PlayerIndex (single bulk call) for current
    roster/team assignments, then fetch CommonPlayerInfo for any player_ids not
    already in the table (to get birthdate, school, etc.).
    """
    from nba_api.stats.endpoints import playerindex, commonplayerinfo

    print("  Fetching PlayerIndex...")
    try:
        idx = playerindex.PlayerIndex(headers=NBA_HEADERS, timeout=60)
        idx_df = idx.get_data_frames()[0]
    except Exception as e:
        print(f"  ERROR fetching PlayerIndex: {e}", file=sys.stderr)
        return

    # Map PlayerIndex columns to common_player_info schema
    idx_df = idx_df.rename(columns={
        "PERSON_ID": "person_id",
        "PLAYER_LAST_NAME": "last_name",
        "PLAYER_FIRST_NAME": "first_name",
        "TEAM_ID": "team_id",
        "TEAM_CITY": "team_city",
        "TEAM_NAME": "team_name",
        "TEAM_ABBREVIATION": "team_abbreviation",
        "JERSEY_NUMBER": "jersey_number",
        "POSITION": "position",
        "HEIGHT": "height",
        "WEIGHT": "weight",
        "COLLEGE": "school",
        "COUNTRY": "country",
        "DRAFT_YEAR": "draft_year",
        "DRAFT_ROUND": "draft_round",
        "DRAFT_NUMBER": "draft_number",
        "FROM_YEAR": "from_year",
        "TO_YEAR": "to_year",
        "ROSTER_STATUS": "roster_status",
        "IS_DEFUNCT": "is_defunct",
    })
    idx_df["display_first_last"] = idx_df["first_name"] + " " + idx_df["last_name"]
    idx_df["person_id"] = idx_df["person_id"].astype(str)

    blob_name = "parquet/common_player_info.parquet"
    existing = download_parquet(bucket, blob_name)

    if existing is not None:
        existing["person_id"] = existing["person_id"].astype(str)
        existing_ids = set(existing["person_id"].astype(str))
        new_ids = set(idx_df["person_id"].astype(str)) - existing_ids
    else:
        existing_ids = set()
        new_ids = set(idx_df["person_id"].astype(str))

    print(f"  {len(new_ids)} new player IDs — fetching CommonPlayerInfo...")

    new_rows = []
    for i, pid in enumerate(sorted(new_ids)):
        try:
            info = commonplayerinfo.CommonPlayerInfo(player_id=pid, headers=NBA_HEADERS, timeout=30)
            row = info.get_data_frames()[0].iloc[0]
            new_rows.append(row.to_dict())
        except Exception as e:
            print(f"  WARNING (player {pid}): {e}", file=sys.stderr)
        if i % 50 == 49:
            print(f"    ...fetched {i + 1}/{len(new_ids)}")
        time.sleep(0.6)

    # Merge: start from existing, update active-player team assignments from idx_df,
    # append truly new players fetched via CommonPlayerInfo.
    if existing is not None:
        # Update current team/roster info for all players in the index
        idx_update_cols = [
            "person_id", "team_id", "team_name", "team_abbreviation",
            "position", "height", "weight", "from_year", "to_year", "roster_status",
        ]
        idx_update = idx_df[[c for c in idx_update_cols if c in idx_df.columns]].copy()
        idx_update["person_id"] = idx_update["person_id"].astype(str)

        merged = existing.merge(
            idx_update, on="person_id", how="left", suffixes=("_old", "")
        )
        # Drop _old columns created by the merge
        old_cols = [c for c in merged.columns if c.endswith("_old")]
        merged = merged.drop(columns=old_cols)
        combined = merged
    else:
        combined = pd.DataFrame()

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        new_df["person_id"] = new_df["person_id"].astype(str) if "person_id" in new_df.columns else new_df.get("PERSON_ID", pd.Series(dtype=str)).astype(str)
        combined = pd.concat([combined, new_df], ignore_index=True) if not combined.empty else new_df

    if combined.empty:
        print("  No data to upload.")
        return

    upload_parquet(bucket, blob_name, combined)
    print(f"  Updated common_player_info ({len(new_rows)} new players added)")


# ---------------------------------------------------------------------------
# Draft history
# ---------------------------------------------------------------------------

def update_draft_history(bucket: storage.Bucket, seasons: list[int] | None = None) -> None:
    """Append missing draft seasons to draft_history."""
    from nba_api.stats.endpoints import drafthistory

    blob_name = "parquet/draft_history.parquet"
    existing = download_parquet(bucket, blob_name)

    if seasons is None:
        # Default: fetch the last 3 draft years
        this_year = date.today().year
        seasons = list(range(this_year - 2, this_year + 1))

    new_rows = []
    for year in seasons:
        season_s = str(year)
        print(f"  Fetching draft history for {season_s}...")
        try:
            dh = drafthistory.DraftHistory(season_year_nullable=season_s, headers=NBA_HEADERS, timeout=60)
            df = dh.get_data_frames()[0]
            if not df.empty:
                new_rows.append(df)
                print(f"    {len(df)} picks")
        except Exception as e:
            print(f"  WARNING (draft {season_s}): {e}", file=sys.stderr)
        time.sleep(0.6)

    if not new_rows:
        print("  No new draft data.")
        return

    new_df = pd.concat(new_rows, ignore_index=True)
    new_df["season"] = new_df["season"].astype(str)
    # Remove any seasons we're refreshing so we can replace them cleanly
    if existing is not None:
        other = existing[~existing["season"].astype(str).isin({str(y) for y in seasons})]
        combined = pd.concat([other, new_df], ignore_index=True)
    else:
        combined = new_df

    upload_parquet(bucket, blob_name, combined)


# ---------------------------------------------------------------------------
# Game details: line_score, game_summary, officials
# ---------------------------------------------------------------------------

def _new_game_ids(bucket: storage.Bucket, days_back: int) -> list[str]:
    """Return game_ids from the game table that are within the last N days."""
    game_df = download_parquet(bucket, "parquet/game.parquet")
    if game_df is None or game_df.empty:
        return []

    cutoff = (date.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    recent = game_df[game_df["game_date"] >= cutoff]
    return list(recent["game_id"].astype(str).unique())


def update_game_details(bucket: storage.Bucket, days_back: int) -> None:
    """
    Fetch BoxScoreSummaryV2 for recent game_ids to refresh line_score,
    game_summary, and officials tables.
    """
    from nba_api.stats.endpoints import boxscoresummaryv2

    game_ids = _new_game_ids(bucket, days_back)
    if not game_ids:
        print("  No recent game_ids found.")
        return

    print(f"  Fetching BoxScoreSummaryV2 for {len(game_ids)} games...")

    line_scores = []
    game_summaries = []
    officials_rows = []

    for i, gid in enumerate(game_ids):
        try:
            summary = boxscoresummaryv2.BoxScoreSummaryV2(game_id=gid, headers=NBA_HEADERS, timeout=30)
            dfs = summary.get_data_frames()
            # Result set indices (BoxScoreSummaryV2):
            #   0: GameSummary, 1: OtherStats, 2: Officials, 3: InactivePlayers,
            #   4: GameInfo, 5: LineScore, 6: LastMeeting, 7: SeasonSeries, 8: AvailableVideo
            if len(dfs) > 5 and not dfs[5].empty:
                ls = dfs[5].copy()
                ls["game_id"] = gid
                line_scores.append(ls)
            if len(dfs) > 0 and not dfs[0].empty:
                gs = dfs[0].copy()
                game_summaries.append(gs)
            if len(dfs) > 2 and not dfs[2].empty:
                of = dfs[2].copy()
                of["game_id"] = gid
                officials_rows.append(of)
        except Exception as e:
            print(f"  WARNING (game {gid}): {e}", file=sys.stderr)
        if i % 50 == 49:
            print(f"    ...processed {i + 1}/{len(game_ids)}")
        time.sleep(0.6)

    def _merge_and_upload(new_rows, blob_name, dedup_col):
        if not new_rows:
            return
        new_df = pd.concat(new_rows, ignore_index=True)
        existing = download_parquet(bucket, blob_name)
        if existing is not None:
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=[dedup_col], keep="last")
        else:
            combined = new_df
        upload_parquet(bucket, blob_name, combined)

    _merge_and_upload(line_scores, "parquet/line_score.parquet", "game_id")
    _merge_and_upload(game_summaries, "parquet/game_summary.parquet", "GAME_ID")
    _merge_and_upload(officials_rows, "parquet/officials.parquet", "official_id")

    print(f"  Updated line_score, game_summary, officials for {len(game_ids)} games")


# ---------------------------------------------------------------------------
# Play-by-play (incremental, slow — run separately or on a reduced schedule)
# ---------------------------------------------------------------------------

def update_play_by_play(bucket: storage.Bucket, days_back: int) -> None:
    """
    Append play-by-play rows for game_ids in the game table that are not yet
    in the play_by_play table.

    This is intentionally limited to recent games (days_back) to avoid the
    multi-hour catch-up cost of fetching thousands of games. Run with a larger
    --days-back value during initial catch-up.

    NOTE: The play_by_play Parquet file is ~500 MB. Each run downloads the full
    file to deduplicate, which takes significant memory. This function is
    excluded from the default daily run; use --include-pbp to enable it.
    """
    from nba_api.stats.endpoints import playbyplayv2

    game_ids = _new_game_ids(bucket, days_back)
    if not game_ids:
        print("  No recent game_ids found.")
        return

    # Find which game_ids are already in play_by_play
    print(f"  Checking play_by_play coverage for {len(game_ids)} games...")
    pbp_blob = "parquet/play_by_play.parquet"
    existing = download_parquet(bucket, pbp_blob)

    if existing is not None:
        existing_ids = set(existing["game_id"].astype(str).unique())
        missing_ids = [g for g in game_ids if str(g) not in existing_ids]
    else:
        existing_ids = set()
        missing_ids = game_ids

    if not missing_ids:
        print("  play_by_play is already up to date.")
        return

    print(f"  Fetching PlayByPlayV2 for {len(missing_ids)} new games...")
    new_rows = []
    for i, gid in enumerate(missing_ids):
        try:
            pbp = playbyplayv2.PlayByPlayV2(game_id=gid, headers=NBA_HEADERS, timeout=60)
            df = pbp.get_data_frames()[0]
            if not df.empty:
                new_rows.append(df)
        except Exception as e:
            print(f"  WARNING (game {gid}): {e}", file=sys.stderr)
        if i % 20 == 19:
            print(f"    ...fetched {i + 1}/{len(missing_ids)}")
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
    print(f"  Added {len(new_df):,} play-by-play rows (total: {len(combined):,})")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Update NBA Parquet data on GCS")
    parser.add_argument("--season", default=None, help="Season string e.g. '2024-25' (default: current)")
    parser.add_argument("--days-back", type=int, default=2, help="Days back for incremental fetches (default: 2)")
    parser.add_argument("--draft-seasons", type=int, nargs="+", default=None,
                        help="Draft year(s) to refresh e.g. --draft-seasons 2023 2024 (default: last 3 years)")
    parser.add_argument("--skip-game-logs", action="store_true")
    parser.add_argument("--skip-player-stats", action="store_true")
    parser.add_argument("--skip-team-stats", action="store_true")
    parser.add_argument("--skip-game-table", action="store_true")
    parser.add_argument("--skip-player-info", action="store_true")
    parser.add_argument("--skip-draft-history", action="store_true")
    parser.add_argument("--skip-game-details", action="store_true",
                        help="Skip line_score / game_summary / officials refresh")
    parser.add_argument("--include-pbp", action="store_true",
                        help="Include play-by-play update (slow — downloads ~500 MB file)")
    args = parser.parse_args()

    season = args.season or current_season()
    print(f"Updating data for season: {season}  (days-back: {args.days_back})")

    client = gcs_client()
    bucket = client.bucket(GCS_BUCKET)

    if not args.skip_game_logs:
        print("\n[player_game_logs]")
        update_player_game_logs(bucket, season, args.days_back)

    if not args.skip_player_stats:
        print("\n[player_season_stats]")
        update_player_season_stats(bucket, season)

    if not args.skip_team_stats:
        print("\n[team_season_stats]")
        update_team_season_stats(bucket, season)

    if not args.skip_game_table:
        print("\n[game]")
        update_game_table(bucket, season, args.days_back)

    if not args.skip_player_info:
        print("\n[common_player_info]")
        update_common_player_info(bucket)

    if not args.skip_draft_history:
        print("\n[draft_history]")
        update_draft_history(bucket, seasons=args.draft_seasons)

    if not args.skip_game_details:
        print("\n[line_score / game_summary / officials]")
        update_game_details(bucket, args.days_back)

    if args.include_pbp:
        print("\n[play_by_play]")
        update_play_by_play(bucket, args.days_back)

    print("\nDone.")


if __name__ == "__main__":
    main()
