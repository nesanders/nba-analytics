# NBA Analytics

A chat web app for NBA statistics. Ask natural language questions and get answers with
AI-generated SQL queries and inline Plotly visualizations.

**Live:** [nsanders.me/nba-analytics](https://nsanders.me/nba-analytics/)

## Example queries

- *Who led the league in assists in 2003-04?*
- *Show LeBron James's scoring trend over his career*
- *Compare Curry and Thompson's 3PT% by season*
- *Which teams had the best net rating in 2022-23?*
- *Show a shot chart for Kobe Bryant in 2005-06*

---

## Architecture

```
Browser (React + Vite + Plotly.js)        GitHub Pages
    │  POST /chat  {message, history}
    │  X-Groq-Key: <user's key>  OR  X-Gemini-Token: <operator token>
    ▼
FastAPI (Python)                          Google Cloud Run (scale-to-zero)
    │  ├── Groq API (llama-3.3-70b) — user-supplied key forwarded at request time
    │  ├── Gemini 2.5 Flash — server-held key, gated by GEMINI_AUTH_TOKEN env var
    │  ├── executes SQL via DuckDB
    │  └── builds Plotly figure spec
    │
    ├── DuckDB                            reads Parquet files from GCS via httpfs at query time
    └── Google Cloud Storage             Parquet files (~18 MB, 17 tables)
         ▲
    Cloud Run Job + Cloud Scheduler      daily updater (nba_api → GCS)
```

**Key design decisions:**

| Concern | Decision |
|---|---|
| LLM provider | Groq (user-supplied free key) or Gemini 2.5 Flash (server-side key, by request) |
| LLM API key | Groq key stays in `localStorage`, never stored on server. Gemini key is server-only. |
| Data queries | DuckDB reads Parquet files directly from GCS via httpfs (HMAC auth); no download at startup |
| Text accuracy | Two-stage LLM: stage 1 generates SQL, stage 2 writes response text from actual results |
| SQL errors | Automatic retry with corrected prompt, up to 3 attempts |
| Chart rendering | Plotly.js, figure spec built server-side and passed as JSON; charts/SQL shown in right-hand artifact tray |
| Shot charts | Fetched on-demand from `stats.nba.com` via `nba_api`; not pre-cached |
| Rate limiting | 20 req/min on `/chat`, 10 req/min on `/shot_chart` (per IP, via slowapi) |

---

## Using the app

### Groq (free, no account required on our end)

1. Get a free Groq API key at [console.groq.com](https://console.groq.com/keys)
2. Open the app — click the **⚙ Change** button in the top-right at any time to switch providers or update your key
3. Select **Groq** and paste your key — it's saved in your browser only, never sent to our servers
4. Ask any NBA statistics question in plain English

### Gemini 2.5 Flash

1. Select the **Gemini 2.5 Flash** tab in the modal
2. Enter the access token (available by request from the app author)
3. The actual Gemini API key is server-side and never exposed

Charts and SQL appear in the **artifact tray** on the right, numbered to match each response.
Click **⤢** on a card to open a full-screen view. Click **▸/▾** to collapse/expand cards.
Click "↗ Artifact #N" links in responses to scroll to and highlight a chart.

---

## Data coverage

### Tables updated automatically (Cloud Scheduler daily/weekly)

| Table | Source | Historical start | Current season |
|---|---|---|---|
| `player_season_stats` | nba_api | **1996-97** | ✅ Current |
| `player_season_stats_advanced` | nba_api | **1996-97** | ✅ Current |
| `team_season_stats` | nba_api | **1996-97** | ✅ Current |
| `team_season_stats_advanced` | nba_api | **1996-97** | ✅ Current |
| `player_game_logs` | nba_api | **1996-97** | ✅ Current |

### Tables from static Kaggle snapshot (wyattowalsh/basketball, ~early 2023)

These were loaded once at init time and are **never refreshed** by the updater.

| Table | Historical start | Stale since |
|---|---|---|
| `game` (team box scores) | 1946-47 | ⚠️ ~early 2023 |
| `play_by_play` | ~1946 | ⚠️ ~early 2023 |
| `line_score` | ~1946 | ⚠️ ~early 2023 |
| `game_summary` | ~1946 | ⚠️ ~early 2023 |
| `game_info`, `other_stats` | ~1946 | ⚠️ ~early 2023 |
| `officials` | ~1946 | ⚠️ ~early 2023 |
| `common_player_info` | All-time | ⚠️ ~early 2023 |
| `draft_history` | 1947 | ⚠️ ~2022 draft |
| `player`, `team` | All-time | ⚠️ ~early 2023 |

### Historical stat-tracking gaps in the `game` table

Even for seasons it does cover, the `game` table reflects what was tracked at the time:

| Stat | Available from |
|---|---|
| Points, FG, FT | 1946-47 |
| Rebounds (total) | 1950-51 |
| Assists | 1946-47 |
| Offensive/defensive rebounds (split) | ~1973-74 |
| Blocks, steals | **1973-74** |
| 3-point field goals | **1979-80** |
| Advanced stats (TS%, eFG%, etc.) | **1996-97** (nba_api only) |

---

## Known limitations

- **Pre-1996 player stats not available.** The NBA Stats API only provides player/team
  per-game stats from 1996-97 onward. Questions about Wilt Chamberlain, Oscar Robertson,
  Magic Johnson, Larry Bird, etc. will return no results from those tables.

- **Blocks and steals not tracked before 1973-74.** The `game` table goes back to 1946,
  but `blk_home/away` and `stl_home/away` are NULL/0 for earlier seasons.

- **3-point line introduced 1979-80.** `fg3m`, `fg3a`, `fg3_pct` columns are 0/NULL
  for games before that season in the `game` table.

- **Shot charts are on-demand, not cached.** The `/shot_chart` endpoint calls
  `stats.nba.com` in real time. If the NBA API is slow or rate-limits the request,
  shot chart queries will be slow or fail.

- **Play-by-play queries can be slow.** The table has 13.5M rows read via httpfs from
  GCS. Always filter by `game_id`; open-ended aggregations over the full table will be slow.

- **LLM SQL accuracy.** Complex multi-table queries or ambiguous questions may produce
  incorrect SQL. The generated SQL is always shown so you can verify it.

---

## Local development

### Prerequisites

- Python 3.11, conda env `nba-analytics` (see `backend/requirements.txt`)
- Node 22
- A Groq API key

### Backend

```bash
conda activate nba-analytics

export LOCAL_PARQUET_DIR=./data/parquet
export GCS_BUCKET_NAME=<your-bucket>
export ALLOWED_ORIGIN=http://localhost:5173

cd backend
uvicorn main:app --reload --port 8080
```

### Frontend

```bash
cd frontend
cp ../.env.example .env.local
# Set VITE_API_URL=http://localhost:8080 in .env.local
npm install
npm run dev
```

### Initialising the data

To download the dataset and upload to GCS from scratch:

```bash
# 1. Download the Kaggle dataset to data/nba.sqlite
conda activate nba-analytics
kaggle datasets download wyattowalsh/basketball -p data/

# 2. Convert to Parquet and fetch player/team stats from nba_api
python scripts/init_gcs.py --bucket <your-gcs-bucket>
```

`init_gcs.py` flags: `--skip-sqlite`, `--skip-player-stats`, `--skip-team-stats`,
`--skip-game-logs`, `--skip-upload` for partial re-runs.

---

## TODO

### Data freshness (Kaggle snapshot tables are stale since ~early 2023)

- [ ] **`game` table updater** — highest priority; team box scores post-2023 are missing.
      Add `update_game_table()` to `updater/update_data.py` using `LeagueGameLog` from
      nba_api; pivot the long (one-row-per-team) format into the wide home/away schema.
- [ ] **`common_player_info` refresh** — player bios, birthdates, and draft info are stale.
      Used for age-matched queries and position filtering. Refreshable via nba_api
      `CommonPlayerInfo` or `PlayerIndex`.
- [ ] **`draft_history` refresh** — missing 2023+ draft classes. Fetchable via nba_api
      `DraftHistory`.
- [ ] **`line_score`, `game_summary`, `officials` refresh** — quarter scores and game
      metadata are stale. These can be fetched per game_id from nba_api after updating
      the `game` table.
- [ ] **`play_by_play` incremental update** — 13.5M rows makes a full re-load impractical.
      Would need per-game-id fetching via nba_api `PlayByPlayV2`, then append + dedup.

### Historical coverage gaps

- [ ] **Pre-1996 player stats** — NBA Stats API only goes back to 1996-97. Full historical
      player per-game stats (Bird, Magic, Wilt, etc.) would require scraping Basketball
      Reference. Large effort; no clean API exists.
- [ ] **Pre-1973 blocks/steals in `game` table** — these stats weren't tracked before
      1973-74; no data source exists to backfill them.

### Performance

- [ ] **Shot chart caching** — pre-fetch popular player/season shot data in GCS so the
      first request isn't slow (currently hits stats.nba.com on-demand).
- [ ] **Play-by-play pre-aggregations** — clutch stats, lineup stats, etc. as summary
      Parquet files to avoid full 13.5M-row scans.
