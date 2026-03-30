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

## Data

| Source | Tables | Coverage |
|---|---|---|
| [wyattowalsh/basketball](https://www.kaggle.com/datasets/wyattowalsh/basketball) (Kaggle) | `game`, `player`, `common_player_info`, `draft_history`, `line_score`, `other_stats`, `game_info`, `game_summary`, `team`, `officials`, `play_by_play`, `inactive_players` | Team box scores and play-by-play 1946–present; player metadata |
| NBA Stats API (`nba_api`) | `player_season_stats`, `player_season_stats_advanced`, `team_season_stats`, `team_season_stats_advanced`, `player_game_logs` | **1996-97 to present only** |

The daily Cloud Scheduler job refreshes the current season's stats after each game day
(Oct–Jun) and weekly during the off-season (Jul–Sep).

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

- [ ] **Pre-1996 player stats** — scrape Basketball Reference for full historical coverage
- [ ] **Shot chart caching** — pre-fetch and store popular player/season shot data in GCS
      so the first request isn't slow
- [ ] **Play-by-play aggregations** — pre-aggregate clutch stats, lineup stats, etc.
      into summary Parquet files so common queries don't scan all 13.5M rows
