# Tech News Daily — Data Fetch

Unified data pipeline for Tech News Daily. Combines RSS news fetching, Sensor Tower app analytics, and Product Hunt top products into a single repository.

## Components

### 1. News Fetcher (`fetch_news.py`)
Fetches tech news from RSS feeds, filters by category using AI, translates Chinese content, and stores results in Supabase.

| Source | RSS URL | Language |
|--------|---------|----------|
| 36kr | https://36kr.com/feed | Chinese (auto-translated) |
| TechCrunch | https://techcrunch.com/feed/ | English |
| Techmeme | https://www.techmeme.com/feed.xml | English |

**Categories:** New Product, New Feature, New VC Investment

**Schedule:** Every hour at :17

### 2. Sensor Tower Fetcher (`fetch_sensortower.py`)
Fetches top apps by downloads, download growth, download delta, and top advertisers from Sensor Tower API.

| Supabase Table | Description |
|----------------|-------------|
| `download_rank_7d` | Top 50 apps by absolute downloads (7-day daily avg) |
| `download_percent_rank_7d` | Top 50 apps by download % increase (7-day) |
| `download_delta_rank_7d` | Top 50 apps by absolute download change (7-day) |
| `advertiser_rank_7d` | Top 50 advertisers by Share of Voice (US) |

**Schedule:** Daily at 00:00 UTC

### 3. Product Hunt Fetcher (`fetch_producthunt_top.py`)
Fetches top 15 products from Product Hunt and stores in Supabase.

**Schedule:** Daily at 00:00 UTC

### 4. RSS Feed Generator (`generate_rss.py`)
Generates a Product Hunt RSS feed from Supabase data and deploys to GitHub Pages.

### 5. Weekly Digest (`generate_weekly_digest.py`)
Publishes weekly digest RSS feed from Supabase digests to GitHub Pages.

**Schedule:** Every Friday at 09:00 UTC

## GitHub Actions Workflows

| Workflow | Schedule | Scripts |
|----------|----------|---------|
| `news-fetcher.yml` | Hourly at :17 | `fetch_news.py` |
| `sensortower-fetcher.yml` | Daily at 00:00 UTC | `fetch_sensortower.py`, `fetch_producthunt_top.py`, `generate_rss.py` + GitHub Pages deploy |
| `weekly-digest.yml` | Fridays at 09:00 UTC | `generate_weekly_digest.py`, `generate_rss.py` + GitHub Pages deploy |

## Required GitHub Secrets

| Secret | Used By |
|--------|---------|
| `SUPABASE_URL` | All scripts |
| `SUPABASE_SERVICE_ROLE_KEY` | All scripts |
| `GEMINI_API_KEY` | `fetch_news.py`, `fetch_sensortower.py`, `generate_weekly_digest.py` |
| `SENSORTOWER_API_KEY` | `fetch_sensortower.py` |
| `PH_API_KEY` | `fetch_producthunt_top.py`, `fetch_news.py` |
| `PH_API_SECRET` | `fetch_producthunt_top.py`, `fetch_news.py` |

## Local Development

```bash
# Set environment variables
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your_key"
export GEMINI_API_KEY="your_key"
export SENSORTOWER_API_KEY="your_key"
export PH_API_KEY="your_key"
export PH_API_SECRET="your_secret"

# Install dependencies
pip install -r requirements.txt

# Run individual scripts
python fetch_news.py
python fetch_sensortower.py
python fetch_producthunt_top.py
python generate_rss.py
python generate_weekly_digest.py
```
