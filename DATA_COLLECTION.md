# YT PoliSignal: Data Collection Architecture

This document defines the exact data streams, polling schedules, and metrics collected by the YT PoliSignal engine. It is designed to get researchers or technical maintainers up to speed on what is being logged, when it happens, and how it avoids Google's API quota limits.

## 1. The Asymmetric Polling Strategy (Avoiding API Quotas)
Scraping 60+ political YouTube channels multiple times per hour would instantly burn through the standard 10,000-token YouTube Data API daily limit. To achieve high-frequency telemetry at zero cost, YT PoliSignal uses a **Two-Phase Scrape**:

### Phase A: RSS Discovery (Free & Unlimited)
Every 30 minutes, the background engine (`apscheduler`) queries the raw XML syndication feeds natively offered by YouTube for every channel.
- **Cost:** 0 API Quota
- **Data Retrieved:** `video_id`, `title`, `published_at`, `channel_id`
- **Purpose:** We only detect that a *new* broadcast exists or parse keywords from its title. We do not fetch views or likes yet.

### Phase B: API Enrichment (Strictly Batched)
Once the RSS poller establishes a queue of net-new videos, the engine fires a batch request to the official YouTube API v3. 
- **Cost:** Highly optimized (1 token per block of 50 videos).
- **Data Retrieved:** `view_count`, `like_count`, `comment_count`, `subscriber_count`.
- **Purpose:** Enriches the local database with pure engagement metrics so our Munger Analytics engine can process the "Demand" side of the equation.

---

## 2. The Internal Chronology (When Data Moves)

The entire dataset is strictly managed by `poller/main.py` operating on specific asynchronous chron intervals to avoid SQLite database locks.

| Polling Job | Interval | Description & Action Executed |
| :--- | :--- | :--- |
| **Initial Seed** | Boot | Connects to API once on startup to grab all channel historical views and subscribers. Builds the Baseline. |
| **RSS Ingestion** | 30 Mins | Hits standard XML feeds to discover new standard uploads across all tracked broadcasters. |
| **Ecosystem Pulse** | 35 Mins | Compiles overall upload velocity vs the 30-day baseline to output a macroscopic "system activity score" (e.g., 105%). |
| **Munger Engagement** | 36 Mins | Grabs likes/comments for every newly discovered video. Weighs comments at `3x` value and produces the **Heat Index**. |
| **Affiliation Divergence** | 3 Hours | Semantically groups keywords to check if an independent creator broke a story before a legacy/affiliated network. |
| **Feedback Detector** | 6 Hours | Looks at channel behavior in the 72 hours *following* a massive engagement spike to see if they upload more of the same topic (proving the audience controls the creator). |
| **Stability Index** | Weekly | Fired every Sunday at midnight. Indexes massive shifts in broadcaster view-velocity rankings. |

---

## 3. Database Schema Overview

All scraped data safely resolves into `/data/yt_temperature.db`. If you need raw data exports, here are the dominant tables that hold historical telemetry:

- `channels`: Static configuration mapping broadcasters to `tier` (1, 2, 3) and `affiliation_type` (independent vs. affiliated).
- `videos`: Master log of every isolated broadcast, including title text and publication timestamp.
- `snapshots`: The core time-series tracking. Every time the API fires, it logs the view/like growth curves for a specific `video_id` at a specific `polled_at` timestamp.
- `engagement_snapshots`: Munger-specific table. Replaces raw views with mathematically formatted `likes_per_view` and `comments_per_view` to isolate friction/demand.
- `feedback_events`: A logged row every time a creator demonstrably alters their content supply to follow a quantified audience demand spike.
- `keywords`: Tokenized vocabulary parsed via NLTK out of recent broadcast titles. Stripped of massive stop-words to isolate proper nouns and political subjects.

---

## 4. Cold Start Calibrations
When launching testing variants on a fresh database, you will see a yellow calibration banner. Because metrics like the **Heat Index** or **Ecosystem Pulse** require 30 days of data to build a legitimate moving average, the system actively degrades elegantly:
- **Days 0-3:** Averages metrics against the *latest 24 hours* available.
- **Days 3-14:** Expands checking boundaries to a standard week.
- **Days 14+:** Statistically sound.
- **Days 30:** Fully calibrated against the rolling trailing month.
