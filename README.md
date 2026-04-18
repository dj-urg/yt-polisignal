# YT Temperature Dashboard

YT Temperature Dashboard is a fully containerized, zero-maintenance application that monitors conservative YouTube channels to provide a real-time "temperature dashboard". It surfaces trending topics, channel convergence, and hottest videos, all operating efficiently within Google's free YouTube API quota. 

## Prerequisites
- Docker
- Docker Compose
- A YouTube Data API v3 Key

## Setup
1. Clone this repository to your local system.
2. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
3. Edit the `.env` file and input your YouTube API key.
4. Run the containers as a daemon:
   ```bash
   docker compose up -d
   ```

## Channels Configuration
The application seeds its data using `channels.csv`. To add or remove channels, simply edit the flat file and restart the poller service to merge the new state:
```bash
docker compose restart poller
```

## Viewing Logs
To verify that scheduled pulls are succeeding, or check for RSS / Database errors, view the logs for the poller service directly:
```bash
docker compose logs -f poller
```

## Dashboard Access
The user interface is hosted securely within a Flask container, and can be easily accessed statically via:
http://localhost:8080

## Quota Limit Strategy
The application actively curtails its API payload by using RSS fetches for free initial discovery, keeping the total footprint under ~4,000 requests per 24 hour cycle. It performs HTTP 403 back-offs to avoid breaching Google's 10,000 token maximum.

## Munger Analytics Layer
This dashboard implements the Supply and Demand Framework from Munger (2024), *The YouTube Apparatus*. Key additions:
- **Engagement Heat Index** — measures audience intensity relative to each channel's baseline, not raw view counts
- **Affiliation Classification** — channels classified as institutionally affiliated or independent per Munger's typology
- **Supply-Demand Feedback Detection** — identifies when channels post more content in response to engagement spikes
- **Affiliation Divergence Index** — tracks whether stories are emerging from independents or being pushed by affiliated media
- **Rank Stability Monitor** — weekly ecosystem stability score based on velocity-rank mobility
- **Cold Start Seeding** — historical API data used to establish baseline on day one

*Reference: Munger, K. (2024). The YouTube Apparatus. Cambridge Elements in Politics and Communication. DOI: 10.1017/9781009359795*
