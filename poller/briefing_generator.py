"""
YouTube in Brief — AI Briefing Generator
Runs nightly via APScheduler, calls local Ollama to produce an editorial
summary of the day's YouTube political media trends.

Requires: OLLAMA_HOST env var (defaults to http://host.docker.internal:11434)
          OLLAMA_MODEL env var (defaults to llama3.2:latest)
"""

import json
import logging
import os
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from db import get_connection

logger = logging.getLogger(__name__)

OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://host.docker.internal:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:latest")


# ─── Data Gathering ────────────────────────────────────────────────────────────

def _gather_signals():
    """Pull structured trend data from the DB for today's briefing context."""
    conn = get_connection()
    c = conn.cursor()
    signals = {}

    try:
        # 1. Ecosystem pulse score
        c.execute("""
            SELECT pulse_score, json_extract(component_json, '$.calibration_mode') as mode
            FROM ecosystem_pulse
            WHERE json_extract(component_json, '$.raw_pulse') IS NOT NULL
            ORDER BY recorded_at DESC LIMIT 1
        """)
        row = c.fetchone()
        signals['pulse'] = round(row['pulse_score']) if row else 100
        signals['pulse_mode'] = row['mode'] if row else 'unknown'

        # 2. Top cross-channel keywords (last 24h, 3+ channels)
        c.execute("""
            SELECT keyword, COUNT(DISTINCT channel_id) as ch_cnt, COUNT(*) as total_mentions
            FROM keywords
            WHERE extracted_at >= datetime('now', '-24 hours')
            GROUP BY keyword
            HAVING ch_cnt >= 3
            ORDER BY ch_cnt DESC, total_mentions DESC
            LIMIT 10
        """)
        signals['top_keywords'] = [
            {"keyword": r['keyword'], "channels": r['ch_cnt'], "mentions": r['total_mentions']}
            for r in c.fetchall()
        ]

        # 3. Fastest growing videos today
        c.execute("""
            SELECT v.title, c.channel_name, c.affiliation_type, c.tier,
                   s_latest.view_count - s_prev.view_count as view_gain
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            JOIN snapshots s_latest ON s_latest.video_id = v.video_id
            JOIN snapshots s_prev   ON s_prev.video_id   = v.video_id
            WHERE v.published_at >= datetime('now', '-24 hours')
              AND s_latest.id = (SELECT MAX(id) FROM snapshots WHERE video_id = v.video_id)
              AND s_prev.id   = (SELECT MIN(id) FROM snapshots WHERE video_id = v.video_id)
              AND s_latest.id != s_prev.id
            ORDER BY view_gain DESC
            LIMIT 5
        """)
        signals['fastest_videos'] = [
            {
                "title": r['title'],
                "channel": r['channel_name'],
                "affiliation": r['affiliation_type'],
                "tier": r['tier'],
                "view_gain": r['view_gain']
            }
            for r in c.fetchall()
        ]

        # 4. Grassroots-emerging topics
        c.execute("""
            SELECT keyword, independent_count, affiliated_count, divergence_score
            FROM affiliation_divergence
            WHERE id IN (SELECT MAX(id) FROM affiliation_divergence GROUP BY keyword)
              AND direction = 'independent_leading'
              AND divergence_score >= 0.6
              AND independent_count >= 3
              AND affiliated_count <= 1
            ORDER BY independent_count DESC
            LIMIT 5
        """)
        signals['grassroots_topics'] = [
            {"keyword": r['keyword'], "ind": r['independent_count'], "aff": r['affiliated_count']}
            for r in c.fetchall()
        ]

        # 5. Breakout channels (upload surge today)
        c.execute("""
            SELECT c.channel_name, c.affiliation_type, ra.uploads_today, ra.deviation_ratio
            FROM rhythm_alerts ra
            JOIN channels c ON ra.channel_id = c.channel_id
            WHERE ra.alerted_at >= datetime('now', '-24 hours')
            ORDER BY ra.deviation_ratio DESC
            LIMIT 4
        """)
        signals['breakout_channels'] = [
            {"channel": r['channel_name'], "affiliation": r['affiliation_type'],
             "uploads": r['uploads_today'], "multiplier": round(r['deviation_ratio'], 1)}
            for r in c.fetchall()
        ]

        # 6. Urgency language trend
        c.execute("SELECT urgency_ratio FROM title_linguistics ORDER BY recorded_at DESC LIMIT 7")
        rows = c.fetchall()
        if rows:
            signals['urgency_today'] = round(rows[0]['urgency_ratio'] * 100, 1)
            signals['urgency_avg_7d'] = round(
                sum(r['urgency_ratio'] for r in rows) / len(rows) * 100, 1
            )
        else:
            signals['urgency_today'] = None
            signals['urgency_avg_7d'] = None

        # 7. Recent feedback events
        c.execute("""
            SELECT c.channel_name, fe.keyword, fe.trigger_engagement_percentile,
                   fe.response_video_count
            FROM feedback_events fe
            JOIN channels c ON c.channel_id = fe.channel_id
            WHERE fe.detected_at >= datetime('now', '-24 hours')
            ORDER BY fe.trigger_engagement_percentile DESC
            LIMIT 3
        """)
        signals['feedback_events'] = [
            {"channel": r['channel_name'], "keyword": r['keyword'],
             "percentile": round(r['trigger_engagement_percentile'] or 0),
             "follow_ups": r['response_video_count']}
            for r in c.fetchall()
        ]

    except Exception as e:
        logger.error(f"[BRIEFING] Error gathering signals: {e}")
    finally:
        conn.close()

    return signals


# ─── Prompt Assembly ───────────────────────────────────────────────────────────

def _build_prompt(signals):
    """Build a structured editorial prompt from today's signals."""

    lines = [
        "You are a dry, analytical media intelligence editor. Write a SHORT editorial briefing (3–4 paragraphs) titled 'YouTube in Brief' for today.",
        "Base your analysis strictly on the data below. Do not invent figures. Do not use bullet points. Use complete sentences.",
        "Write in a concise, authoritative tone — similar to The Economist's 'The World in Brief' section. Focus on what is genuinely notable.",
        "",
        "=== TODAY'S DATA ===",
        "",
        f"Ecosystem Activity: {signals.get('pulse', '?')}% of 30-day baseline",
    ]

    kws = signals.get('top_keywords', [])
    if kws:
        kw_str = ", ".join(f"'{k['keyword']}' ({k['channels']} channels)" for k in kws[:5])
        lines.append(f"Top cross-channel keywords: {kw_str}")
    else:
        lines.append("Top cross-channel keywords: insufficient data")

    vids = signals.get('fastest_videos', [])
    if vids:
        lines.append("Fastest-growing videos today:")
        for v in vids:
            lines.append(f"  - \"{v['title']}\" by {v['channel']} (Tier {v['tier']}, {v['affiliation']}) — +{v['view_gain']:,} views")
    
    grass = signals.get('grassroots_topics', [])
    if grass:
        g_str = ", ".join(f"'{g['keyword']}' ({g['ind']} independents, {g['aff']} affiliates)" for g in grass)
        lines.append(f"Grassroots-only topics (not yet picked up by legacy media): {g_str}")

    breakouts = signals.get('breakout_channels', [])
    if breakouts:
        b_str = ", ".join(f"{b['channel']} ({b['multiplier']}× normal upload rate)" for b in breakouts)
        lines.append(f"Upload surge channels today: {b_str}")

    urg = signals.get('urgency_today')
    urg_avg = signals.get('urgency_avg_7d')
    if urg is not None:
        direction = "above" if urg > urg_avg else "below"
        lines.append(f"Urgency language in titles: {urg}% today vs {urg_avg}% 7-day average ({direction} average)")

    feedbacks = signals.get('feedback_events', [])
    if feedbacks:
        lines.append("Audience feedback events detected:")
        for f in feedbacks:
            lines.append(f"  - {f['channel']} responded to '{f['keyword']}' spike ({f['percentile']}th percentile) with {f['follow_ups']} follow-up videos")

    lines += [
        "",
        "=== END DATA ===",
        "",
        "Write the editorial briefing now. Start directly with content — no title header needed. Three to four paragraphs maximum."
    ]

    return "\n".join(lines)


# ─── Ollama Call ───────────────────────────────────────────────────────────────

def _call_ollama(prompt):
    """Send the prompt to local Ollama and return the response text."""
    url = f"{OLLAMA_HOST}/api/generate"
    payload = json.dumps({
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.4,   # Low temperature for factual, consistent output
            "num_predict": 500,   # ~3–4 paragraphs max
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    logger.info(f"[BRIEFING] Calling Ollama at {url} with model {OLLAMA_MODEL}...")
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        return data.get("response", "").strip()


# ─── Storage ───────────────────────────────────────────────────────────────────

def _store_briefing(text, signals):
    """Persist the generated briefing to the daily_briefings table."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO daily_briefings (briefing_text, generated_by, signals_json, generated_at)
            VALUES (?, 'ollama', ?, datetime('now'))
        """, (text, json.dumps(signals)))
        conn.commit()
        logger.info("[BRIEFING] Stored today's AI briefing to database.")
    except Exception as e:
        logger.error(f"[BRIEFING] Failed to store briefing: {e}")
    finally:
        conn.close()


# ─── Main Entry Point ──────────────────────────────────────────────────────────

def generate_daily_briefing():
    """
    Main entry point called by APScheduler at 23:00 UTC daily.
    Gathers signals → builds prompt → calls Ollama → stores result.
    Falls back to the template-string briefing if Ollama is unavailable.
    """
    logger.info("[BRIEFING] Starting nightly YouTube in Brief generation...")
    try:
        signals  = _gather_signals()
        prompt   = _build_prompt(signals)
        briefing = _call_ollama(prompt)

        if not briefing:
            raise ValueError("Ollama returned empty response")

        _store_briefing(briefing, signals)
        logger.info("[BRIEFING] Done. Briefing written successfully.")

    except urllib.error.URLError as e:
        logger.warning(f"[BRIEFING] Ollama unreachable ({e}) — skipping AI briefing for today.")
    except Exception as e:
        logger.error(f"[BRIEFING] Unexpected error during briefing generation: {e}")
