"""
RSS Poller Module.
Fetches video meta via RSS to save API quota.
"""

import feedparser
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime
from db import get_connection, get_cursor
import keyword_extractor

def _parse_date(raw_date_str):
    """Safely parse an RSS date string to PostgreSQL-compatible ISO format."""
    if not raw_date_str:
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    try:
        dt = parsedate_to_datetime(raw_date_str)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

def run():
    """Runs the RSS polling process for all channels."""
    conn = get_connection()
    cursor = get_cursor(conn)
    cursor.execute("SELECT channel_id, channel_name FROM channels")
    channels = cursor.fetchall()
    cursor.close()
    conn.close()

    for channel in channels:
        channel_id = channel['channel_id']
        channel_name = channel['channel_name']
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

        try:
            feed = feedparser.parse(rss_url)
            new_videos_count = 0

            # Open a fresh connection per channel to prevent cursor bleed
            ch_conn = get_connection()
            ch_cursor = get_cursor(ch_conn)

            for entry in feed.entries:
                video_id = getattr(entry, 'yt_videoid', None)
                if not video_id:
                    continue

                title = entry.get('title', '')
                description = entry.get('summary', '')[:500]
                published_at = _parse_date(entry.get('published', ''))

                # Skip if already known
                ch_cursor.execute("SELECT 1 FROM videos WHERE video_id = %s", (video_id,))
                if ch_cursor.fetchone():
                    continue

                ch_cursor.execute("""
                    INSERT INTO videos (video_id, channel_id, title, description, published_at, tags)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (video_id, channel_id, title, description, published_at, "[]"))
                ch_conn.commit()

                # Extract keywords only once, immediately after insert
                keyword_extractor.extract_and_store(video_id, channel_id, title, description)
                new_videos_count += 1

            ch_cursor.close()
            ch_conn.close()
            logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {channel_name} — {new_videos_count} new videos found")

        except Exception as e:
            logging.error(f"Failed to fetch or parse RSS for {channel_name} ({channel_id}): {e}")
