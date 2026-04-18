"""
RSS Poller Module.
Fetches video meta via RSS to save API quota.
"""

import feedparser
import requests
import logging
from datetime import datetime
from db import get_connection
import keyword_extractor

def run():
    """Runs the RSS polling process for all channels."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT channel_id, channel_name FROM channels")
    channels = cursor.fetchall()
    
    for channel in channels:
        channel_id = channel['channel_id']
        channel_name = channel['channel_name']
        
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        
        try:
            feed = feedparser.parse(rss_url)
            new_videos_count = 0
            
            for entry in feed.entries:
                video_id = entry.yt_videoid
                title = entry.title
                description = entry.summary if 'summary' in entry else ""
                published_at = entry.published
                
                # Check if video exists
                cursor.execute("SELECT 1 FROM videos WHERE video_id = ?", (video_id,))
                if not cursor.fetchone():
                    # Insert new video
                    cursor.execute("""
                        INSERT INTO videos (video_id, channel_id, title, description, published_at, tags)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (video_id, channel_id, title, description, published_at, "[]"))
                    conn.commit()
                    
                    # Queue extraction of keywords
                    keyword_extractor.extract_and_store(video_id, channel_id, title, description)
                    
                    new_videos_count += 1
            
            conn.commit()
            logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {channel_name} — {new_videos_count} new videos found")

        except Exception as e:
            logging.error(f"Failed to fetch or parse RSS for {channel_name} ({channel_id}): {e}")
    
    conn.close()
