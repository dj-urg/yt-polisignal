"""
API Poller Module.
Manages YouTube API calls responsibly to avoid quota starvation.
"""

import os
import requests
import logging
from datetime import datetime, timedelta
from db import get_connection, get_cursor

API_KEY = os.getenv("YOUTUBE_API_KEY")
API_DISABLED_UNTIL = None

def check_quota():
    global API_DISABLED_UNTIL
    if API_DISABLED_UNTIL and datetime.now() < API_DISABLED_UNTIL:
        logging.warning("API Polling is disabled due to previous 403 Rate Limit.")
        return False
    # Reset disable boolean if time has elapsed
    if API_DISABLED_UNTIL and datetime.now() >= API_DISABLED_UNTIL:
        API_DISABLED_UNTIL = None
    return True

def handle_quota_error():
    global API_DISABLED_UNTIL
    logging.error("Quota Exceeded (HTTP 403). Disabling API calls for the rest of the day.")
    # Midnight PST basically or just +24h to be safe
    API_DISABLED_UNTIL = datetime.now() + timedelta(days=1)

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def enrich_new_videos():
    """JOB A: Enrich new videos with stats."""
    if not check_quota() or not API_KEY:
        return
        
    conn = get_connection()
    cursor = get_cursor(conn)
    
    # Query videos with no snapshot
    cursor.execute("""
        SELECT v.video_id FROM videos v
        LEFT JOIN snapshots s ON v.video_id = s.video_id
        WHERE s.id IS NULL
    """)
    rows = cursor.fetchall()
    video_ids = [r['video_id'] for r in rows]
    
    if not video_ids:
        cursor.close()
        conn.close()
        return
        
    for batch in chunker(video_ids, 50):
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "statistics",
            "id": ",".join(batch),
            "key": API_KEY
        }
        res = requests.get(url, params=params)
        
        if res.status_code == 403:
            handle_quota_error()
            break
        elif res.status_code != 200:
            logging.error(f"YouTube videos list error: {res.text}")
            continue
            
        data = res.json()
        for item in data.get("items", []):
            vid = item.get("id")
            stats = item.get("statistics", {})
            views = int(stats.get("viewCount", 0) or 0)
            likes = int(stats.get("likeCount", 0) or 0)
            comments = int(stats.get("commentCount", 0) or 0)
            
            cursor.execute("""
                INSERT INTO snapshots (video_id, view_count, like_count, comment_count)
                VALUES (%s, %s, %s, %s)
            """, (vid, views, likes, comments))
            
    conn.commit()
    cursor.close()
    conn.close()


def refresh_hot_videos():
    """JOB B: Refresh stats on recent hot videos."""
    if not check_quota() or not API_KEY:
        return
        
    conn = get_connection()
    cursor = get_cursor(conn)
    
    # 72 hours window
    date_limit = (datetime.now() - timedelta(hours=72)).strftime('%Y-%m-%d %H:%M:%S')
    
    cursor.execute("""
        SELECT v.video_id FROM videos v
        WHERE v.published_at >= %s
        ORDER BY v.published_at DESC
        LIMIT 200
    """, (date_limit,))
    
    rows = cursor.fetchall()
    video_ids = [r['video_id'] for r in rows]
    
    if not video_ids:
        cursor.close()
        conn.close()
        return
        
    for batch in chunker(video_ids, 50):
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "statistics",
            "id": ",".join(batch),
            "key": API_KEY
        }
        res = requests.get(url, params=params)
        
        if res.status_code == 403:
            handle_quota_error()
            break
        elif res.status_code != 200:
            logging.error(f"YouTube videos list error: {res.text}")
            continue
            
        data = res.json()
        for item in data.get("items", []):
            vid = item.get("id")
            stats = item.get("statistics", {})
            views = int(stats.get("viewCount", 0) or 0)
            likes = int(stats.get("likeCount", 0) or 0)
            comments = int(stats.get("commentCount", 0) or 0)
            
            cursor.execute("""
                INSERT INTO snapshots (video_id, view_count, like_count, comment_count)
                VALUES (%s, %s, %s, %s)
            """, (vid, views, likes, comments))
            
    conn.commit()
    cursor.close()
    conn.close()


def refresh_channel_stats():
    """Daily channel subscription updates."""
    if not check_quota() or not API_KEY:
        return
        
    conn = get_connection()
    cursor = get_cursor(conn)
    
    cursor.execute("SELECT channel_id FROM channels")
    rows = cursor.fetchall()
    channel_ids = [r['channel_id'] for r in rows]
    
    for batch in chunker(channel_ids, 50):
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "statistics",
            "id": ",".join(batch),
            "key": API_KEY
        }
        res = requests.get(url, params=params)
        
        if res.status_code == 403:
            handle_quota_error()
            break
        elif res.status_code != 200:
            logging.error(f"YouTube channels list error: {res.text}")
            continue
            
        data = res.json()
        for item in data.get("items", []):
            cid = item.get("id")
            stats = item.get("statistics", {})
            subs = int(stats.get("subscriberCount", 0) or 0)
            videocount = int(stats.get("videoCount", 0) or 0)
            
            cursor.execute("""
                UPDATE channels SET subscriber_count = %s, last_updated = CURRENT_TIMESTAMP
                WHERE channel_id = %s
            """, (subs, cid))
            
            cursor.execute("""
                INSERT INTO channel_snapshots (channel_id, subscriber_count, video_count)
                VALUES (%s, %s, %s)
            """, (cid, subs, videocount))
            
    conn.commit()
    cursor.close()
    conn.close()
