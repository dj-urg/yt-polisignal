import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from db import get_connection, get_cursor

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

URGENCY_WORDS = {
    "breaking", "urgent", "alert", "emergency", "crisis", "shocking",
    "bombshell", "explosive", "exposed", "revealed", "caught", "warning",
    "panic", "chaos", "disaster", "threat", "critical", "now", "immediately"
}

INDIVIDUAL_INDICATORS = {
    "trump", "biden", "harris", "pelosi", "obama", "desantis",
    "musk", "gates", "soros", "schumer", "mcconnell", "aoc",
    "vance", "walz", "putin", "zelensky", "xi", "fauci",
    "kennedy", "rfk"
}

def compute_topic_velocity():
    """Calculate rate of change for each keyword over the last 6 hours vs. prior 6 hours."""
    logging.info("Running compute_topic_velocity...")
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        
        now = datetime.utcnow()
        t_minus_6h = now - timedelta(hours=6)
        t_minus_12h = now - timedelta(hours=12)

        # 1. Fetch current 6h window
        # GROUP_CONCAT -> STRING_AGG in PostgreSQL
        cursor.execute("""
            SELECT keyword, COUNT(*) as cnt, COUNT(DISTINCT channel_id) as ch_cnt, STRING_AGG(video_id, ',') as vids
            FROM keywords
            WHERE extracted_at >= %s
            GROUP BY keyword
        """, (t_minus_6h,))
        current_data = {row['keyword']: row for row in cursor.fetchall()}

        # 2. Fetch previous 6h window
        cursor.execute("""
            SELECT keyword, COUNT(*) as cnt
            FROM keywords
            WHERE extracted_at >= %s AND extracted_at < %s
            GROUP BY keyword
        """, (t_minus_12h, t_minus_6h))
        prev_data = {row['keyword']: row['cnt'] for row in cursor.fetchall()}

        for keyword, row in current_data.items():
            current_count = row['cnt']
            prev_count = prev_data.get(keyword, 0)
            
            if current_count < 3 or row['ch_cnt'] < 2:
                continue
            
            if prev_count == 0:
                velocity = float(current_count * 100)
            else:
                velocity = ((current_count - prev_count) / prev_count) * 100.0

            velocity = min(velocity, 500.0)

            video_ids_json = json.dumps(list(set(row['vids'].split(','))))

            cursor.execute("""
                INSERT INTO topic_snapshots (keyword, mention_count, channel_count, video_ids)
                VALUES (%s, %s, %s, %s)
            """, (keyword, current_count, row['ch_cnt'], video_ids_json))

        conn.commit()
    except Exception as e:
        logging.error(f"Error in compute_topic_velocity: {e}")
    finally:
        cursor.close()
        conn.close()

def detect_first_movers():
    """Find the channel that published first for each keyword in the last 24h."""
    logging.info("Running detect_first_movers...")
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        t_minus_24h = datetime.utcnow() - timedelta(hours=24)
        
        cursor.execute("""
            SELECT k.keyword, k.channel_id, k.video_id, v.published_at
            FROM keywords k
            JOIN videos v ON k.video_id = v.video_id
            WHERE k.extracted_at > %s
            ORDER BY k.keyword, v.published_at ASC
        """, (t_minus_24h,))
        
        rows = cursor.fetchall()
        seen_keywords = set()
        for row in rows:
            if row['keyword'] in seen_keywords:
                continue
            seen_keywords.add(row['keyword'])
            cursor.execute("""
                INSERT INTO first_movers (keyword, channel_id, video_id, first_seen_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(keyword, channel_id) DO NOTHING
            """, (row['keyword'], row['channel_id'], row['video_id'], row['published_at']))
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error in detect_first_movers: {e}")
    finally:
        cursor.close()
        conn.close()

def compute_channel_rhythm():
    """Maintain rolling 30-day upload baseline per channel and detect deviations."""
    logging.info("Running compute_channel_rhythm...")
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        t_minus_30d = datetime.utcnow() - timedelta(days=30)
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

        # Baseline per channel
        cursor.execute("""
            SELECT channel_id, COUNT(*) as total_30d
            FROM videos
            WHERE published_at >= %s
            GROUP BY channel_id
        """, (t_minus_30d,))
        
        rows = cursor.fetchall()
        for row in rows:
            avg_daily = row['total_30d'] / 30.0
            cursor.execute("""
                INSERT INTO channel_rhythm (channel_id, avg_daily_uploads, last_calculated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT(channel_id) DO UPDATE SET
                    avg_daily_uploads=EXCLUDED.avg_daily_uploads,
                    last_calculated_at=EXCLUDED.last_calculated_at
            """, (row['channel_id'], avg_daily))

        # Check today's deviations
        cursor.execute("""
            SELECT v.channel_id, COUNT(*) as uploads_today, cr.avg_daily_uploads
            FROM videos v
            JOIN channel_rhythm cr ON v.channel_id = cr.channel_id
            WHERE v.published_at >= %s
            GROUP BY v.channel_id, cr.avg_daily_uploads
        """, (today_start,))
        
        rows = cursor.fetchall()
        for row in rows:
            avg_daily = row['avg_daily_uploads']
            uploads_today = row['uploads_today']
            denom = avg_daily if avg_daily > 0 else 1.0
            deviation = uploads_today / denom
            
            if deviation >= 2.5:
                # Insert only 1 alert per channel per day
                cursor.execute("""
                    SELECT 1 FROM rhythm_alerts 
                    WHERE channel_id = %s AND alerted_at >= %s
                """, (row['channel_id'], today_start))
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO rhythm_alerts (channel_id, uploads_today, baseline_avg, deviation_ratio)
                        VALUES (%s, %s, %s, %s)
                    """, (row['channel_id'], uploads_today, avg_daily, deviation))
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error in compute_channel_rhythm: {e}")
    finally:
        cursor.close()
        conn.close()

def compute_ecosystem_pulse():
    """Single scalar score representing overall temperature."""
    logging.info("Running compute_ecosystem_pulse...")
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        
        t_minus_6h = datetime.utcnow() - timedelta(hours=6)

        cursor.execute("SELECT channel_id, tier FROM channels")
        channels = {row['channel_id']: row['tier'] for row in cursor.fetchall()}

        cursor.execute("""
            SELECT channel_id, COUNT(*) as cnt
            FROM keywords WHERE extracted_at >= %s GROUP BY channel_id
        """, (t_minus_6h,))
        cur_counts = {row['channel_id']: row['cnt'] for row in cursor.fetchall()}

        pulse = 0.0
        tier_weights = {1: 0.5, 2: 0.35, 3: 0.15}
        
        tier1_sum = 0
        tier2_sum = 0
        tier3_sum = 0
        
        top_contributors = []

        for cid, tier in channels.items():
            cur = cur_counts.get(cid, 0)
            # Raw volume pulse calculation
            score = float(cur)
            weight = tier_weights.get(tier, 0.2)
            
            pulse += (score * weight)
            
            if tier == 1: tier1_sum += score
            if tier == 2: tier2_sum += score
            if tier == 3: tier3_sum += score
            
            top_contributors.append({"channel_name": str(cid), "score": score})

        # MUNGER COLD START MITIGATION
        # PostgreSQL: EXTRACT(EPOCH FROM (NOW() - date)) / 86400
        cursor.execute("""
            SELECT EXTRACT(EPOCH FROM (NOW() - MIN(seed_date))) / 86400 as d
            FROM ecosystem_baseline_seed
        """)
        dr = cursor.fetchone()
        days_since_seed = dr['d'] if dr and dr['d'] is not None else 0
        
        if days_since_seed < 3:
            calib_mode = "today"
            # PostgreSQL casting and JSON extraction
            cursor.execute("SELECT AVG((component_json::json->>'raw_pulse')::float) as a FROM ecosystem_pulse WHERE recorded_at::date = NOW()::date AND (component_json::json->>'raw_pulse') IS NOT NULL")
        elif days_since_seed < 14:
            calib_mode = "week"
            cursor.execute("SELECT AVG((component_json::json->>'raw_pulse')::float) as a FROM ecosystem_pulse WHERE recorded_at >= NOW() - INTERVAL '7 days' AND (component_json::json->>'raw_pulse') IS NOT NULL")
        else:
            calib_mode = "month"
            cursor.execute("SELECT AVG((component_json::json->>'raw_pulse')::float) as a FROM ecosystem_pulse WHERE recorded_at >= NOW() - INTERVAL '30 days' AND (component_json::json->>'raw_pulse') IS NOT NULL")
            
        val = cursor.fetchone()['a']
        baseline = float(val) if val else pulse if pulse > 0 else 1.0
            
        # Normalize to 100 scale based on relative baseline comparison
        ratio = pulse / max(baseline, 0.1)
        pulse_normalized = min(ratio * 100, 200)
        
        top_contributors.sort(key=lambda x: x['score'], reverse=True)
        top_contribs_json = top_contributors[:5]

        comp_json = json.dumps({
            "raw_pulse": pulse,
            "calibration_mode": calib_mode,
            "tier1_avg": tier1_sum / max(len([v for v in channels.values() if v==1]), 1),
            "tier2_avg": tier2_sum / max(len([v for v in channels.values() if v==2]), 1),
            "tier3_avg": tier3_sum / max(len([v for v in channels.values() if v==3]), 1),
            "channel_count": len(channels),
            "top_contributing_channels": top_contribs_json
        })

        cursor.execute("""
            INSERT INTO ecosystem_pulse (pulse_score, component_json)
            VALUES (%s, %s)
        """, (pulse_normalized, comp_json))

        conn.commit()
    except Exception as e:
        logging.error(f"Error in compute_ecosystem_pulse: {e}")
    finally:
        cursor.close()
        conn.close()

def classify_topic_lifespans():
    """Classify topics as flash, slow_burn, or recurring."""
    logging.info("Running classify_topic_lifespans...")
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        t_minus_7d = datetime.utcnow() - timedelta(days=7)

        cursor.execute("""
            SELECT keyword, MIN(extracted_at) as first_seen, MAX(extracted_at) as last_seen, COUNT(DISTINCT channel_id) as total_ch
            FROM keywords
            WHERE extracted_at >= %s
            GROUP BY keyword
        """, (t_minus_7d,))
        
        rows = cursor.fetchall()
        for row in rows:
            keyword = row['keyword']
            f_dt = row['first_seen']
            l_dt = row['last_seen']
            
            # PostgreSQL returns datetime objects
            if isinstance(f_dt, str):
                f_dt = datetime.fromisoformat(f_dt.replace('Z',''))
            if isinstance(l_dt, str):
                l_dt = datetime.fromisoformat(l_dt.replace('Z',''))
                
            lifespan_h = (l_dt - f_dt).total_seconds() / 3600.0
            peak_ch = row['total_ch']

            if lifespan_h <= 12 and peak_ch >= 5:
                classification = "flash"
            elif lifespan_h >= 72:
                classification = "slow_burn"
            elif lifespan_h >= 24:
                classification = "recurring"
            else:
                classification = "flash"
                
            cursor.execute("""
                INSERT INTO topic_lifespan (keyword, first_seen_at, last_seen_at, peak_channels, peak_at, classification)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                ON CONFLICT(keyword) DO UPDATE SET
                    last_seen_at=EXCLUDED.last_seen_at,
                    peak_channels=EXCLUDED.peak_channels,
                    classification=EXCLUDED.classification
            """, (keyword, f_dt, l_dt, peak_ch, classification))
            
        conn.commit()
    except Exception as e:
        logging.error(f"Error in classify_topic_lifespans: {e}")
    finally:
        cursor.close()
        conn.close()

def track_diffusion():
    """Detect when a topic crosses tier boundaries."""
    logging.info("Running track_diffusion...")
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        t_minus_48h = datetime.utcnow() - timedelta(hours=48)
        
        # Get earliest video published per keyword per tier
        cursor.execute("""
            SELECT k.keyword, c.tier, MIN(v.published_at) as first_pub
            FROM keywords k
            JOIN videos v ON k.video_id = v.video_id
            JOIN channels c ON k.channel_id = c.channel_id
            WHERE k.extracted_at > %s AND c.tier IS NOT NULL
            GROUP BY k.keyword, c.tier
        """, (t_minus_48h,))
        
        data = {}
        rows = cursor.fetchall()
        for row in rows:
            kw = row['keyword']
            if kw not in data: data[kw] = {}
            pub_date = row['first_pub']
            if isinstance(pub_date, str):
                data[kw][row['tier']] = datetime.fromisoformat(pub_date.replace('Z', ''))
            else:
                data[kw][row['tier']] = pub_date

        for kw, tiers in data.items():
            if 3 in tiers and 1 in tiers:
                lag = (tiers[1] - tiers[3]).total_seconds() / 3600.0
                if lag > 0:
                    cursor.execute("""
                        INSERT INTO diffusion_events (keyword, from_tier, to_tier, lag_hours)
                        VALUES (%s, 3, 1, %s)
                        ON CONFLICT(keyword, from_tier, to_tier) DO NOTHING
                    """, (kw, lag))
            if 3 in tiers and 2 in tiers:
                lag = (tiers[2] - tiers[3]).total_seconds() / 3600.0
                if lag > 0:
                    cursor.execute("""
                        INSERT INTO diffusion_events (keyword, from_tier, to_tier, lag_hours)
                        VALUES (%s, 3, 2, %s)
                        ON CONFLICT(keyword, from_tier, to_tier) DO NOTHING
                    """, (kw, lag))
            if 2 in tiers and 1 in tiers:
                lag = (tiers[1] - tiers[2]).total_seconds() / 3600.0
                if lag > 0:
                    cursor.execute("""
                        INSERT INTO diffusion_events (keyword, from_tier, to_tier, lag_hours)
                        VALUES (%s, 2, 1, %s)
                        ON CONFLICT(keyword, from_tier, to_tier) DO NOTHING
                    """, (kw, lag))

        conn.commit()
    except Exception as e:
        logging.error(f"Error in track_diffusion: {e}")
    finally:
        cursor.close()
        conn.close()

def analyze_title_linguistics():
    """Track linguistic patterns in titles over 24h."""
    logging.info("Running analyze_title_linguistics...")
    try:
        conn = get_connection()
        cursor = get_cursor(conn)
        t_minus_24h = datetime.utcnow() - timedelta(hours=24)
        
        cursor.execute("SELECT title FROM videos WHERE published_at >= %s", (t_minus_24h,))
        titles = [r['title'].lower() for r in cursor.fetchall() if r['title']]
        
        total = len(titles)
        if total == 0:
            return

        qt_cnt = 0
        urg_cnt = 0
        ind_cnt = 0
        
        for t in titles:
            if t.rstrip().endswith('?'): qt_cnt += 1
            if any(w in t.split() for w in URGENCY_WORDS): urg_cnt += 1
            if any(x in t for x in INDIVIDUAL_INDICATORS): ind_cnt += 1

        urg_ratio = urg_cnt / total
        
        cursor.execute("""
            INSERT INTO title_linguistics (question_titles, urgency_titles, named_individual_titles, total_titles, urgency_ratio)
            VALUES (%s, %s, %s, %s, %s)
        """, (qt_cnt, urg_cnt, ind_cnt, total, urg_ratio))
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error in analyze_title_linguistics: {e}")
    finally:
        cursor.close()
        conn.close()
