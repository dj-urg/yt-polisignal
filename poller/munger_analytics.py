"""
Munger Analytics Module
Implements the Supply and Demand Framework from Kevin Munger's 'The YouTube Apparatus' (2024).
This theoretical shift reorients our tracking away from what creators naturally want to say
(supply) and heavily measures what the audience demands via intense engagement signals.
By measuring true engagement intensity rather than passive view counts, we establish leading
indicators of ideological movement and can accurately track when audience demand literally
creates creator supply.
"""

import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from statistics import median
from db import get_connection, get_cursor

logger = logging.getLogger(__name__)

def check_db_conn(conn=None):
    if not conn:
        return get_connection()
    return conn

def compute_engagement_ratios(db_conn=None):
    """
    Computes active vs passive engagement per video for fresh snapshots.
    Replaces raw counts with meaningful ratios.
    """
    logger.info("Running Munger: compute_engagement_ratios...")
    conn = check_db_conn(db_conn)
    cursor = get_cursor(conn)
    try:
        t_minus_6h = datetime.utcnow() - timedelta(hours=6)
        # Find recent snapshots that aren't in engagement_snapshots yet — 6h window for safe overlap
        cursor.execute("""
            SELECT s.video_id, v.channel_id, s.polled_at, s.view_count, s.like_count, s.comment_count
            FROM snapshots s
            JOIN videos v ON s.video_id = v.video_id
            WHERE s.polled_at > %s 
            AND NOT EXISTS (
                SELECT 1 FROM engagement_snapshots es 
                WHERE es.video_id = s.video_id AND es.polled_at = s.polled_at
            )
        """, (t_minus_6h,))
        
        rows = cursor.fetchall()
        count = 0
        for row in rows:
            vw = max(row['view_count'] or 0, 1)
            lc = row['like_count'] or 0
            cc = row['comment_count'] or 0
            
            lpv = lc / vw
            cpv = cc / vw
            score = (lc * 1.0 + cc * 3.0) / vw
            
            cursor.execute("""
                INSERT INTO engagement_snapshots 
                (video_id, channel_id, polled_at, view_count, like_count, comment_count, likes_per_view, comments_per_view, engagement_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['video_id'], row['channel_id'], row['polled_at'], row['view_count'], lc, cc, lpv, cpv, score))
            count += 1
            
        conn.commit()
        
        if count > 0:
            _update_channel_engagement_baseline(conn)
            
        logger.info(f"Processed {count} engagement snapshots.")
        return count
    except Exception as e:
        logger.error(f"Error in compute_engagement_ratios: {e}")
    finally:
        cursor.close()
        if not db_conn: conn.close()

def _update_channel_engagement_baseline(conn):
    """Inner function to compute rolling 30-day baseline."""
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            SELECT channel_id, AVG(engagement_score) as avg_score, 
                   AVG(likes_per_view) as avg_lpv, AVG(comments_per_view) as avg_cpv,
                   COUNT(DISTINCT video_id) as v_cnt
            FROM engagement_snapshots
            WHERE polled_at >= NOW() - INTERVAL '30 days'
            AND view_count >= 100
            GROUP BY channel_id
        """)
        
        rows = cursor.fetchall()
        for row in rows:
            cursor.execute("""
                INSERT INTO channel_engagement_baseline 
                (channel_id, avg_engagement_score, avg_likes_per_view, avg_comments_per_view, baseline_video_count, last_calculated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT(channel_id) DO UPDATE SET
                    avg_engagement_score=EXCLUDED.avg_engagement_score,
                    avg_likes_per_view=EXCLUDED.avg_likes_per_view,
                    avg_comments_per_view=EXCLUDED.avg_comments_per_view,
                    baseline_video_count=EXCLUDED.baseline_video_count,
                    last_calculated_at=EXCLUDED.last_calculated_at
            """, (row['channel_id'], row['avg_score'], row['avg_lpv'], row['avg_cpv'], row['v_cnt']))
        conn.commit()
    except Exception as e:
        logger.error(f"Error in _update_channel_engagement_baseline: {e}")
    finally:
        cursor.close()

def compute_engagement_heat_index(db_conn=None):
    """
    Produce a ranked list of videos by engagement intensity right now.
    """
    conn = check_db_conn(db_conn)
    cursor = get_cursor(conn)
    results = []
    try:
        cursor.execute("""
            SELECT es.video_id, v.title, es.channel_id, c.channel_name, c.affiliation_type, c.tier,
                   es.engagement_score, es.view_count, es.like_count, es.comment_count, v.published_at,
                   b.avg_engagement_score as base_score
            FROM engagement_snapshots es
            JOIN videos v ON es.video_id = v.video_id
            JOIN channels c ON es.channel_id = c.channel_id
            JOIN channel_engagement_baseline b ON es.channel_id = b.channel_id
            WHERE es.polled_at >= NOW() - INTERVAL '6 hours'
            AND es.id IN (SELECT MAX(id) FROM engagement_snapshots GROUP BY video_id)
        """)
        
        rows = cursor.fetchall()
        for row in rows:
            base = max(row['base_score'] or 0, 0.001)
            heat_index = row['engagement_score'] / base
            
            if heat_index >= 5.0: heat_tier = "critical"
            elif heat_index >= 2.0: heat_tier = "hot"
            elif heat_index >= 1.2: heat_tier = "warm"
            else: heat_tier = "normal"
            
            d = dict(row)
            d['heat_index'] = heat_index
            d['heat_tier'] = heat_tier
            results.append(d)
            
        results.sort(key=lambda x: x['heat_index'], reverse=True)
    except Exception as e:
        logger.error(f"Error in compute_engagement_heat_index: {e}")
    finally:
        cursor.close()
        if not db_conn: conn.close()
        
    return results

def detect_feedback_loops(db_conn=None):
    """
    Detects when a channel's posting behavior responds to an engagement spike.
    Demand creates Supply.
    """
    logger.info("Running Munger: detect_feedback_loops...")
    conn = check_db_conn(db_conn)
    cursor = get_cursor(conn)
    try:
        # Step 1: Find spikes
        cursor.execute("""
            SELECT es.video_id, es.channel_id, c.channel_name, es.engagement_score,
                   k.keyword, v.published_at
            FROM engagement_snapshots es
            JOIN videos v ON es.video_id = v.video_id
            JOIN keywords k ON k.video_id = es.video_id
            JOIN channels c ON c.channel_id = es.channel_id
            WHERE v.published_at > NOW() - INTERVAL '7 days'
            AND es.engagement_score > (
                SELECT AVG(engagement_score) * 1.5
                FROM engagement_snapshots es2
                WHERE es2.channel_id = es.channel_id
            )
            AND es.id IN (SELECT MAX(id) FROM engagement_snapshots GROUP BY video_id)
        """)
        spikes = cursor.fetchall()
        
        for spike in spikes:
            # Step 2: Check for response uploads
            cursor.execute("""
                SELECT COUNT(DISTINCT v.video_id) as response_count
                FROM videos v
                JOIN keywords k ON k.video_id = v.video_id
                WHERE v.channel_id = %s
                AND k.keyword = %s
                AND v.published_at > %s
                AND v.published_at < %s + INTERVAL '72 hours'
                AND v.video_id != %s
            """, (spike['channel_id'], spike['keyword'], spike['published_at'], spike['published_at'], spike['video_id']))
            response_count = cursor.fetchone()['response_count']
            
            if response_count >= 2:
                # Step 3: Compute percentile
                cursor.execute("""
                    SELECT COUNT(*)::float / GREATEST(total.cnt, 1) * 100 as pct
                    FROM engagement_snapshots es
                    CROSS JOIN (SELECT COUNT(*) as cnt FROM engagement_snapshots WHERE channel_id = %s) total
                    WHERE es.channel_id = %s
                    AND es.engagement_score <= %s
                    GROUP BY total.cnt
                """, (spike['channel_id'], spike['channel_id'], spike['engagement_score']))
                row_pct = cursor.fetchone()
                pct = row_pct['pct'] if row_pct else 0
                
                if pct > 75:
                    # Deduplicate: did we log this keyword/channel today?
                    cursor.execute("""
                        SELECT 1 FROM feedback_events 
                        WHERE channel_id = %s AND keyword = %s 
                        AND detected_at > NOW() - INTERVAL '7 days'
                    """, (spike['channel_id'], spike['keyword']))
                    
                    if not cursor.fetchone():
                        cursor.execute("""
                            INSERT INTO feedback_events 
                            (channel_id, keyword, trigger_video_id, trigger_engagement_score, trigger_engagement_percentile, response_video_count)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (spike['channel_id'], spike['keyword'], spike['video_id'], spike['engagement_score'], pct, response_count))
                        logger.info(f"[FEEDBACK] {spike['channel_name']} responded to '{spike['keyword']}' spike ({pct:.0f}th pct, score {spike['engagement_score']:.3f}) with {response_count} videos in 72h")
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error in detect_feedback_loops: {e}")
    finally:
        cursor.close()
        if not db_conn: conn.close()

def compute_rank_stability(db_conn=None):
    """
    Track weekly velocity-based rank for mobility indexing.
    """
    logger.info("Running Munger: compute_rank_stability...")
    conn = check_db_conn(db_conn)
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            SELECT v.channel_id, c.channel_name,
                   SUM(s2.view_count - s1.view_count) as weekly_view_gain
            FROM snapshots s1
            JOIN snapshots s2 ON s1.video_id = s2.video_id
            JOIN videos v ON s1.video_id = v.video_id
            JOIN channels c ON c.channel_id = v.channel_id
            WHERE s1.polled_at >= NOW() - INTERVAL '14 days'
            AND s1.polled_at < NOW() - INTERVAL '7 days'
            AND s2.polled_at >= NOW() - INTERVAL '7 days'
            AND s2.id = (SELECT MAX(id) FROM snapshots WHERE video_id = s2.video_id AND polled_at >= NOW() - INTERVAL '7 days')
            AND s1.id = (SELECT MAX(id) FROM snapshots WHERE video_id = s1.video_id AND polled_at < NOW() - INTERVAL '7 days')
            GROUP BY v.channel_id, c.channel_name
        """)
        velocities = [dict(r) for r in cursor.fetchall()]
        velocities.sort(key=lambda x: x['weekly_view_gain'] or 0, reverse=True)
        
        today = datetime.utcnow().date()
        week_of = today - timedelta(days=today.weekday())
        
        # Get prior ranks safely
        cursor.execute("""
            SELECT channel_id, velocity_rank FROM channel_rank_snapshots 
            WHERE week_of = (SELECT MAX(week_of) FROM channel_rank_snapshots WHERE week_of < %s)
        """, (week_of,))
        prior_ranks = {r['channel_id']: r['velocity_rank'] for r in cursor.fetchall()}
        
        ranked_results = []
        for rank, ch in enumerate(velocities, start=1):
            prior = prior_ranks.get(ch['channel_id'])
            change = prior - rank if prior else None
            
            cursor.execute("""
                INSERT INTO channel_rank_snapshots 
                (channel_id, week_of, velocity_score, velocity_rank, rank_change)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT(channel_id, week_of) DO UPDATE SET
                    velocity_score=EXCLUDED.velocity_score,
                    velocity_rank=EXCLUDED.velocity_rank,
                    rank_change=EXCLUDED.rank_change
            """, (ch['channel_id'], week_of, ch['weekly_view_gain'], rank, change))
            
            d = ch.copy()
            d['rank_change'] = change
            ranked_results.append(d)
        
        shifts = [abs(r['rank_change']) for r in ranked_results if r['rank_change'] is not None]
        new_entrants = [r['channel_name'] for r in ranked_results if r['rank_change'] is None]
        
        if shifts:
            total_change = sum(shifts)
            med_change = median(shifts)
            
            srt_risers = sorted([r for r in ranked_results if r['rank_change']], key=lambda x: x['rank_change'], reverse=True)
            srt_fallers = sorted([r for r in ranked_results if r['rank_change']], key=lambda x: x['rank_change'])
            
            comp_json = json.dumps({
                "type": "rank_stability",
                "total_rank_change": total_change,
                "median_rank_change": float(med_change),
                "biggest_riser": {"channel_name": srt_risers[0]['channel_name'], "rank_change": srt_risers[0]['rank_change']} if srt_risers else None,
                "biggest_faller": {"channel_name": srt_fallers[0]['channel_name'], "rank_change": srt_fallers[0]['rank_change']} if srt_fallers else None,
                "new_entrants": new_entrants
            })
            
            logger.info(f"[RANK STABILITY] Median weekly shift: {med_change:.1f} positions across {len(shifts)} channels")
            
        conn.commit()
    except Exception as e:
        logger.error(f"Error in compute_rank_stability: {e}")
    finally:
        cursor.close()
        if not db_conn: conn.close()

def compute_affiliation_divergence(db_conn=None):
    """
    Measure whether topics are emerging organically (independent) or top-down (affiliated).
    """
    logger.info("Running Munger: compute_affiliation_divergence...")
    conn = check_db_conn(db_conn)
    cursor = get_cursor(conn)
    try:
        cursor.execute("SELECT DISTINCT keyword FROM keywords WHERE extracted_at > NOW() - INTERVAL '24 hours'")
        kws = [r['keyword'] for r in cursor.fetchall()]
        
        for keyword in kws:
            cursor.execute("""
                SELECT c.affiliation_type, COUNT(DISTINCT k.channel_id) as channel_count
                FROM keywords k
                JOIN channels c ON k.channel_id = c.channel_id
                WHERE k.keyword = %s
                AND k.extracted_at > NOW() - INTERVAL '24 hours'
                GROUP BY c.affiliation_type
            """, (keyword,))
            res = {r['affiliation_type']: r['channel_count'] for r in cursor.fetchall()}
            
            aff_count = res.get('affiliated', 0)
            ind_count = res.get('independent', 0)
            tot = aff_count + ind_count
            
            if tot == 0: continue
            
            divergence = abs(aff_count - ind_count) / tot
            if ind_count > aff_count: direc = "independent_leading"
            elif aff_count > ind_count: direc = "affiliated_leading"
            else: direc = "balanced"
            
            cursor.execute("""
                INSERT INTO affiliation_divergence 
                (keyword, affiliated_count, independent_count, divergence_score, direction, total_channel_coverage)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (keyword, aff_count, ind_count, divergence, direc, tot))
            
            # Log grassroots emerging
            if direc == "independent_leading" and divergence >= 0.6 and ind_count >= 3 and aff_count <= 1:
                logger.info(f"[GRASSROOTS] '{keyword}' covered by {ind_count} independents, {aff_count} affiliates — potential agenda-setter")
                
        conn.commit()
    except Exception as e:
        logger.error(f"Error in compute_affiliation_divergence: {e}")
    finally:
        cursor.close()
        if not db_conn: conn.close()

def seed_historical_baseline(db_conn, api_key):
    """
    Cure cold start problem via YouTube API. Runs exactly once on system init.
    """
    logger.info("Running Munger: seed_historical_baseline...")
    conn = check_db_conn(db_conn)
    cursor = get_cursor(conn)
    try:
        cursor.execute("SELECT COUNT(*) as c FROM ecosystem_baseline_seed")
        count = cursor.fetchone()['c']
        if count > 0:
            logger.info("Baseline already seeded, skipping.")
            return

        import urllib.request
        cursor.execute("SELECT channel_id FROM channels")
        cids = [r['channel_id'] for r in cursor.fetchall()]
        
        # Batch by 50
        def divide_chunks(l, n): 
            for i in range(0, len(l), n): yield l[i:i + n]
            
        today = datetime.utcnow().date()
        
        SEED_LIKES_PER_VIEW    = 0.04
        SEED_COMMENTS_PER_VIEW = 0.005
        SEED_ENGAGEMENT_SCORE  = (SEED_LIKES_PER_VIEW + SEED_COMMENTS_PER_VIEW * 3)

        for chunk in list(divide_chunks(cids, 50)):
            ids_str = ",".join(chunk)
            url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={ids_str}&key={api_key}"
            try:
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req) as response:
                    data = json.loads(response.read().decode())
                    for item in data.get('items', []):
                        ch_id = item['id']
                        stats = item['statistics']
                        vw = int(stats.get('viewCount', 0))
                        sub = int(stats.get('subscriberCount', 0))
                        
                        cursor.execute("""
                            INSERT INTO ecosystem_baseline_seed
                            (channel_id, seed_date, view_count, subscriber_count, source)
                            VALUES (%s, %s, %s, %s, 'api_seed')
                            ON CONFLICT(channel_id, seed_date) DO NOTHING
                        """, (ch_id, today, vw, sub))
                        
                        # Apply synthetic baseline approximations
                        cursor.execute("""
                            INSERT INTO channel_engagement_baseline
                            (channel_id, avg_engagement_score, avg_likes_per_view,
                             avg_comments_per_view, baseline_video_count, last_calculated_at)
                            VALUES (%s, %s, %s, %s, 0, NOW())
                            ON CONFLICT(channel_id) DO NOTHING
                        """, (ch_id, SEED_ENGAGEMENT_SCORE, SEED_LIKES_PER_VIEW, SEED_COMMENTS_PER_VIEW))
                        
            except Exception as nested_e:
                logger.error(f"API baseline skip: {nested_e}")
                
        conn.commit()
    except Exception as e:
        logger.error(f"Error in seed_historical_baseline: {e}")
    finally:
        cursor.close()
        if not db_conn: conn.close()
