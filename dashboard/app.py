"""
Flask Application for YT Temperature Dashboard
"""

from flask import Flask, render_template, jsonify, request, session, redirect, url_for
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import os
import functools
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('ADMIN_SECRET_KEY', 'change-me-in-production')
DATABASE_URL = os.getenv("DATABASE_URL")

ADMIN_USER = os.getenv('ADMIN_USER', 'admin')
ADMIN_PASS = os.getenv('ADMIN_PASS', 'polisignal')

def admin_required(f):
    """Decorator: redirect to login if not authenticated."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login', next=request.path))
        return f(*args, **kwargs)
    return decorated

def get_db():
    """Returns a PostgreSQL connection with RealDictCursor."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set.")
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def get_cursor(conn):
    """Returns a cursor with RealDictCursor."""
    return conn.cursor(cursor_factory=RealDictCursor)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/methodology')
def methodology():
    return render_template('methodology.html')

@app.route('/channels')
def channels():
    return render_template('channels.html')

@app.route('/analytics')
def analytics():
    return render_template('analytics.html')

@app.route('/api/channels_list')
def api_channels_list():
    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute("""
        SELECT channel_id, channel_name, tier, category, subscriber_count, last_updated 
        FROM channels
    """)
    channels = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify(channels)

@app.route('/api/summary')
def api_summary():
    conn = get_db()
    cursor = get_cursor(conn)
    
    # 1. Total Channels
    cursor.execute("SELECT COUNT(*) as count FROM channels")
    total_channels = cursor.fetchone()['count']
    
    # 2. Videos Last 24h
    cursor.execute("SELECT COUNT(*) as count FROM videos WHERE published_at >= NOW() - INTERVAL '24 hours'")
    videos_last_24h = cursor.fetchone()['count']
    
    # 3. Top Converging Keywords (requires filtering for n_channels >= 3 within 24 hours)
    cursor.execute("""
        SELECT keyword, COUNT(DISTINCT channel_id) as channel_count, COUNT(video_id) as video_count
        FROM keywords 
        WHERE extracted_at > NOW() - INTERVAL '24 hours' 
        GROUP BY keyword 
        HAVING COUNT(DISTINCT channel_id) >= 3 
        ORDER BY channel_count DESC 
        LIMIT 15
    """)
    top_converging_keywords = [dict(row) for row in cursor.fetchall()]

    # 4. Hottest Videos Processing (using the memory technique for velocity calculation)
    # Query videos and their last two snapshots in the last 72 hours
    cursor.execute("""
        SELECT v.video_id, v.title, v.published_at, c.channel_name, c.tier
        FROM videos v
        JOIN channels c ON v.channel_id = c.channel_id
        WHERE v.published_at > NOW() - INTERVAL '72 hours'
    """)
    recent_videos = cursor.fetchall()
    
    hottest_videos = []
    surge_alerts = []
    
    for v in recent_videos:
        cursor.execute("""
            SELECT view_count, polled_at FROM snapshots
            WHERE video_id = %s
            ORDER BY polled_at DESC
            LIMIT 2
        """, (v['video_id'],))
        snaps = cursor.fetchall()
        
        if len(snaps) >= 2:
            latest = snaps[0]
            previous = snaps[1]
            
            # PostgreSQL returns datetime objects for TIMESTAMP
            t1 = previous['polled_at']
            t2 = latest['polled_at']
            
            hours = (t2 - t1).total_seconds() / 3600
            diff = latest['view_count'] - previous['view_count']
            
            velocity = diff / hours if hours > 0 else 0
            
            vid_obj = {
                "video_id": v['video_id'],
                "title": v['title'],
                "channel_name": v['channel_name'],
                "tier": v['tier'],
                "views": latest['view_count'],
                "velocity": velocity,
                "published_at": v['published_at'].isoformat() if isinstance(v['published_at'], datetime.datetime) else v['published_at']
            }
            hottest_videos.append(vid_obj)
            
            if v['tier'] == 3 and velocity > 1000: # Base threshold for surge inclusion, we filter actual 95th later if needed
                surge_alerts.append(vid_obj)

    # Sort and slice
    hottest_videos.sort(key=lambda x: x['velocity'], reverse=True)
    hottest_videos = hottest_videos[:15]
    
    # Simplistic surge detection
    surge_alerts.sort(key=lambda x: x['velocity'], reverse=True)
    surge_alerts = surge_alerts[:5]
    
    # 5. Channel Activity
    cursor.execute("""
        SELECT c.channel_name, c.tier, COUNT(v.video_id) as videos_today
        FROM channels c
        LEFT JOIN videos v ON c.channel_id = v.channel_id AND v.published_at >= NOW() - INTERVAL '24 hours'
        GROUP BY c.channel_id, c.channel_name, c.tier
    """)
    channel_activity = [dict(row) for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify({
        "total_channels": total_channels,
        "videos_last_24h": videos_last_24h,
        "top_converging_keywords": top_converging_keywords,
        "hottest_videos": hottest_videos,
        "channel_activity": channel_activity,
        "surge_alerts": surge_alerts
    })

@app.route('/api/pulse')
def api_pulse():
    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute("SELECT * FROM ecosystem_pulse ORDER BY recorded_at DESC LIMIT 48")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if not rows:
        return jsonify({"current": 100, "baseline": 100, "delta_24h": 0, "history": [], "components": {}})

    current = rows[0]
    history = [{"recorded_at": r['recorded_at'].isoformat() if isinstance(r['recorded_at'], datetime.datetime) else r['recorded_at'], "pulse_score": r['pulse_score']} for r in rows[::-1]]
    
    delta_24h = 0
    if len(rows) >= 24:
        delta_24h = current['pulse_score'] - rows[-1]['pulse_score']

    import json
    components = json.loads(current['component_json']) if current['component_json'] else {}

    return jsonify({
        "current": current['pulse_score'],
        "baseline": 100,
        "delta_24h": delta_24h,
        "history": history,
        "components": components
    })

@app.route('/api/topics/velocity')
def api_topics_velocity():
    conn = get_db()
    cursor = get_cursor(conn)
    
    cursor.execute("""
        SELECT ts.keyword, ts.mention_count as volume,
               ((ts.mention_count - COALESCE(prev.mention_count, 0)) / GREATEST(COALESCE(prev.mention_count, 1), 1.0) * 100) as velocity,
               ts.channel_count, tl.classification
        FROM (SELECT keyword, MAX(id) as max_id FROM topic_snapshots GROUP BY keyword) curr_max
        JOIN topic_snapshots ts ON curr_max.max_id = ts.id
        LEFT JOIN (
            SELECT keyword, MAX(id) as prev_id FROM topic_snapshots 
            WHERE snapshot_at < NOW() - INTERVAL '6 hours' GROUP BY keyword
        ) prev_max ON ts.keyword = prev_max.keyword
        LEFT JOIN topic_snapshots prev ON prev_max.prev_id = prev.id
        LEFT JOIN topic_lifespan tl ON ts.keyword = tl.keyword
        WHERE ts.mention_count > 1
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    topics = []
    vols = []
    vels = []
    for r in rows:
        vols.append(r['volume'])
        vels.append(r['velocity'])
        
    if not topics and not vols:
        return jsonify({"topics": [], "quadrant_thresholds": {"volume_median": 0, "velocity_median": 0}})

    import statistics
    vol_median = statistics.median(vols) if vols else 0
    vel_median = statistics.median(vels) if vels else 0

    for r in rows:
        vol = r['volume']
        vel = r['velocity']
        if vol >= vol_median and vel >= vel_median: quad = "on_fire"
        elif vol >= vol_median and vel < vel_median: quad = "high_vol"
        elif vol < vol_median and vel >= vel_median: quad = "rising"
        else: quad = "cooling"

        topics.append({
            "keyword": r['keyword'],
            "volume": vol,
            "velocity": vel,
            "channel_count": r['channel_count'],
            "classification": r['classification'] or "flash",
            "quadrant": quad
        })

    return jsonify({
        "topics": topics,
        "quadrant_thresholds": {
            "volume_median": vol_median,
            "velocity_median": vel_median
        }
    })

@app.route('/api/topics/convergence')
def api_topics_convergence():
    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute("""
        SELECT k.keyword, k.channel_id, c.channel_name, c.tier 
        FROM keywords k
        JOIN channels c ON k.channel_id = c.channel_id
        WHERE k.extracted_at > NOW() - INTERVAL '24 hours'
    """)
    rows = cursor.fetchall()
    
    cursor.execute("SELECT COUNT(*) as c FROM channels")
    total_channels = cursor.fetchone()['c'] or 1
    cursor.close()
    conn.close()

    nodes_dict = {}
    channel_keywords = {}
    keyword_freq = {}
    
    for r in rows:
        cid = r['channel_id']
        cname = r['channel_name']
        tier = r['tier']
        kw = r['keyword']
        
        if cid not in nodes_dict:
            nodes_dict[cid] = {"id": cid, "name": cname, "tier": tier, "video_count_24h": 0}
            channel_keywords[cid] = set()
            
        nodes_dict[cid]['video_count_24h'] += 1
        channel_keywords[cid].add(kw)
        
    # Pre-calculate frequency of keywords across channels
    for cid, kws in channel_keywords.items():
        for kw in kws:
            keyword_freq[kw] = keyword_freq.get(kw, 0) + 1

    edges = []
    cids = list(channel_keywords.keys())
    for i in range(len(cids)):
        for j in range(i+1, len(cids)):
            c1 = cids[i]
            c2 = cids[j]
            shared = channel_keywords[c1].intersection(channel_keywords[c2])
            
            # Filter generic keywords exactly per prompt assignment
            valid_shared = [kw for kw in shared if keyword_freq[kw] < 0.4 * total_channels]
            
            if len(valid_shared) >= 2:
                edges.append({
                    "source": c1,
                    "target": c2,
                    "shared_keywords": valid_shared,
                    "strength": len(valid_shared)
                })

    return jsonify({
        "nodes": list(nodes_dict.values()),
        "edges": edges
    })

@app.route('/api/channels/rhythm')
def api_channels_rhythm():
    conn = get_db()
    cursor = get_cursor(conn)
    
    cursor.execute("""
        SELECT ra.channel_id, c.channel_name, c.tier, ra.uploads_today, ra.baseline_avg, ra.deviation_ratio, ra.alerted_at
        FROM rhythm_alerts ra
        JOIN channels c ON ra.channel_id = c.channel_id
        WHERE ra.alerted_at > NOW() - INTERVAL '24 hours'
        ORDER BY ra.deviation_ratio DESC
    """)
    alerts = [dict(r) for r in cursor.fetchall()]

    cursor.execute("""
        SELECT cr.channel_id, c.channel_name, c.tier, cr.avg_daily_uploads as baseline_avg,
               (SELECT COUNT(*) FROM videos v WHERE v.channel_id = cr.channel_id AND v.published_at > NOW() - INTERVAL '24 hours') as uploads_today
        FROM channel_rhythm cr
        JOIN channels c ON cr.channel_id = c.channel_id
    """)
    all_channels = []
    for r in cursor.fetchall():
        d = dict(r)
        d['deviation_ratio'] = d['uploads_today'] / d['baseline_avg'] if d['baseline_avg'] > 0 else 1
        all_channels.append(d)
        
    all_channels.sort(key=lambda x: x['deviation_ratio'], reverse=True)
    cursor.close()
    conn.close()

    return jsonify({
        "alerts": alerts,
        "all_channels": all_channels
    })

@app.route('/api/firstmovers')
def api_firstmovers():
    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute("""
        SELECT c.channel_name, c.tier, COUNT(*) as first_mover_count_30d, MAX(c.channel_id) as channel_id
        FROM first_movers fm
        JOIN channels c ON fm.channel_id = c.channel_id
        WHERE fm.first_seen_at > NOW() - INTERVAL '30 days'
        GROUP BY c.channel_name, c.tier
        ORDER BY first_mover_count_30d DESC
        LIMIT 10
    """)
    
    leaderboard = []
    rows = cursor.fetchall()
    for r in rows:
        d = dict(r)
        cursor.execute("SELECT keyword FROM first_movers WHERE channel_id = %s LIMIT 3", (d['channel_id'],))
        d['top_topics'] = [k['keyword'] for k in cursor.fetchall()]
        leaderboard.append(d)

    cursor.execute("""
        SELECT fm.keyword, c.channel_name, fm.first_seen_at, 
               (SELECT lag_hours FROM diffusion_events de WHERE de.keyword = fm.keyword AND de.from_tier=3 AND de.to_tier=1 LIMIT 1) as hours_before_tier1
        FROM first_movers fm
        JOIN channels c ON fm.channel_id = c.channel_id
        ORDER BY fm.first_seen_at DESC
        LIMIT 5
    """)
    recent = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    conn.close()

    return jsonify({"leaderboard": leaderboard, "recent": recent})

@app.route('/api/diffusion')
def api_diffusion():
    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute("""
        SELECT keyword, from_tier, to_tier, lag_hours, crossed_at
        FROM diffusion_events
        ORDER BY crossed_at DESC LIMIT 10
    """)
    recent = [dict(r) for r in cursor.fetchall()]
    
    cursor.execute("SELECT AVG(lag_hours) as a FROM diffusion_events WHERE from_tier=3 AND to_tier=2")
    t32 = cursor.fetchone()['a'] or 0
    cursor.execute("SELECT AVG(lag_hours) as a FROM diffusion_events WHERE from_tier=2 AND to_tier=1")
    t21 = cursor.fetchone()['a'] or 0
    cursor.execute("SELECT AVG(lag_hours) as a FROM diffusion_events WHERE from_tier=3 AND to_tier=1")
    t31 = cursor.fetchone()['a'] or 0
    cursor.close()
    conn.close()

    return jsonify({
        "recent_events": recent,
        "avg_lag_by_route": {
            "3_to_2": round(float(t32), 1),
            "2_to_1": round(float(t21), 1),
            "3_to_1": round(float(t31), 1)
        }
    })

@app.route('/api/linguistics')
def api_linguistics():
    conn = get_db()
    cursor = get_cursor(conn)
    cursor.execute("SELECT * FROM title_linguistics ORDER BY recorded_at DESC LIMIT 7")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if not rows:
        return jsonify({"current": {}, "history_7d": [], "urgency_delta_vs_7d_avg": 0})
        
    current = dict(rows[0])
    current['question_ratio'] = current['question_titles'] / max(current['total_titles'],1)
    current['individual_ratio'] = current['named_individual_titles'] / max(current['total_titles'],1)
    
    history = [{"recorded_at": r['recorded_at'].isoformat() if isinstance(r['recorded_at'], datetime.datetime) else r['recorded_at'], "urgency_ratio": r['urgency_ratio']} for r in rows[::-1]]
    avg_7d = sum(r['urgency_ratio'] for r in rows) / len(rows)
    delta = current['urgency_ratio'] - avg_7d

    return jsonify({
        "current": current,
        "history_7d": history,
        "urgency_delta_vs_7d_avg": delta
    })

@app.route('/api/daily_briefing')
def api_daily_briefing():
    conn = get_db()
    cursor = get_cursor(conn)

    try:
        # Schema creation is skipped as tables were assumed migrated.
        
        # Serve the most recent AI-generated briefing if one exists
        cursor.execute("""
            SELECT briefing_text, generated_at, generated_by
            FROM daily_briefings
            ORDER BY generated_at DESC
            LIMIT 1
        """)
        row = cursor.fetchone()

        if row:
            return jsonify({
                "briefing": row['briefing_text'],
                "generated_at": row['generated_at'].isoformat() if isinstance(row['generated_at'], datetime.datetime) else row['generated_at'],
                "source": row['generated_by']
            })

        # ── Fallback template until the first AI briefing runs ─────────────────
        pulse = 100
        try:
            # Use PostgreSQL JSON extraction: column::json->>'key'
            cursor.execute("SELECT pulse_score FROM ecosystem_pulse WHERE (component_json::json->>'raw_pulse') IS NOT NULL ORDER BY recorded_at DESC LIMIT 1")
            r1 = cursor.fetchone()
            if r1: pulse = r1['pulse_score']
        except Exception: pass

        top_kw, top_kw_ch = None, 0
        try:
            cursor.execute("SELECT keyword, channel_count FROM topic_snapshots ORDER BY id DESC LIMIT 1")
            r2 = cursor.fetchone()
            if r2: top_kw, top_kw_ch = r2['keyword'], r2['channel_count']
        except Exception: pass

        first_mover_channel, hours_ago = None, 0
        if top_kw:
            try:
                cursor.execute("SELECT c.channel_name, fm.first_seen_at FROM first_movers fm JOIN channels c ON fm.channel_id = c.channel_id WHERE fm.keyword = %s", (top_kw,))
                r3 = cursor.fetchone()
                if r3:
                    first_mover_channel = r3['channel_name']
                    dt = r3['first_seen_at']
                    hours_ago = (datetime.datetime.utcnow() - dt).total_seconds() / 3600
            except Exception: pass

        breakouts = []
        try:
            cursor.execute("SELECT c.channel_name FROM rhythm_alerts ra JOIN channels c ON ra.channel_id = c.channel_id WHERE ra.alerted_at > NOW() - INTERVAL '24 hours'")
            breakouts = [r['channel_name'] for r in cursor.fetchall()]
        except Exception: pass

        urg_delta = 0
        try:
            cursor.execute("SELECT urgency_ratio FROM title_linguistics ORDER BY recorded_at DESC LIMIT 7")
            lr = cursor.fetchall()
            if lr:
                urg_delta = lr[0]['urgency_ratio'] - sum(x['urgency_ratio'] for x in lr) / len(lr)
        except Exception: pass

        briefing = f"The media ecosystem is running at {pulse:.0f}% of its 30-day baseline — {'above' if pulse > 100 else 'below'} average activity.\n\n"
        if top_kw:
            briefing += f"The dominant converging topic is '{top_kw}'"
            if first_mover_channel:
                briefing += f", first covered by {first_mover_channel} {hours_ago:.0f} hours ago"
            briefing += f" and now appearing across {top_kw_ch} channels.\n\n"
        if breakouts:
            briefing += f"⚡ {len(breakouts)} channel(s) in upload surge mode: {', '.join(breakouts[:3])}.\n\n"
        if urg_delta != 0:
            briefing += f"Urgency language in titles is {'up' if urg_delta > 0 else 'down'} {abs(urg_delta * 100):.0f}% vs. the 7-day average.\n\n"
        briefing += "AI-generated briefing updates daily at 20:00 (local) via Ollama."

        return jsonify({
            "briefing": briefing.strip(),
            "generated_at": datetime.datetime.utcnow().isoformat(),
            "source": "template"
        })

    except Exception as e:
        app.logger.error(f"Briefing error: {e}")
        return jsonify({
            "briefing": "Briefing not yet available — data is still being collected.",
            "generated_at": datetime.datetime.utcnow().isoformat(),
            "source": "error"
        })
    finally:
        cursor.close()
        conn.close()


# --- MUNGER API ROUTES ---

@app.route('/api/calibration')
def api_calibration():
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        # julianday equivalent in PostgreSQL: EXTRACT(EPOCH FROM (NOW() - date)) / 86400
        cursor.execute("SELECT EXTRACT(EPOCH FROM (NOW() - MIN(seed_date))) / 86400 as d, MIN(seed_date) as sd FROM ecosystem_baseline_seed")
        r = cursor.fetchone()
        days = int(r['d']) if r and r['d'] is not None else 0
        seed_date = r['sd'].isoformat() if r and r['sd'] else ''
        
        if days < 3:
            mode = "today"
            conf = "low"
            msg = f"Baseline calibrating — {days} days of data collected. Full accuracy at 14 days."
        elif days < 14:
            mode = "week"
            conf = "partial"
            msg = f"Baseline calibrating — {days} days of data collected. Full accuracy at 14 days."
        else:
            mode = "month"
            conf = "high"
            msg = "System calibrated — 30 day rolling baseline established."
            
        return jsonify({
            "mode": mode,
            "days_of_data": days,
            "seed_date": seed_date,
            "baseline_confidence": conf,
            "message": msg,
            "metrics_affected": ["ecosystem_pulse", "heat_index", "engagement_baselines"]
        })
    finally:
        cursor.close()
        conn.close()

@app.route('/api/engagement/heat')
def api_engagement_heat():
    conn = get_db()
    cursor = get_cursor(conn)
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
        
        results = []
        ind_count = 0
        aff_count = 0
        crit = 0; hot = 0; warm = 0
        
        rows = cursor.fetchall()
        for row in rows:
            base = max(row['base_score'] or 0, 0.001)
            heat_index = row['engagement_score'] / base
            
            if heat_index >= 5.0: heat_tier = "critical"; crit += 1
            elif heat_index >= 2.0: heat_tier = "hot"; hot += 1
            elif heat_index >= 1.2: heat_tier = "warm"; warm += 1
            else: heat_tier = "normal"
            
            if row['affiliation_type'] == 'independent': ind_count += row['engagement_score']
            else: aff_count += row['engagement_score']
            
            d = dict(row)
            d['channel_baseline'] = base
            d['heat_index'] = heat_index
            d['heat_tier'] = heat_tier
            # Handle datetime objects
            if isinstance(d['published_at'], datetime.datetime):
                d['published_at'] = d['published_at'].isoformat()
            results.append(d)
            
        results.sort(key=lambda x: x['heat_index'], reverse=True)
        
        total_heat = (ind_count + aff_count) or 1
        
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat(),
            "heat_index": results[:50], # Just return top 50 
            "summary": {
                "critical_count": crit,
                "hot_count": hot,
                "warm_count": warm,
                "independent_share": ind_count / total_heat,
                "affiliated_share": aff_count / total_heat
            }
        })
    finally:
        cursor.close()
        conn.close()

@app.route('/api/engagement/channel/<channel_id>')
def api_engagement_channel(channel_id):
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        cursor.execute("SELECT channel_name, affiliation_type FROM channels WHERE channel_id=%s", (channel_id,))
        ch = cursor.fetchone()
        
        cursor.execute("SELECT * FROM channel_engagement_baseline WHERE channel_id=%s", (channel_id,))
        base = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) as c FROM feedback_events WHERE channel_id=%s AND detected_at >= NOW() - INTERVAL '30 days'", (channel_id,))
        fb = cursor.fetchone()['c']
        
        cursor.execute("""
            SELECT es.video_id, v.title, es.engagement_score, v.published_at
            FROM engagement_snapshots es
            JOIN videos v ON v.video_id = es.video_id
            WHERE es.channel_id=%s
            AND es.id IN (SELECT MAX(id) FROM engagement_snapshots GROUP BY video_id)
            ORDER BY v.published_at DESC LIMIT 10
        """, (channel_id,))
        vids = []
        base_score = float(base['avg_engagement_score']) if base else 0.001
        for v in cursor.fetchall():
            d = dict(v)
            d['heat_index'] = float(v['engagement_score']) / max(base_score, 0.001)
            if isinstance(d['published_at'], datetime.datetime):
                d['published_at'] = d['published_at'].isoformat()
            vids.append(d)
            
        return jsonify({
            "channel_name": ch['channel_name'] if ch else "Unknown",
            "affiliation_type": ch['affiliation_type'] if ch else "independent",
            "baseline": dict(base) if base else {},
            "recent_videos": vids,
            "feedback_events_30d": fb
        })
    finally:
        cursor.close()
        conn.close()

@app.route('/api/feedback')
def api_feedback():
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            SELECT f.*, c.channel_name, c.affiliation_type, c.tier, v.title as trigger_video_title
            FROM feedback_events f
            JOIN channels c ON c.channel_id = f.channel_id
            LEFT JOIN videos v ON v.video_id = f.trigger_video_id
            ORDER BY f.detected_at DESC LIMIT 20
        """)
        events = []
        for r in cursor.fetchall():
            d = dict(r)
            if isinstance(d['detected_at'], datetime.datetime):
                d['detected_at'] = d['detected_at'].isoformat()
            events.append(d)
        
        cursor.execute("""
            SELECT c.channel_name, c.affiliation_type, COUNT(f.id) as ev_count
            FROM feedback_events f
            JOIN channels c ON c.channel_id = f.channel_id
            WHERE f.detected_at >= NOW() - INTERVAL '30 days'
            GROUP BY c.channel_name, c.affiliation_type
            ORDER BY ev_count DESC LIMIT 5
        """)
        resp = []
        for r in cursor.fetchall():
            resp.append({
                "channel_name": r['channel_name'],
                "affiliation_type": r['affiliation_type'],
                "feedback_events_30d": r['ev_count']
            })
            
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN c.affiliation_type = 'independent' THEN 1 ELSE 0 END) as ind,
                SUM(CASE WHEN c.affiliation_type = 'affiliated' THEN 1 ELSE 0 END) as aff
            FROM feedback_events f
            JOIN channels c ON c.channel_id = f.channel_id
            WHERE f.detected_at >= NOW() - INTERVAL '30 days'
        """)
        tots = cursor.fetchone()
        ind = tots['ind'] or 0
        aff = tots['aff'] or 0
        ratio = ind / max(aff, 1)
        
        return jsonify({
            "recent_events": events,
            "most_feedback_responsive_channels": resp,
            "insight": f"Independent channels show {ratio:.1f}x more feedback events than affiliated channels"
        })
    finally:
        cursor.close()
        conn.close()

@app.route('/api/stability')
def api_stability():
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        today = datetime.datetime.utcnow().date()
        week_of = (today - datetime.timedelta(days=today.weekday()))
        
        cursor.execute("""
            SELECT r.*, c.channel_name, c.tier, c.affiliation_type
            FROM channel_rank_snapshots r
            JOIN channels c ON c.channel_id = r.channel_id
            WHERE r.week_of = %s
        """, (week_of,))
        cur = cursor.fetchall()
        
        med_ch = 0
        tot_ch = 0
        history = []
        
        cursor.execute("SELECT component_json FROM ecosystem_pulse WHERE (component_json::json->>'type') = 'rank_stability' ORDER BY recorded_at DESC LIMIT 10")
        for h in cursor.fetchall():
            import json
            j = json.loads(h['component_json'])
            history.append({
                "week_of": j.get('week_of', week_of.isoformat()),
                "total_rank_change": j.get("total_rank_change", 0),
                "median_rank_change": j.get("median_rank_change", 0)
            })
            
        if history:
            med_ch = history[0]['median_rank_change']
            tot_ch = history[0]['total_rank_change']
            
        interp = "locked"
        if med_ch >= 10: interp = "volatile"
        elif med_ch >= 6: interp = "unsettled"
        elif med_ch >= 3: interp = "stable"
        
        movers_raw = [dict(r) for r in cur]
        risers = sorted([r for r in movers_raw if r['rank_change'] and r['rank_change'] > 0], key=lambda x: x['rank_change'], reverse=True)
        fallers = sorted([r for r in movers_raw if r['rank_change'] and r['rank_change'] < 0], key=lambda x: x['rank_change'])
        new_entrants = [{"channel_name": r['channel_name']} for r in movers_raw if r['rank_change'] is None]
        
        for r in risers: 
            r['current_rank'] = r['velocity_rank']
            if isinstance(r['week_of'], datetime.date):
                r['week_of'] = r['week_of'].isoformat()
        for r in fallers: 
            r['current_rank'] = r['velocity_rank']
            if isinstance(r['week_of'], datetime.date):
                r['week_of'] = r['week_of'].isoformat()
        
        return jsonify({
            "current_week": week_of.isoformat(),
            "ecosystem_stability": {
                "total_rank_change": tot_ch,
                "median_rank_change": med_ch,
                "interpretation": interp
            },
            "movers": {
                "biggest_risers": risers[:5],
                "biggest_fallers": fallers[:5],
                "new_entrants": new_entrants
            },
            "history": history
        })
    finally:
        cursor.close()
        conn.close()

@app.route('/status')
def status():
    return render_template('status.html')

@app.route('/api/status')
def api_status():
    conn = get_db()
    cursor = get_cursor(conn)
    
    components = []
    now = datetime.datetime.utcnow()
    
    # ─── Component Definition ──────────────────────────────────────────
    config = [
        {
            "name": "RSS Poller",
            "table": "videos",
            "col": "published_at",
            "desc": "Fetches new video metadata from YouTube RSS feeds",
            "degraded": 120,
            "outage": 360
        },
        {
            "name": "API Snapshot Poller",
            "table": "snapshots",
            "col": "polled_at",
            "desc": "Enriches videos with view/like/comment counts from YouTube API",
            "degraded": 120,
            "outage": 360
        },
        {
            "name": "Keyword Extractor",
            "table": "keywords",
            "col": "extracted_at",
            "desc": "Extracts and stores semantic keywords from video titles and descriptions",
            "degraded": 120,
            "outage": 360
        },
        {
            "name": "Engagement Tracker",
            "table": "engagement_snapshots",
            "col": "polled_at",
            "desc": "Computes per-video engagement scores and channel baselines",
            "degraded": 180,
            "outage": 480
        },
        {
            "name": "Ecosystem Pulse",
            "table": "ecosystem_pulse",
            "col": "recorded_at",
            "desc": "Records the ecosystem-wide pulse score",
            "degraded": 180,
            "outage": 720
        }
    ]

    worst_status = "operational"

    for cfg in config:
        # 1. Last Event & Status
        # Note: For RSS, we use MAX(published_at) as instructed.
        cursor.execute(f"SELECT MAX({cfg['col']}) as last_e FROM {cfg['table']}")
        last_event = cursor.fetchone()['last_e']
        
        gap = 0
        status_val = "operational"
        if last_event:
            # PostgreSQL returns timezone-aware datetimes if configured, 
            # but usually it's naive UTC in these apps.
            if last_event.tzinfo:
                last_event = last_event.replace(tzinfo=None)
            
            gap = int((now - last_event).total_seconds() / 60)
            if gap > cfg['outage']: 
                status_val = "outage"
            elif gap > cfg['degraded']: 
                status_val = "degraded"
        else:
            status_val = "no_data"

        # Update overall status
        status_ranks = {"operational": 0, "degraded": 1, "outage": 2}
        if status_ranks.get(status_val, 0) > status_ranks.get(worst_status, 0):
            worst_status = status_val

        # 2. 90-Day Uptime Strips
        # Efficient daily aggregation
        cursor.execute(f"""
            WITH dates AS (
                SELECT generate_series(CURRENT_DATE - INTERVAL '89 days', CURRENT_DATE, '1 day')::date AS d
            )
            SELECT d.d, COUNT(t.{cfg['col']}) as n
            FROM dates d
            LEFT JOIN (
                SELECT {cfg['col']} FROM {cfg['table']} 
                WHERE {cfg['col']} >= CURRENT_DATE - INTERVAL '90 days'
            ) t ON t.{cfg['col']}::date = d.d
            GROUP BY d.d ORDER BY d.d ASC
        """)
        uptime_rows = cursor.fetchall()
        uptime_90d = []
        ok_count = 0
        for r in uptime_rows:
            stat = "ok" if r['n'] > 0 else "gap"
            if r['n'] > 0: ok_count += 1
            uptime_90d.append({
                "date": r['d'].isoformat() if isinstance(r['d'], (datetime.date, datetime.datetime)) else str(r['d']),
                "status": stat
            })

        components.append({
            "name": cfg['name'],
            "description": cfg['desc'],
            "status": status_val,
            "last_event": last_event.isoformat() if last_event else None,
            "gap_minutes": gap,
            "uptime_90d": uptime_90d,
            "uptime_pct": round((ok_count / 90) * 100, 1) if uptime_90d else 0
        })

    cursor.close()
    conn.close()

    return jsonify({
        "overall": worst_status,
        "checked_at": now.isoformat(),
        "components": components
    })


# ─── ADMIN ROUTES ─────────────────────────────────────────────────────────────

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        if username == ADMIN_USER and password == ADMIN_PASS:
            session['admin_logged_in'] = True
            next_url = request.args.get('next', '/admin')
            return redirect(next_url)
        error = 'Invalid credentials. Please try again.'
    return render_template('admin_login.html', error=error)


@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))


@app.route('/admin')
@admin_required
def admin():
    return render_template('admin.html')


@app.route('/api/system_health')
@admin_required
def api_system_health():
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        stats = {}

        cursor.execute("SELECT COUNT(*) as n FROM channels")
        stats['total_channels'] = cursor.fetchone()['n']

        cursor.execute("SELECT COUNT(*) as n FROM videos")
        stats['total_videos'] = cursor.fetchone()['n']

        cursor.execute("SELECT COUNT(*) as n FROM videos WHERE published_at >= NOW() - INTERVAL '24 hours'")
        stats['videos_24h'] = cursor.fetchone()['n']

        cursor.execute("SELECT COUNT(*) as n FROM snapshots")
        stats['total_snapshots'] = cursor.fetchone()['n']

        cursor.execute("SELECT COUNT(*) as n FROM snapshots WHERE polled_at >= NOW() - INTERVAL '24 hours'")
        stats['snapshots_24h'] = cursor.fetchone()['n']

        cursor.execute("SELECT COUNT(*) as n FROM keywords WHERE extracted_at >= NOW() - INTERVAL '24 hours'")
        stats['keywords_24h'] = cursor.fetchone()['n']

        cursor.execute("SELECT COUNT(*) as n FROM engagement_snapshots WHERE polled_at >= NOW() - INTERVAL '24 hours'")
        stats['engagement_snapshots_24h'] = cursor.fetchone()['n']

        cursor.execute("SELECT COUNT(*) as n FROM feedback_events")
        stats['total_feedback_events'] = cursor.fetchone()['n']

        cursor.execute("SELECT COUNT(*) as n FROM affiliation_divergence WHERE recorded_at >= NOW() - INTERVAL '24 hours'")
        stats['divergence_records_24h'] = cursor.fetchone()['n']

        # Last poller heartbeat: most recent snapshot write
        cursor.execute("SELECT MAX(polled_at) as last_poll FROM snapshots")
        row = cursor.fetchone()
        stats['last_poll'] = row['last_poll'].isoformat() if row and row['last_poll'] else None

        # Last RSS fetch: most recent video inserted
        cursor.execute("SELECT MAX(published_at) as last_rss FROM videos")
        row = cursor.fetchone()
        stats['last_rss'] = row['last_rss'].isoformat() if row and row['last_rss'] else None

        # Channels breakdown by tier
        cursor.execute("SELECT tier, COUNT(*) as n FROM channels GROUP BY tier ORDER BY tier")
        stats['channels_by_tier'] = {str(r['tier']): r['n'] for r in cursor.fetchall()}

        # Channels breakdown by affiliation
        cursor.execute("SELECT affiliation_type, COUNT(*) as n FROM channels GROUP BY affiliation_type")
        stats['channels_by_affiliation'] = {r['affiliation_type']: r['n'] for r in cursor.fetchall()}

        # Hourly snapshot volume over last 24h (sparkline data)
        cursor.execute("""
            SELECT TO_CHAR(polled_at, 'HH24:00') as hour, COUNT(*) as n
            FROM snapshots
            WHERE polled_at >= NOW() - INTERVAL '24 hours'
            GROUP BY hour ORDER BY hour
        """)
        stats['snapshot_sparkline'] = [dict(r) for r in cursor.fetchall()]

        stats['generated_at'] = datetime.datetime.utcnow().isoformat()
        return jsonify(stats)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/admin/channels', methods=['GET'])
@admin_required
def admin_get_channels():
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            SELECT channel_id, channel_name, tier, category, affiliation_type, affiliation_org,
                   subscriber_count, last_updated
            FROM channels ORDER BY tier ASC, channel_name ASC
        """)
        rows = [dict(r) for r in cursor.fetchall()]
        for r in rows:
            if isinstance(r['last_updated'], datetime.datetime):
                r['last_updated'] = r['last_updated'].isoformat()
        return jsonify(rows)
    finally:
        cursor.close()
        conn.close()


@app.route('/api/admin/channels', methods=['POST'])
@admin_required
def admin_add_channel():
    data = request.get_json()
    if not data or not data.get('channel_id') or not data.get('channel_name'):
        return jsonify({'error': 'channel_id and channel_name are required'}), 400

    conn = get_db()
    cursor = get_cursor(conn)
    try:
        cursor.execute("""
            INSERT INTO channels (channel_id, channel_name, tier, category, affiliation_type, affiliation_org)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(channel_id) DO UPDATE SET
                channel_name = EXCLUDED.channel_name,
                tier = EXCLUDED.tier,
                category = EXCLUDED.category,
                affiliation_type = EXCLUDED.affiliation_type,
                affiliation_org = EXCLUDED.affiliation_org
        """, (
            data['channel_id'].strip(),
            data['channel_name'].strip(),
            int(data.get('tier', 3)),
            data.get('category', 'commentator').strip(),
            data.get('affiliation_type', 'independent').strip(),
            data.get('affiliation_org', '').strip()
        ))
        conn.commit()
        return jsonify({'status': 'ok', 'channel_id': data['channel_id']}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/admin/channels/<channel_id>', methods=['DELETE'])
@admin_required
def admin_delete_channel(channel_id):
    conn = get_db()
    cursor = get_cursor(conn)
    try:
        # Verify it exists first
        cursor.execute("SELECT channel_id FROM channels WHERE channel_id = %s", (channel_id,))
        if not cursor.fetchone():
            return jsonify({'error': 'Channel not found'}), 404
        # Soft-remove: delete from channels (stops future polling) but keep historical videos/snapshots
        cursor.execute("DELETE FROM channels WHERE channel_id = %s", (channel_id,))
        conn.commit()
        return jsonify({'status': 'removed', 'channel_id': channel_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()
