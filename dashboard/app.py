"""
Flask Application for YT Temperature Dashboard
"""

from flask import Flask, render_template, jsonify
import sqlite3
import datetime

app = Flask(__name__)
DB_PATH = "data/yt_temperature.db"

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=20.0)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/methodology')
def methodology():
    return render_template('methodology.html')

@app.route('/channels')
def channels():
    return render_template('channels.html')

@app.route('/api/channels_list')
def api_channels_list():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT channel_id, channel_name, tier, category, subscriber_count, last_updated 
        FROM channels
    """)
    channels = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(channels)

@app.route('/api/summary')
def api_summary():
    conn = get_db()
    cursor = conn.cursor()
    
    # 1. Total Channels
    cursor.execute("SELECT COUNT(*) as count FROM channels")
    total_channels = cursor.fetchone()['count']
    
    # 2. Videos Last 24h
    cursor.execute("SELECT COUNT(*) as count FROM videos WHERE published_at >= datetime('now', '-24 hours')")
    videos_last_24h = cursor.fetchone()['count']
    
    # 3. Top Converging Keywords (requires filtering for n_channels >= 3 within 24 hours)
    cursor.execute("""
        SELECT keyword, COUNT(DISTINCT channel_id) as channel_count, COUNT(video_id) as video_count
        FROM keywords 
        WHERE extracted_at > datetime('now', '-24 hours') 
        GROUP BY keyword 
        HAVING channel_count >= 3 
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
        WHERE v.published_at > datetime('now', '-72 hours')
    """)
    recent_videos = cursor.fetchall()
    
    hottest_videos = []
    surge_alerts = []
    
    for v in recent_videos:
        cursor.execute("""
            SELECT view_count, polled_at FROM snapshots
            WHERE video_id = ?
            ORDER BY polled_at DESC
            LIMIT 2
        """, (v['video_id'],))
        snaps = cursor.fetchall()
        
        if len(snaps) >= 2:
            latest = snaps[0]
            previous = snaps[1]
            
            t1 = datetime.datetime.fromisoformat(previous['polled_at'].replace('Z', ''))
            t2 = datetime.datetime.fromisoformat(latest['polled_at'].replace('Z', ''))
            
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
                "published_at": v['published_at']
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
        LEFT JOIN videos v ON c.channel_id = v.channel_id AND v.published_at >= datetime('now', '-24 hours')
        GROUP BY c.channel_id
    """)
    channel_activity = [dict(row) for row in cursor.fetchall()]

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
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ecosystem_pulse ORDER BY recorded_at DESC LIMIT 48")
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return jsonify({"current": 100, "baseline": 100, "delta_24h": 0, "history": [], "components": {}})

    current = rows[0]
    history = [{"recorded_at": r['recorded_at'], "pulse_score": r['pulse_score']} for r in rows[::-1]]
    
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
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT ts.keyword, ts.mention_count as volume,
               ((ts.mention_count - COALESCE(prev.mention_count, 0)) / MAX(COALESCE(prev.mention_count, 1), 1.0) * 100) as velocity,
               ts.channel_count, tl.classification
        FROM (SELECT keyword, MAX(id) as max_id FROM topic_snapshots GROUP BY keyword) curr_max
        JOIN topic_snapshots ts ON curr_max.max_id = ts.id
        LEFT JOIN (
            SELECT keyword, MAX(id) as prev_id FROM topic_snapshots 
            WHERE snapshot_at < datetime('now', '-6 hours') GROUP BY keyword
        ) prev_max ON ts.keyword = prev_max.keyword
        LEFT JOIN topic_snapshots prev ON prev_max.prev_id = prev.id
        LEFT JOIN topic_lifespan tl ON ts.keyword = tl.keyword
        WHERE ts.mention_count > 1
    """)
    rows = cursor.fetchall()
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
    cursor = conn.cursor()
    cursor.execute("""
        SELECT k.keyword, k.channel_id, c.channel_name, c.tier 
        FROM keywords k
        JOIN channels c ON k.channel_id = c.channel_id
        WHERE k.extracted_at > datetime('now','-24 hours')
    """)
    rows = cursor.fetchall()
    
    cursor.execute("SELECT COUNT(*) as c FROM channels")
    total_channels = cursor.fetchone()['c'] or 1
    conn.close()

    nodes_dict = {}
    channel_keywords = {}
    keyword_freq = {}
    
    for r in rows:
        cid = r['channel_id']
        cname = r['channel_name']
        tier = r['tier']
        kw = r['keyword']
        
        if cid not in list(nodes_dict.keys()):
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
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT ra.channel_id, c.channel_name, c.tier, ra.uploads_today, ra.baseline_avg, ra.deviation_ratio, ra.alerted_at
        FROM rhythm_alerts ra
        JOIN channels c ON ra.channel_id = c.channel_id
        WHERE ra.alerted_at > datetime('now', '-24 hours')
        ORDER BY ra.deviation_ratio DESC
    """)
    alerts = [dict(r) for r in cursor.fetchall()]

    cursor.execute("""
        SELECT cr.channel_id, c.channel_name, c.tier, cr.avg_daily_uploads as baseline_avg,
               (SELECT COUNT(*) FROM videos v WHERE v.channel_id = cr.channel_id AND v.published_at > datetime('now', '-24 hours')) as uploads_today
        FROM channel_rhythm cr
        JOIN channels c ON cr.channel_id = c.channel_id
    """)
    all_channels = []
    for r in cursor.fetchall():
        d = dict(r)
        d['deviation_ratio'] = d['uploads_today'] / d['baseline_avg'] if d['baseline_avg'] > 0 else 1
        all_channels.append(d)
        
    all_channels.sort(key=lambda x: x['deviation_ratio'], reverse=True)
    conn.close()

    return jsonify({
        "alerts": alerts,
        "all_channels": all_channels
    })

@app.route('/api/firstmovers')
def api_firstmovers():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.channel_name, c.tier, COUNT(*) as first_mover_count_30d
        FROM first_movers fm
        JOIN channels c ON fm.channel_id = c.channel_id
        WHERE fm.first_seen_at > datetime('now','-30 days')
        GROUP BY c.channel_id
        ORDER BY first_mover_count_30d DESC
        LIMIT 10
    """)
    
    leaderboard = []
    for r in cursor.fetchall():
        d = dict(r)
        cursor.execute("SELECT keyword FROM first_movers WHERE channel_id = (SELECT channel_id FROM channels WHERE channel_name=?) LIMIT 3", (d['channel_name'],))
        d['top_topics'] = [k[0] for k in cursor.fetchall()]
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
    conn.close()

    return jsonify({"leaderboard": leaderboard, "recent": recent})

@app.route('/api/diffusion')
def api_diffusion():
    conn = get_db()
    cursor = conn.cursor()
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
    conn.close()

    return jsonify({
        "recent_events": recent,
        "avg_lag_by_route": {
            "3_to_2": round(t32, 1),
            "2_to_1": round(t21, 1),
            "3_to_1": round(t31, 1)
        }
    })

@app.route('/api/linguistics')
def api_linguistics():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM title_linguistics ORDER BY recorded_at DESC LIMIT 7")
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        return jsonify({"current": {}, "history_7d": [], "urgency_delta_vs_7d_avg": 0})
        
    current = dict(rows[0])
    current['question_ratio'] = current['question_titles'] / max(current['total_titles'],1)
    current['individual_ratio'] = current['named_individual_titles'] / max(current['total_titles'],1)
    
    history = [{"recorded_at": r['recorded_at'], "urgency_ratio": r['urgency_ratio']} for r in rows[::-1]]
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
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT pulse_score FROM ecosystem_pulse ORDER BY recorded_at DESC LIMIT 1")
        r1 = cursor.fetchone()
        pulse = r1['pulse_score'] if r1 else 100
        
        cursor.execute("SELECT keyword, channel_count FROM topic_snapshots ORDER BY id DESC LIMIT 1")
        r2 = cursor.fetchone()
        top_kw = r2['keyword'] if r2 else "unknown"
        top_kw_ch = r2['channel_count'] if r2 else 0
        
        cursor.execute("SELECT c.channel_name, fm.first_seen_at FROM first_movers fm JOIN channels c ON fm.channel_id = c.channel_id WHERE fm.keyword = ?", (top_kw,))
        r3 = cursor.fetchone()
        first_mover_channel = r3['channel_name'] if r3 else "Unknown"
        hours_ago = 0
        if r3:
            import datetime
            dt = datetime.datetime.fromisoformat(r3['first_seen_at'].replace('Z',''))
            hours_ago = (datetime.datetime.utcnow() - dt).total_seconds() / 3600
        
        cursor.execute("SELECT c.channel_name FROM rhythm_alerts ra JOIN channels c ON ra.channel_id = c.channel_id WHERE c.tier = 3 AND ra.alerted_at > datetime('now','-24 hours')")
        breakouts = [r[0] for r in cursor.fetchall()]
        
        cursor.execute("SELECT urgency_ratio FROM title_linguistics ORDER BY recorded_at DESC LIMIT 7")
        lr = cursor.fetchall()
        urg_cur = lr[0]['urgency_ratio'] if lr else 0
        urg_avg = sum(x['urgency_ratio'] for x in lr)/max(len(lr),1) if lr else 0
        urg_delta = urg_cur - urg_avg
        
        cursor.execute("SELECT keyword, lag_hours FROM diffusion_events WHERE from_tier=3 AND to_tier=1 ORDER BY crossed_at DESC LIMIT 1")
        diff = cursor.fetchone()
        cursor.execute("SELECT AVG(lag_hours) as a FROM diffusion_events WHERE from_tier=3 AND to_tier=1")
        avg_lag = cursor.fetchone()['a'] or 0
        
        briefing = f"Today the conservative media ecosystem is running at {pulse:.0f}% of its 30-day baseline — {'above' if pulse > 100 else 'below'} average activity.\n\n"
        briefing += f"The dominant converging topic is '{top_kw}', first covered by {first_mover_channel} {hours_ago:.0f} hours ago and now appearing across {top_kw_ch} channels.\n\n"
        if breakouts:
            briefing += f"⚡ {len(breakouts)} Tier 3 channel(s) are in breakout mode: {', '.join(breakouts[:3])}.\n\n"
        briefing += f"Urgency language in titles is {'up' if urg_delta > 0 else 'down'} {abs(urg_delta * 100):.0f}% vs. the 7-day average.\n\n"
        if diff and diff['lag_hours'] < avg_lag:
            briefing += f"🚨 Breaking pattern: '{diff['keyword']}' crossed from Tier 3 to Tier 1 in just {round(diff['lag_hours'],1)} hours — faster than the {round(avg_lag,1)}h average."
    except Exception as e:
        briefing = "Intelligence briefing temporarily unavailable while polling syncs."

    conn.close()

    import datetime
    return jsonify({
        "briefing": briefing.strip(),
        "generated_at": datetime.datetime.utcnow().isoformat()
    })

# --- MUNGER API ROUTES ---

@app.route('/api/calibration')
def api_calibration():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT julianday('now') - julianday(MIN(seed_date)) as d, MIN(seed_date) as sd FROM ecosystem_baseline_seed")
        r = cursor.fetchone()
        days = int(r['d']) if r and r['d'] else 0
        seed_date = r['sd'] if r and r['sd'] else ''
        
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
        conn.close()

@app.route('/api/engagement/heat')
def api_engagement_heat():
    import sys
    sys.path.append('../poller')
    # Because munger_analytics sits in poller, we query DB directly here
    # Actually, heat index translates directly to a DB read logic per the prompt
    
    conn = get_db()
    cursor = conn.cursor()
    try:
        # Prompt: Return sorted list of dicts with video metadata, heat_index, heat_tier, channel affiliation_type
        cursor.execute("""
            SELECT es.video_id, v.title, es.channel_id, c.channel_name, c.affiliation_type, c.tier,
                   es.engagement_score, es.view_count, es.like_count, es.comment_count, v.published_at,
                   b.avg_engagement_score as base_score
            FROM engagement_snapshots es
            JOIN videos v ON es.video_id = v.video_id
            JOIN channels c ON es.channel_id = c.channel_id
            JOIN channel_engagement_baseline b ON es.channel_id = b.channel_id
            WHERE es.polled_at >= datetime('now', '-6 hours')
            AND es.id IN (SELECT MAX(id) FROM engagement_snapshots GROUP BY video_id)
        """)
        
        results = []
        ind_count = 0
        aff_count = 0
        crit = 0; hot = 0; warm = 0
        
        for row in cursor.fetchall():
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
            results.append(d)
            
        results.sort(key=lambda x: x['heat_index'], reverse=True)
        
        total_heat = (ind_count + aff_count) or 1
        
        import datetime
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
        conn.close()

@app.route('/api/engagement/channel/<channel_id>')
def api_engagement_channel(channel_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT channel_name, affiliation_type FROM channels WHERE channel_id=?", (channel_id,))
        ch = cursor.fetchone()
        
        cursor.execute("SELECT * FROM channel_engagement_baseline WHERE channel_id=?", (channel_id,))
        base = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) as c FROM feedback_events WHERE channel_id=? AND detected_at >= datetime('now', '-30 days')", (channel_id,))
        fb = cursor.fetchone()['c']
        
        cursor.execute("""
            SELECT es.video_id, v.title, es.engagement_score, v.published_at
            FROM engagement_snapshots es
            JOIN videos v ON v.video_id = es.video_id
            WHERE es.channel_id=?
            AND es.id IN (SELECT MAX(id) FROM engagement_snapshots GROUP BY video_id)
            ORDER BY v.published_at DESC LIMIT 10
        """, (channel_id,))
        vids = []
        base_score = float(base['avg_engagement_score']) if base else 0.001
        for v in cursor.fetchall():
            d = dict(v)
            d['heat_index'] = v['engagement_score'] / max(base_score, 0.001)
            vids.append(d)
            
        return jsonify({
            "channel_name": ch['channel_name'] if ch else "Unknown",
            "affiliation_type": ch['affiliation_type'] if ch else "independent",
            "baseline": dict(base) if base else {},
            "recent_videos": vids,
            "feedback_events_30d": fb
        })
    finally:
        conn.close()

@app.route('/api/feedback')
def api_feedback():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT f.*, c.channel_name, c.affiliation_type, c.tier, v.title as trigger_video_title
            FROM feedback_events f
            JOIN channels c ON c.channel_id = f.channel_id
            LEFT JOIN videos v ON v.video_id = f.trigger_video_id
            ORDER BY f.detected_at DESC LIMIT 20
        """)
        events = [dict(r) for r in cursor.fetchall()]
        
        cursor.execute("""
            SELECT c.channel_name, c.affiliation_type, COUNT(f.id) as ev_count
            FROM feedback_events f
            JOIN channels c ON c.channel_id = f.channel_id
            WHERE f.detected_at >= datetime('now', '-30 days')
            GROUP BY f.channel_id
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
            WHERE f.detected_at >= datetime('now', '-30 days')
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
        conn.close()

@app.route('/api/stability')
def api_stability():
    conn = get_db()
    cursor = conn.cursor()
    try:
        import datetime
        today = datetime.datetime.utcnow().date()
        week_of = (today - datetime.timedelta(days=today.weekday())).isoformat()
        
        cursor.execute("""
            SELECT r.*, c.channel_name, c.tier, c.affiliation_type
            FROM channel_rank_snapshots r
            JOIN channels c ON c.channel_id = r.channel_id
            WHERE r.week_of = ?
        """, (week_of,))
        cur = cursor.fetchall()
        
        med_ch = 0
        tot_ch = 0
        history = []
        
        cursor.execute("SELECT component_json FROM ecosystem_pulse WHERE json_extract(component_json, '$.type') = 'rank_stability' ORDER BY recorded_at DESC LIMIT 10")
        for h in cursor.fetchall():
            import json
            j = json.loads(h['component_json'])
            history.append({
                "week_of": j.get('week_of', week_of), # pulse doesn't store week_of, it's just history
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
        
        risers = sorted([dict(r) for r in cur if r['rank_change'] and r['rank_change'] > 0], key=lambda x: x['rank_change'], reverse=True)
        fallers = sorted([dict(r) for r in cur if r['rank_change'] and r['rank_change'] < 0], key=lambda x: x['rank_change'])
        new_entrants = [{"channel_name": r['channel_name']} for r in cur if r['rank_change'] is None]
        
        for r in risers: r['current_rank'] = r['velocity_rank']
        for r in fallers: r['current_rank'] = r['velocity_rank']
        
        return jsonify({
            "current_week": week_of,
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
        conn.close()

@app.route('/api/divergence')
def api_divergence():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT d.*, 
            (SELECT COUNT(DISTINCT channel_id) FROM keywords WHERE keyword=d.keyword) as hs
            FROM affiliation_divergence d
            WHERE d.id IN (SELECT MAX(id) FROM affiliation_divergence GROUP BY keyword)
        """)
        rows = [dict(r) for r in cursor.fetchall()]
        
        grass = [r for r in rows if r['direction'] == 'independent_leading' and r['divergence_score'] >= 0.6 and r['independent_count'] >= 3 and r['affiliated_count'] <= 1]
        aff = [r for r in rows if r['direction'] == 'affiliated_leading' and r['divergence_score'] >= 0.5]
        bal = [r for r in rows if r['direction'] == 'balanced']
        
        grass = sorted(grass, key=lambda x: x['independent_count'], reverse=True)
        aff = sorted(aff, key=lambda x: x['affiliated_count'], reverse=True)
        bal = sorted(bal, key=lambda x: x['total_channel_coverage'], reverse=True)
        
        import datetime
        return jsonify({
            "updated_at": datetime.datetime.utcnow().isoformat(),
            "grassroots_emerging": grass[:10],
            "affiliated_pushing": aff[:10],
            "balanced": bal[:10],
            "munger_insight": f"{len(grass)} topics currently emerging from independents with no affiliated coverage — potential agenda-setters"
        })
    finally:
        conn.close()
