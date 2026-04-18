"""
Database module for YT Temperature.
Manages SQLite connections and creates tables on first run.
"""

import sqlite3
import csv
import logging

DB_PATH = "data/yt_temperature.db"

def get_connection():
    """Returns a SQLite connection with row factory enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def migrate_channels_table(conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(channels)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'affiliation_type' not in columns:
        cursor.execute("ALTER TABLE channels ADD COLUMN affiliation_type TEXT DEFAULT 'independent'")
    if 'affiliation_org' not in columns:
        cursor.execute("ALTER TABLE channels ADD COLUMN affiliation_org TEXT DEFAULT ''")
    conn.commit()

def init_db():
    """Initializes the database schema."""
    conn = get_connection()
    migrate_channels_table(conn)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_id      TEXT PRIMARY KEY,
            channel_name    TEXT NOT NULL,
            tier            INTEGER,
            category        TEXT,
            notes           TEXT,
            subscriber_count INTEGER,
            last_updated    TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            video_id        TEXT PRIMARY KEY,
            channel_id      TEXT,
            title           TEXT,
            description     TEXT,
            published_at    TIMESTAMP,
            duration        TEXT,
            tags            TEXT,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id        TEXT,
            polled_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            view_count      INTEGER,
            like_count      INTEGER,
            comment_count   INTEGER,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword         TEXT,
            video_id        TEXT,
            channel_id      TEXT,
            extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id          TEXT,
            polled_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            subscriber_count    INTEGER,
            video_count         INTEGER,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS topic_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword         TEXT NOT NULL,
            snapshot_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mention_count   INTEGER NOT NULL,
            channel_count   INTEGER NOT NULL,
            video_ids       TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS first_movers (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword         TEXT NOT NULL,
            channel_id      TEXT NOT NULL,
            video_id        TEXT NOT NULL,
            first_seen_at   TIMESTAMP NOT NULL,
            UNIQUE(keyword, channel_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_rhythm (
            channel_id          TEXT PRIMARY KEY,
            avg_daily_uploads   REAL,
            last_calculated_at  TIMESTAMP,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rhythm_alerts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id      TEXT NOT NULL,
            alerted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            uploads_today   INTEGER,
            baseline_avg    REAL,
            deviation_ratio REAL,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ecosystem_pulse (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pulse_score REAL NOT NULL,
            component_json TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS topic_lifespan (
            keyword         TEXT PRIMARY KEY,
            first_seen_at   TIMESTAMP,
            last_seen_at    TIMESTAMP,
            peak_channels   INTEGER,
            peak_at         TIMESTAMP,
            classification  TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS diffusion_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword         TEXT NOT NULL,
            from_tier       INTEGER NOT NULL,
            to_tier         INTEGER NOT NULL,
            crossed_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            lag_hours       REAL,
            trigger_video_id TEXT,
            UNIQUE(keyword, from_tier, to_tier)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS title_linguistics (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            recorded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            question_titles INTEGER,
            urgency_titles  INTEGER,
            named_individual_titles INTEGER,
            total_titles    INTEGER,
            urgency_ratio   REAL
        )
    ''')

    # MUNGER ANALYTICS LAYER

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS engagement_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id            TEXT NOT NULL,
            channel_id          TEXT NOT NULL,
            polled_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            view_count          INTEGER,
            like_count          INTEGER,
            comment_count       INTEGER,
            likes_per_view      REAL,
            comments_per_view   REAL,
            engagement_score    REAL,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_engagement_baseline (
            channel_id              TEXT PRIMARY KEY,
            avg_engagement_score    REAL,
            avg_likes_per_view      REAL,
            avg_comments_per_view   REAL,
            baseline_video_count    INTEGER,
            last_calculated_at      TIMESTAMP,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS feedback_events (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id                  TEXT NOT NULL,
            keyword                     TEXT NOT NULL,
            trigger_video_id            TEXT NOT NULL,
            trigger_engagement_score    REAL NOT NULL,
            trigger_engagement_percentile REAL NOT NULL,
            response_video_count        INTEGER NOT NULL,
            response_window_hours       INTEGER DEFAULT 72,
            detected_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_rank_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id      TEXT NOT NULL,
            week_of         DATE NOT NULL,
            velocity_score  REAL,
            velocity_rank   INTEGER,
            rank_change     INTEGER,
            UNIQUE(channel_id, week_of),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS affiliation_divergence (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword                 TEXT NOT NULL,
            recorded_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            affiliated_count        INTEGER,
            independent_count       INTEGER,
            divergence_score        REAL,
            direction               TEXT,
            total_channel_coverage  INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ecosystem_baseline_seed (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id      TEXT NOT NULL,
            seed_date       DATE NOT NULL,
            view_count      INTEGER,
            subscriber_count INTEGER,
            source          TEXT DEFAULT 'api_historical',
            UNIQUE(channel_id, seed_date)
        )
    ''')

    conn.commit()
    conn.close()
    
    load_channels()

def load_channels():
    """Loads channels from the CSV into the database if not present."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        with open("channels.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # Merge into database
                cursor.execute("""
                    INSERT INTO channels (channel_id, channel_name, tier, category, notes, affiliation_type, affiliation_org)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        channel_name=excluded.channel_name,
                        tier=excluded.tier,
                        category=excluded.category,
                        notes=excluded.notes,
                        affiliation_type=excluded.affiliation_type,
                        affiliation_org=excluded.affiliation_org
                """, (row['channel_id'], row['channel_name'], row['tier'], row['category'], row['notes'], row.get('affiliation_type', 'independent'), row.get('affiliation_org', '')))
                count += 1
            conn.commit()
            logging.info(f"Loaded {count} channels from CSV into DB.")
    except Exception as e:
        logging.error(f"Error loading channels.csv: {e}")
    finally:
        conn.close()
