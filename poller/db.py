"""
Database module for YT Temperature.
Manages PostgreSQL connections.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import csv
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.getenv("DATABASE_URL")
CSV_PATH = os.path.join(BASE_DIR, "channels.csv")

def get_connection():
    """Returns a PostgreSQL connection with RealDictCursor enabled."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set.")
    
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def get_cursor(conn):
    """Returns a cursor with RealDictCursor."""
    return conn.cursor(cursor_factory=RealDictCursor)

def init_db():
    """
    Initializes the database schema if needed. 
    Note: Schema is assumed to exist per user instructions.
    This function remains as a reference for the required schema.
    """
    # Table creation logic is skipped as per user instructions
    # but we keep the structure for compatibility if needed.
    pass

def load_channels():
    """Loads channels from the CSV into the database if not present."""
    conn = get_connection()
    cursor = get_cursor(conn)
    try:
        if not os.path.exists(CSV_PATH):
            logging.warning(f"CSV file not found at {CSV_PATH}")
            return

        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # Merge into database using PostgreSQL syntax
                cursor.execute("""
                    INSERT INTO channels (channel_id, channel_name, tier, category, notes, affiliation_type, affiliation_org)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        channel_name=EXCLUDED.channel_name,
                        tier=EXCLUDED.tier,
                        category=EXCLUDED.category,
                        notes=EXCLUDED.notes,
                        affiliation_type=EXCLUDED.affiliation_type,
                        affiliation_org=EXCLUDED.affiliation_org
                """, (
                    row['channel_id'], 
                    row['channel_name'], 
                    row['tier'] if row['tier'] else None, 
                    row['category'], 
                    row['notes'], 
                    row.get('affiliation_type', 'independent'), 
                    row.get('affiliation_org', '')
                ))
                count += 1
            conn.commit()
            logging.info(f"Loaded {count} channels from CSV into DB.")
    except Exception as e:
        logging.error(f"Error loading channels.csv: {e}")
    finally:
        cursor.close()
        conn.close()
