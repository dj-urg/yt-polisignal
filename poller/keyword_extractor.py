"""
Keyword Extractor Module.
Pure Python, offline tracking of textual convergence.
"""

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
import logging
from db import get_connection, get_cursor

EXTRA_STOPWORDS = {"watch", "video", "new", "today", "breaking",
                   "live", "show", "episode", "week", "year", "says",
                   "news", "official", "full", "latest"}

try:
    stop_words = set(stopwords.words('english')).union(EXTRA_STOPWORDS)
except LookupError:
    nltk.download('stopwords')
    nltk.download('punkt')
    stop_words = set(stopwords.words('english')).union(EXTRA_STOPWORDS)

def clean_text(text):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    tokens = word_tokenize(text)
    filtered = [word for word in tokens if word not in stop_words and len(word) > 2]
    return filtered

def extract_and_store(video_id, channel_id, title, description):
    """Tokenize video fields and store into keywords database."""
    conn = get_connection()
    cursor = get_cursor(conn)
    
    combined_text = title + " " + (description[:300] if description else "")
    tokens = clean_text(combined_text)
    
    # Store Unigrams
    extracted_keywords = set()
    for token in tokens:
        extracted_keywords.add(token)
        
    # Store Bigrams
    if len(tokens) > 1:
        bigrams = nltk.bigrams(tokens)
        for bigram in bigrams:
            extracted_keywords.add(f"{bigram[0]} {bigram[1]}")
            
    try:
        # For simplicity directly insert keywords. We will filter frequency=1 in the dashboard/DB queries.
        for kw in extracted_keywords:
            cursor.execute("""
                INSERT INTO keywords (keyword, video_id, channel_id) 
                VALUES (%s, %s, %s)
            """, (kw, video_id, channel_id))
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting keywords for {video_id}: {e}")
    finally:
        cursor.close()
        conn.close()

def run():
    # Job runner if ever needed for retroactive execution
    pass
