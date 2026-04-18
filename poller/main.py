"""
Poller entrypoint. Manages APScheduler for polling jobs.
"""

import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
import db
import rss_poller
import api_poller

import analytics
import munger_analytics
import briefing_generator

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def initial_run():
    logging.info("Starting initial seed run...")
    rss_poller.run()
    api_poller.enrich_new_videos()
    api_poller.refresh_hot_videos()
    api_poller.refresh_channel_stats()
    
    logging.info("Starting initial analytics block...")
    analytics.compute_topic_velocity()
    analytics.detect_first_movers()
    analytics.compute_channel_rhythm()
    analytics.compute_ecosystem_pulse()
    analytics.classify_topic_lifespans()
    analytics.track_diffusion()
    analytics.analyze_title_linguistics()
    
    logging.info("Starting Munger Analytics block...")
    import os
    munger_analytics.seed_historical_baseline(None, os.getenv("YOUTUBE_API_KEY"))
    munger_analytics.compute_engagement_ratios()
    munger_analytics.compute_affiliation_divergence()
    logging.info("Initial run complete.")

if __name__ == '__main__':
    load_dotenv()
    db.init_db()
    db.load_channels()
    
    initial_run()
    
    scheduler = BlockingScheduler()
    
    # max_instances=1 ensures no overlaps
    scheduler.add_job(rss_poller.run, 'interval', minutes=30, max_instances=1)
    scheduler.add_job(api_poller.enrich_new_videos, 'interval', minutes=35, max_instances=1)
    scheduler.add_job(api_poller.refresh_hot_videos, 'interval', hours=3, max_instances=1)
    scheduler.add_job(api_poller.refresh_channel_stats, 'cron', hour=6, max_instances=1)
    
    # Analytics chron jobs safely executing independently
    scheduler.add_job(analytics.compute_topic_velocity,    'interval', minutes=32, max_instances=1)
    scheduler.add_job(analytics.detect_first_movers,       'interval', minutes=33, max_instances=1)
    scheduler.add_job(analytics.compute_channel_rhythm,    'interval', hours=1, max_instances=1)
    scheduler.add_job(analytics.compute_ecosystem_pulse,   'interval', minutes=35, max_instances=1)
    scheduler.add_job(analytics.classify_topic_lifespans,  'interval', hours=6, max_instances=1)
    scheduler.add_job(analytics.track_diffusion,           'interval', hours=3, max_instances=1)
    scheduler.add_job(analytics.analyze_title_linguistics, 'interval', hours=1, max_instances=1)
    
    # Munger Analytics jobs
    scheduler.add_job(munger_analytics.compute_engagement_ratios, 'interval', minutes=36, max_instances=1)
    scheduler.add_job(munger_analytics.compute_affiliation_divergence, 'interval', hours=3, max_instances=1)
    scheduler.add_job(munger_analytics.detect_feedback_loops, 'interval', hours=6, max_instances=1)
    scheduler.add_job(munger_analytics.compute_rank_stability, 'cron', day_of_week='sun', hour=0, minute=0, max_instances=1)

    # Nightly AI briefing — runs at 23:00 UTC using local Ollama
    scheduler.add_job(briefing_generator.generate_daily_briefing, 'cron', hour=23, minute=0, max_instances=1)
    
    logging.info("Scheduler started successfully. System is now polling.")
    scheduler.start()
