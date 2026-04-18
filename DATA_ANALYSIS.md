# YT PoliSignal: Data Analysis Methodology

This document outlines the analytical modeling mapped onto the raw data retrieved by the YT PoliSignal Poller. The analytical core of this dashboard uses variables derived from Kevin Munger’s *The YouTube Apparatus (2024)*, specifically shifting focus from traditional "Supply-Side" telemetry (what creators make) to "Demand-Side" telemetry (how audiences shape creator incentives).

## 1. The Supply & Demand Framework

Traditional YouTube analytics falsely assume that raw views equal popularity. Munger's framework identifies that views are a lagging indicator padded by passive algorithm feeds and auto-play logic. 

**Demand Signals** measure active friction. YT PoliSignal actively devalues passive views and tracks the active engagement gradient:
- **Likes**: Moderate explicit demand.
- **Comments**: High explicit demand.

### The Engagement Heat Index (`munger_analytics.py`)
To isolate *Demand*, the engine mathematically strips views from the equation by indexing likes and comments against a per-channel baseline.
1. The engine logs the total views, likes, and comments for a video roughly 36 minutes after publication.
2. It calculates the `likes_per_view` and `comments_per_view` ratio.
3. Because leaving a comment requires substantially more manual friction than clicking "like," comments are weighted at a sheer **3x multiplier**.
4. The system produces a single **Demand Signal Score** (0-100+) denoting how "hot" the audience reaction is compared to the channel's 30-day moving baseline.

---

## 2. Audience Feedback Loops

One of the cornerstone metrics is identifying when a creator is actively "following orders" from their highly engaged audience base. 

The `detect_feedback_loops` chron job actively scouts the historical timeline for instances where an audience explicitly rewarded a topic, and the creator pivoted to mass-produce it:
1. **The Catalyst:** A video triggers an Engagement Heat Index belonging securely in the upper 90th percentile for that specific channel.
2. **The Countdown:** The system begins a 72-hour mathematical watchdog countdown on that specific channel ID.
3. **The Proof:** If the creator uploads another video heavily utilizing the same extracted noun-phrases (e.g., "Mamluk," "Inflation") within that 72-hour window, the engine classifies this as a confirmed **Feedback Event**.

*A channel with a high frequency of "Feedback Events" is statistically captive to its audience's extreme demands.*

---

## 3. Affiliation Divergence

Not all conservative broadcasters occupy the same structural layer of the media ecosystem. YT PoliSignal breaks the 60+ channels into distinct `affiliation_type` groups:
- **Institutional / Affiliated:** Bound by structural inertia. (e.g., Fox News, Daily Wire).
- **Independent / Grassroots:** Highly agile, natively tied to rapid micro-trends. (e.g., solo commentators, streamers).

The `compute_affiliation_divergence` chron job utilizes the NLTK Natural Language Toolkit to track the specific velocity of semantic subjects. 
When a keyword begins heavily accelerating through the Grassroots matrix but has nearly zero mentions in the Institutional network, it flags a **Grassroots Divergence**. This is a powerful predictive indicator of a "bubble" topic that Fox News or Daily Wire may formally syndicate 4-5 days later.

---

## 4. Rank Stability Monitoring

Audience networks naturally churn. Rather than looking closely at minor percentage shifts in views, the system weekly logs macroscopic channel mobility.
- Each Sunday at midnight (`compute_rank_stability`), the absolute total baseline of all tier lists is sorted by volume velocity. 
- Massive vertical mobility (+7 rank slots in 7 days) usually indicates the creator successfully tapped into a massive overlapping ideological bubble. Downward mobility indicates audience exhaustion.
