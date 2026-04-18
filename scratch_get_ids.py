import urllib.request
import urllib.parse
import re
import csv
import time

channels = [
    # Tier 1 - Major Networks (20)
    ("Fox News", 1, "network", "Flagship conservative network"),
    ("Daily Wire", 1, "network", "Major conservative media company"),
    ("Newsmax", 1, "network", "Conservative news network"),
    ("One America News Network", 1, "network", "Conservative news network"),
    ("BlazeTV", 1, "network", "Glenn Beck's network"),
    ("The First TV", 1, "network", "Conservative commentary network"),
    ("Fox Business", 1, "network", "Conservative business news"),
    ("Right Side Broadcasting Network", 1, "network", "Trump rallies and news"),
    ("CBN News", 1, "faith", "Christian conservative news"),
    ("Breitbart", 1, "news", "Conservative news outlet"),
    ("The Daily Signal", 1, "news", "Heritage Foundation news outlet"),
    ("Wall Street Journal", 1, "news", "Conservative leaning financial news"),
    ("Sky News Australia", 1, "news", "Often covers US conservative politics"),
    ("New York Post", 1, "news", "Conservative leaning newspaper"),
    ("The Washington Times", 1, "news", "Conservative daily newspaper"),
    ("Real America's Voice", 1, "network", "Conservative television network"),
    ("PragerU", 1, "culture", "Conservative educational videos"),
    ("NTD News", 1, "news", "Epoch Times affiliated network"),
    ("Epoch Times", 1, "news", "Conservative newspaper"),
    ("Salem News Channel", 1, "network", "Conservative radio network"),

    # Tier 2 - Established commentators (20)
    ("Ben Shapiro", 2, "commentator", "Daily Wire founder"),
    ("Timcast IRL", 2, "commentator", "Tim Pool's nightly show"),
    ("Matt Walsh", 2, "commentator", "Daily Wire host"),
    ("Glenn Beck", 2, "commentator", "Blaze Media founder"),
    ("Steven Crowder", 2, "commentator", "Louder with Crowder"),
    ("Mark Levin", 2, "commentator", "Conservative radio host"),
    ("Dan Bongino", 2, "commentator", "Conservative podcaster"),
    ("Megyn Kelly", 2, "commentator", "Former Fox News anchor"),
    ("Michael Knowles", 2, "commentator", "Daily Wire host"),
    ("Candace Owens", 2, "commentator", "Conservative commentator"),
    ("Charlie Kirk", 2, "commentator", "Turning Point USA founder"),
    ("Tucker Carlson", 2, "commentator", "Former Fox News host"),
    ("Dave Rubin", 2, "commentator", "The Rubin Report"),
    ("Andrew Klavan", 2, "commentator", "Daily Wire host"),
    ("Brett Cooper", 2, "commentator", "Daily Wire host (The Comments Section)"),
    ("Lauren Chen", 2, "commentator", "BlazeTV contributor"),
    ("Viva Frei", 2, "commentator", "Conservative legal analyst"),
    ("Patrick Bet-David", 2, "economics", "Valuetainment founder"),
    ("Bill O'Reilly", 2, "commentator", "Former Fox News anchor"),
    ("Dinesh D'Souza", 2, "commentator", "Conservative filmmaker"),

    # Tier 3 - Rising / Niche / Alternative (20)
    ("The Quartering", 3, "culture", "Anti-woke pop culture commentator"),
    ("The Officer Tatum", 3, "commentator", "Conservative political commentator"),
    ("Amala Ekpunobi", 3, "culture", "PragerU personality"),
    ("HodgeTwins", 3, "satire", "Conservative comedy duo"),
    ("AwakenWithJP", 3, "satire", "Conservative satirical comedian"),
    ("Black Conservative Perspective", 3, "commentator", "Conservative news commentary"),
    ("Russell Brand", 3, "commentator", "Anti-establishment commentator"),
    ("Redacted", 3, "news", "Alternative news with Clayton Morris"),
    ("Sydney Watson", 3, "culture", "Conservative political and social commentator"),
    ("Liberal Hivemind", 3, "commentator", "Conservative news reaction channel"),
    ("Salty Cracker", 3, "commentator", "Vocal conservative commentary"),
    ("Actual Justice Warrior", 3, "commentator", "Criminal justice commentary"),
    ("Don't Walk, Run! Productions", 3, "news", "Conservative political reporting"),
    ("Mr Reagan", 3, "commentator", "Conservative political commentary"),
    ("Nuance Bro", 3, "commentator", "On-the-ground conservative interviews"),
    ("Elijah Schaffer", 3, "commentator", "Slightly Offensive podcast"),
    ("Anthony Brian Logan", 3, "commentator", "Conservative political commentary"),
    ("Robert Gouveia", 3, "commentator", "Conservative legal analysis"),
    ("The Lotus Eaters", 3, "culture", "Carl Benjamin's podcast"),
    ("Fleccas Talks", 3, "culture", "Conservative street interviews"),
]

def get_channel_id(name):
    query = urllib.parse.quote(name)
    url = f"https://www.youtube.com/results?search_query={query}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            html = response.read().decode('utf-8')
            match = re.search(r'"channelId":"(UC[a-zA-Z0-9_\-]{22})"', html)
            if match:
                return match.group(1)
            else:
                print(f"Could not find ID for {name}")
                return None
    except Exception as e:
        print(f"Error for {name}: {e}")
        return None

with open('channels.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['channel_id', 'channel_name', 'tier', 'category', 'notes'])
    for name, tier, cat, notes in channels:
        print(f"Fetching {name}...")
        cid = get_channel_id(name)
        if cid:
            writer.writerow([cid, name, tier, cat, notes])
        else:
            # Fallback to some placeholder if absolutely needed or skip
            writer.writerow([f"UC_UNKNOWN_{name.replace(' ', '')}", name, tier, cat, notes])
        time.sleep(1) # Be nice
    
print("Done writing channels.csv")
