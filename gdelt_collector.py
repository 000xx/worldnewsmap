"""
GDELT Collector — Phase 2
Fetches the latest GDELT 2.0 event update (15-min CSV), parses events,
filters by quality thresholds, and groups by location.
"""

import csv
import io
import zipfile
import urllib.request
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ── CAMEO Root Code → Category Mapping ──────────────────────────
CAMEO_CATEGORY_MAP = {
    # Verbal Cooperation
    "01": "politics",       # Make public statement
    "02": "politics",       # Appeal
    "03": "politics",       # Express intent to cooperate
    "04": "politics",       # Consult
    "05": "politics",       # Engage in diplomatic cooperation
    "06": "positive",       # Engage in material cooperation
    "07": "economy",        # Provide aid
    "08": "positive",       # Yield / Concede
    # Verbal Conflict
    "09": "politics",       # Investigate
    "10": "politics",       # Demand
    "11": "politics",       # Disapprove
    "12": "politics",       # Reject
    "13": "conflict",       # Threaten
    "14": "conflict",       # Protest
    # Material Conflict
    "15": "conflict",       # Exhibit military posture
    "16": "conflict",       # Reduce relations
    "17": "conflict",       # Coerce
    "18": "conflict",       # Assault
    "19": "conflict",       # Fight — Use conventional military force
    "20": "conflict",       # Fight — Use unconventional mass violence
}

# Sub-category refinements (EventBaseCode level)
CAMEO_SUBCAT_OVERRIDES = {
    "0231": "humanitarian",  # Appeal for humanitarian aid
    "0233": "humanitarian",  # Appeal for aid
    "0234": "humanitarian",  # Appeal for economic aid
    "0253": "economy",       # Appeal for economic cooperation
    "0331": "humanitarian",  # Express intent to provide humanitarian aid
    "0613": "humanitarian",  # Provide humanitarian aid
    "0614": "economy",       # Provide economic aid
    "0710": "economy",       # Provide economic cooperation
    "0711": "economy",       # Provide economic aid
    "0712": "economy",       # Provide military aid
    "0814": "economy",       # Ease economic sanctions
    "1411": "politics",      # Demonstrate / rally
    "1412": "politics",      # Conduct hunger strike
    "1413": "conflict",      # Conduct strike / boycott
    "1414": "conflict",      # Obstruct passage
    "1724": "economy",       # Impose embargo or sanctions
}

# Additional tone-based overrides
def classify_event(event_code, goldstein, avg_tone):
    """Map a CAMEO event code + scores to a category list."""
    categories = []

    # Check sub-category overrides first
    for prefix, cat in CAMEO_SUBCAT_OVERRIDES.items():
        if event_code.startswith(prefix):
            categories.append(cat)
            break

    # Fall back to root code
    if not categories:
        root = event_code[:2]
        cat = CAMEO_CATEGORY_MAP.get(root, "politics")
        categories.append(cat)

    # Add secondary categories based on tone / goldstein
    if avg_tone > 3.0 and goldstein > 3.0 and "positive" not in categories:
        categories.append("positive")
    if goldstein < -7.0 and "conflict" not in categories:
        categories.append("conflict")

    return categories


# ── GDELT Column Indices (2.0 Event Table) ──────────────────────
COL = {
    "GlobalEventID": 0,
    "Day": 1,
    "Actor1Name": 6,
    "Actor1CountryCode": 7,
    "Actor2Name": 16,
    "Actor2CountryCode": 17,
    "IsRootEvent": 26,
    "EventCode": 27,
    "EventBaseCode": 28,
    "EventRootCode": 29,
    "QuadClass": 30,
    "GoldsteinScale": 31,
    "NumMentions": 32,
    "NumSources": 33,
    "NumArticles": 34,
    "AvgTone": 35,
    "Actor1Geo_Type": 36,
    "Actor1Geo_FullName": 37,
    "Actor1Geo_CountryCode": 38,
    "Actor1Geo_Lat": 40,
    "Actor1Geo_Long": 41,
    "Actor2Geo_Type": 42,
    "Actor2Geo_FullName": 43,
    "Actor2Geo_CountryCode": 44,
    "Actor2Geo_Lat": 46,
    "Actor2Geo_Long": 47,
    "ActionGeo_Type": 48,
    "ActionGeo_FullName": 49,
    "ActionGeo_CountryCode": 50,
    "ActionGeo_ADM1Code": 51,
    "ActionGeo_Lat": 52,
    "ActionGeo_Long": 53,
    "ActionGeo_FeatureID": 54,
    "DATEADDED": 55,
    "SOURCEURL": 57,
}


def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def get_latest_gdelt_url():
    """
    Construct the URL for the latest GDELT 2.0 export file.
    GDELT updates every 15 minutes. Files are at:
    http://data.gdeltproject.org/gdeltv2/<YYYYMMDDHHMMSS>.export.CSV.zip
    We try the most recent 15-min mark and fall back.
    """
    now = datetime.now(timezone.utc)
    # Round down to nearest 15 min
    minute = (now.minute // 15) * 15
    base_time = now.replace(minute=minute, second=0, microsecond=0)

    urls = []
    for offset in range(0, 5):  # Try current and 4 previous windows
        t = base_time - timedelta(minutes=15 * offset)
        stamp = t.strftime("%Y%m%d%H%M%S")
        urls.append(f"http://data.gdeltproject.org/gdeltv2/{stamp}.export.CSV.zip")
    return urls


def fetch_gdelt_csv(urls=None):
    """Download and extract the latest GDELT CSV. Returns list of rows."""
    if urls is None:
        urls = get_latest_gdelt_url()

    for url in urls:
        try:
            print(f"  Trying {url}...")
            req = urllib.request.Request(url, headers={"User-Agent": "WorldNewsMap/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()

            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as f:
                    text = f.read().decode("utf-8", errors="replace")

            rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
            print(f"  ✓ Fetched {len(rows)} events from {url}")
            return rows, url
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            continue

    print("  ✗ All GDELT URLs failed")
    return [], None


def parse_events(rows, min_sources=5):
    """
    Parse raw GDELT rows into structured event dicts.
    Filters: must have action geo coordinates, NumSources >= threshold.
    """
    events = []

    for row in rows:
        if len(row) < 58:
            continue

        num_sources = safe_int(row[COL["NumSources"]])
        if num_sources < min_sources:
            continue

        lat = safe_float(row[COL["ActionGeo_Lat"]])
        lng = safe_float(row[COL["ActionGeo_Long"]])
        if lat == 0.0 and lng == 0.0:
            continue

        event_code = row[COL["EventCode"]].strip()
        goldstein = safe_float(row[COL["GoldsteinScale"]])
        avg_tone = safe_float(row[COL["AvgTone"]])

        categories = classify_event(event_code, goldstein, avg_tone)

        geo_name = row[COL["ActionGeo_FullName"]].strip()
        country_code = row[COL["ActionGeo_CountryCode"]].strip()

        # Parse city and country from FullName (format: "City, State, Country")
        parts = [p.strip() for p in geo_name.split(",")]
        city = parts[0] if parts else geo_name
        country = parts[-1] if len(parts) > 1 else country_code

        events.append({
            "id": row[COL["GlobalEventID"]],
            "lat": lat,
            "lng": lng,
            "city": city,
            "country": country,
            "country_code": country_code,
            "geo_name": geo_name,
            "event_code": event_code,
            "quad_class": safe_int(row[COL["QuadClass"]]),
            "goldstein": goldstein,
            "num_mentions": safe_int(row[COL["NumMentions"]]),
            "num_sources": num_sources,
            "num_articles": safe_int(row[COL["NumArticles"]]),
            "avg_tone": avg_tone,
            "categories": categories,
            "source_url": row[COL["SOURCEURL"]].strip() if len(row) > COL["SOURCEURL"] else "",
            "is_root": safe_int(row[COL["IsRootEvent"]]),
            "actor1_name": row[COL["Actor1Name"]].strip(),
            "actor2_name": row[COL["Actor2Name"]].strip(),
            "date_added": row[COL["DATEADDED"]].strip(),
        })

    print(f"  ✓ Parsed {len(events)} events (min {min_sources} sources)")
    return events


def cluster_events(events, precision=1):
    """
    Group events by rounded coordinates to merge nearby events into hotspots.
    precision=1 means round to 1 decimal place (~11km clusters).
    """
    clusters = defaultdict(lambda: {
        "events": [],
        "lat_sum": 0, "lng_sum": 0,
        "total_sources": 0, "total_mentions": 0, "total_articles": 0,
        "tone_sum": 0, "goldstein_sum": 0,
        "categories": defaultdict(int),
        "source_urls": [],
        "cities": defaultdict(int),
        "countries": defaultdict(int),
    })

    for ev in events:
        key = (round(ev["lat"], precision), round(ev["lng"], precision))
        c = clusters[key]
        c["events"].append(ev)
        c["lat_sum"] += ev["lat"]
        c["lng_sum"] += ev["lng"]
        c["total_sources"] += ev["num_sources"]
        c["total_mentions"] += ev["num_mentions"]
        c["total_articles"] += ev["num_articles"]
        c["tone_sum"] += ev["avg_tone"]
        c["goldstein_sum"] += ev["goldstein"]
        for cat in ev["categories"]:
            c["categories"][cat] += 1
        if ev["source_url"]:
            c["source_urls"].append(ev["source_url"])
        c["cities"][ev["city"]] += 1
        c["countries"][ev["country"]] += 1

    hotspots = []
    for key, c in clusters.items():
        n = len(c["events"])
        avg_lat = c["lat_sum"] / n
        avg_lng = c["lng_sum"] / n
        avg_tone = c["tone_sum"] / n
        avg_goldstein = c["goldstein_sum"] / n

        # Primary city: most frequent
        city = max(c["cities"], key=c["cities"].get)
        country = max(c["countries"], key=c["countries"].get)

        # Sorted categories by frequency
        sorted_cats = sorted(c["categories"].items(), key=lambda x: -x[1])
        categories = [cat for cat, _ in sorted_cats]

        # Composite intensity: blend sources, goldstein impact, tone intensity
        # NumSources is base; |GoldsteinScale| boosts impact; |AvgTone| boosts emotional charge
        raw_intensity = (
            c["total_sources"]
            * (1 + abs(avg_goldstein) / 10)
            * (1 + abs(avg_tone) / 10)
        )

        hotspots.append({
            "lat": round(avg_lat, 4),
            "lng": round(avg_lng, 4),
            "city": city,
            "country": country,
            "categories": categories,
            "intensity_raw": raw_intensity,
            "numSources": c["total_sources"],
            "numMentions": c["total_mentions"],
            "numArticles": c["total_articles"],
            "avgTone": round(avg_tone, 2),
            "avgGoldstein": round(avg_goldstein, 2),
            "eventCount": n,
            "sourceUrls": c["source_urls"][:10],  # Keep top 10
        })

    # Normalize intensity to 0-100
    if hotspots:
        max_raw = max(h["intensity_raw"] for h in hotspots)
        for h in hotspots:
            h["intensity"] = round((h["intensity_raw"] / max_raw) * 100) if max_raw > 0 else 0
            del h["intensity_raw"]

    # Sort by intensity descending
    hotspots.sort(key=lambda h: -h["intensity"])

    print(f"  ✓ Clustered into {len(hotspots)} hotspots")
    return hotspots


def collect(min_sources=5):
    """Main collection pipeline. Returns list of hotspot dicts."""
    print("[GDELT Collector]")
    print("  Fetching latest GDELT update...")

    rows, url = fetch_gdelt_csv()
    if not rows:
        return []

    events = parse_events(rows, min_sources=min_sources)
    if not events:
        return []

    hotspots = cluster_events(events)
    return hotspots


if __name__ == "__main__":
    hotspots = collect(min_sources=5)
    print(f"\nTop 10 hotspots:")
    for h in hotspots[:10]:
        print(f"  {h['city']}, {h['country']} — intensity: {h['intensity']}, "
              f"sources: {h['numSources']}, tone: {h['avgTone']}, "
              f"cats: {h['categories']}")
