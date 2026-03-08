"""
GDELT Collector — Phase 2
Fetches GDELT daily export files for the last 2-3 days, giving us a full
36-hour window of events. Events have had time to accumulate source counts,
so filtering by 5+ sources is meaningful. Intensity is weighted by recency
so breaking news gets bigger bubbles than aging stories.
"""

import csv
import io
import math
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


# ── GDELT Column Indices ────────────────────────────────────────
# V1.0 daily exports (from data.gdeltproject.org/events/) have 57-58 columns.
# V2.0 15-min exports have 61 columns (adds ADM2Code fields for each geo).
# The daily files we fetch are V1.0 format. Column order:
#
#  0  GlobalEventID
#  1  SQLDATE
#  2  MonthYear
#  3  Year
#  4  FractionDate
#  5  Actor1Code
#  6  Actor1Name
#  7  Actor1CountryCode
#  8  Actor1KnownGroupCode
#  9  Actor1EthnicCode
# 10  Actor1Religion1Code
# 11  Actor1Religion2Code
# 12  Actor1Type1Code
# 13  Actor1Type2Code
# 14  Actor1Type3Code
# 15  Actor2Code
# 16  Actor2Name
# 17  Actor2CountryCode
# 18  Actor2KnownGroupCode
# 19  Actor2EthnicCode
# 20  Actor2Religion1Code
# 21  Actor2Religion2Code
# 22  Actor2Type1Code
# 23  Actor2Type2Code
# 24  Actor2Type3Code
# 25  IsRootEvent
# 26  EventCode
# 27  EventBaseCode
# 28  EventRootCode
# 29  QuadClass
# 30  GoldsteinScale
# 31  NumMentions
# 32  NumSources
# 33  NumArticles
# 34  AvgTone
# 35  Actor1Geo_Type
# 36  Actor1Geo_FullName
# 37  Actor1Geo_CountryCode
# 38  Actor1Geo_ADM1Code
# 39  Actor1Geo_Lat
# 40  Actor1Geo_Long
# 41  Actor1Geo_FeatureID
# 42  Actor2Geo_Type
# 43  Actor2Geo_FullName
# 44  Actor2Geo_CountryCode
# 45  Actor2Geo_ADM1Code
# 46  Actor2Geo_Lat
# 47  Actor2Geo_Long
# 48  Actor2Geo_FeatureID
# 49  ActionGeo_Type
# 50  ActionGeo_FullName
# 51  ActionGeo_CountryCode
# 52  ActionGeo_ADM1Code
# 53  ActionGeo_Lat
# 54  ActionGeo_Long
# 55  ActionGeo_FeatureID
# 56  DATEADDED
# 57  SOURCEURL  (may be absent in very old files)

COL_V1 = {
    "GlobalEventID": 0,
    "Day": 1,
    "Actor1Name": 6,
    "Actor1CountryCode": 7,
    "Actor2Name": 16,
    "Actor2CountryCode": 17,
    "IsRootEvent": 25,
    "EventCode": 26,
    "EventBaseCode": 27,
    "EventRootCode": 28,
    "QuadClass": 29,
    "GoldsteinScale": 30,
    "NumMentions": 31,
    "NumSources": 32,
    "NumArticles": 33,
    "AvgTone": 34,
    "Actor1Geo_Type": 35,
    "Actor1Geo_FullName": 36,
    "Actor1Geo_CountryCode": 37,
    "Actor1Geo_Lat": 39,
    "Actor1Geo_Long": 40,
    "Actor2Geo_Type": 42,
    "Actor2Geo_FullName": 43,
    "Actor2Geo_CountryCode": 44,
    "Actor2Geo_Lat": 46,
    "Actor2Geo_Long": 47,
    "ActionGeo_Type": 49,
    "ActionGeo_FullName": 50,
    "ActionGeo_CountryCode": 51,
    "ActionGeo_ADM1Code": 52,
    "ActionGeo_Lat": 53,
    "ActionGeo_Long": 54,
    "ActionGeo_FeatureID": 55,
    "DATEADDED": 56,
    "SOURCEURL": 57,
}

# V2.0 has 3 extra ADM2Code columns (one per geo section), shifting indices
COL_V2 = {
    "GlobalEventID": 0,
    "Day": 1,
    "Actor1Name": 6,
    "Actor1CountryCode": 7,
    "Actor2Name": 16,
    "Actor2CountryCode": 17,
    "IsRootEvent": 25,
    "EventCode": 26,
    "EventBaseCode": 27,
    "EventRootCode": 28,
    "QuadClass": 29,
    "GoldsteinScale": 30,
    "NumMentions": 31,
    "NumSources": 32,
    "NumArticles": 33,
    "AvgTone": 34,
    "Actor1Geo_Type": 35,
    "Actor1Geo_FullName": 36,
    "Actor1Geo_CountryCode": 37,
    "Actor1Geo_Lat": 40,
    "Actor1Geo_Long": 41,
    "Actor2Geo_Type": 43,
    "Actor2Geo_FullName": 44,
    "Actor2Geo_CountryCode": 45,
    "Actor2Geo_Lat": 48,
    "Actor2Geo_Long": 49,
    "ActionGeo_Type": 51,
    "ActionGeo_FullName": 52,
    "ActionGeo_CountryCode": 53,
    "ActionGeo_ADM1Code": 54,
    "ActionGeo_Lat": 56,
    "ActionGeo_Long": 57,
    "ActionGeo_FeatureID": 58,
    "DATEADDED": 59,
    "SOURCEURL": 60,
}


def detect_format(row):
    """Detect whether a row is V1 (57-58 cols) or V2 (61 cols) format."""
    if len(row) >= 61:
        return COL_V2
    return COL_V1


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


# ── Fetching Daily Export Files ─────────────────────────────────

def get_daily_export_urls(num_days=5):
    """
    Build URLs for GDELT daily export files.
    Format: http://data.gdeltproject.org/events/YYYYMMDD.export.CSV.zip

    IMPORTANT: GDELT posts each day's file the NEXT morning around 6AM EST.
    So "today's" file doesn't exist yet. We start from yesterday and go back.
    We try 5 days to ensure we get at least 2 successful downloads even if
    some files 404 due to timing.
    """
    now = datetime.now(timezone.utc)
    urls = []
    for offset in range(1, num_days + 1):  # Start from 1 (yesterday), not 0 (today)
        day = now - timedelta(days=offset)
        stamp = day.strftime("%Y%m%d")
        urls.append(f"http://data.gdeltproject.org/events/{stamp}.export.CSV.zip")
    return urls


def fetch_daily_export(url):
    """Download and extract a single GDELT daily export CSV. Returns rows or None."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "WorldNewsMap/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()

        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                text = f.read().decode("utf-8", errors="replace")

        rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
        return rows
    except Exception as e:
        return None, str(e)


def fetch_gdelt_daily(num_days=5, target_files=2):
    """
    Fetch daily export files, trying up to num_days back.
    Stops after successfully downloading target_files.
    Returns list of (file_date_str, rows) tuples.
    """
    urls = get_daily_export_urls(num_days)
    all_files = []

    for url in urls:
        filename = url.split("/")[-1]
        # Extract date from filename like "20260306.export.CSV.zip"
        file_date = filename.split(".")[0]
        print(f"  Fetching {filename}...")
        result = fetch_daily_export(url)

        if isinstance(result, tuple):
            _, error = result
            print(f"    ✗ Failed: {error}")
        elif result is not None:
            all_files.append((file_date, result))
            print(f"    ✓ {len(result):,} events")
            if len(all_files) >= target_files:
                break
        else:
            print(f"    ✗ Failed (unknown error)")

    total = sum(len(rows) for _, rows in all_files)
    print(f"  Total: {total:,} events from {len(all_files)} files")
    return all_files


# ── Parsing and Filtering ───────────────────────────────────────

def parse_event_time(date_added_str):
    """
    Parse GDELT DATEADDED field (YYYYMMDDHHMMSS) into a datetime.
    Falls back to None if unparseable.
    """
    try:
        if len(date_added_str) >= 14:
            return datetime.strptime(date_added_str[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        elif len(date_added_str) >= 8:
            return datetime.strptime(date_added_str[:8], "%Y%m%d").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        pass
    return None


def recency_weight(event_time, now, half_life_hours=12):
    """
    Exponential decay weight based on how recent the event is.
    Events from right now get weight ~1.0.
    Events from 12 hours ago get weight ~0.5.
    Events from 24 hours ago get weight ~0.25.
    Events from 36 hours ago get weight ~0.125.
    """
    if event_time is None:
        return 0.3  # Default for unparseable timestamps

    age_hours = (now - event_time).total_seconds() / 3600
    if age_hours < 0:
        age_hours = 0
    return math.pow(0.5, age_hours / half_life_hours)


def parse_events(file_data, min_sources=5):
    """
    Parse raw GDELT rows into structured event dicts.
    file_data is a list of (file_date_str, rows) tuples from fetch_gdelt_daily.

    Uses the file date for recency weighting instead of DATEADDED,
    since daily export files cover one day each and DATEADDED can be unreliable.

    Filters:
      - Must have action geo coordinates (not 0,0)
      - NumSources >= min_sources
    """
    now = datetime.now(timezone.utc)
    events = []
    filtered_sources = 0
    filtered_geo = 0

    for file_date_str, rows in file_data:
        # Parse file date for recency weighting
        try:
            file_date = datetime.strptime(file_date_str, "%Y%m%d").replace(
                hour=12, tzinfo=timezone.utc  # Assume midday for the file's events
            )
        except ValueError:
            file_date = now - timedelta(days=1)

        file_weight = recency_weight(file_date, now)

        # Detect format from first row
        COL = COL_V1
        for row in rows:
            if len(row) > 10:
                COL = detect_format(row)
                break

        fmt = "V2" if COL is COL_V2 else "V1"
        print(f"  Processing {file_date_str} ({len(rows):,} rows, format {fmt}, recency weight {file_weight:.2f})")

        for row in rows:
            if len(row) < 56:
                continue

            # Check source count
            num_sources = safe_int(row[COL["NumSources"]])
            if num_sources < min_sources:
                filtered_sources += 1
                continue

            # Check geo coordinates
            lat = safe_float(row[COL["ActionGeo_Lat"]])
            lng = safe_float(row[COL["ActionGeo_Long"]])
            if lat == 0.0 and lng == 0.0:
                filtered_geo += 1
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

            source_url = ""
            if len(row) > COL["SOURCEURL"]:
                source_url = row[COL["SOURCEURL"]].strip()

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
                "source_url": source_url,
                "is_root": safe_int(row[COL["IsRootEvent"]]),
                "actor1_name": row[COL["Actor1Name"]].strip(),
                "actor2_name": row[COL["Actor2Name"]].strip(),
                "date_added": file_date_str,
                "event_time": file_date,
                "recency_weight": file_weight,
            })

    print(f"  ✓ Parsed {len(events):,} events (min {min_sources} sources)")
    print(f"    Filtered: {filtered_sources:,} too few sources, {filtered_geo:,} no geo")
    return events


# ── Clustering ──────────────────────────────────────────────────

def cluster_events(events, precision=1):
    """
    Group events by rounded coordinates to merge nearby events into hotspots.
    precision=1 means round to 1 decimal place (~11km clusters).
    Intensity is weighted by recency — newer events contribute more.
    """
    clusters = defaultdict(lambda: {
        "events": [],
        "lat_sum": 0, "lng_sum": 0,
        "total_sources": 0, "total_mentions": 0, "total_articles": 0,
        "weighted_sources": 0,  # Recency-weighted source count
        "tone_sum": 0, "goldstein_sum": 0,
        "categories": defaultdict(int),
        "source_urls": [],
        "cities": defaultdict(int),
        "countries": defaultdict(int),
        "newest_time": None,
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
        c["weighted_sources"] += ev["num_sources"] * ev["recency_weight"]
        c["tone_sum"] += ev["avg_tone"]
        c["goldstein_sum"] += ev["goldstein"]
        for cat in ev["categories"]:
            c["categories"][cat] += 1
        if ev["source_url"]:
            c["source_urls"].append(ev["source_url"])
        c["cities"][ev["city"]] += 1
        c["countries"][ev["country"]] += 1

        # Track newest event in cluster
        if ev["event_time"]:
            if c["newest_time"] is None or ev["event_time"] > c["newest_time"]:
                c["newest_time"] = ev["event_time"]

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

        # Composite intensity using RECENCY-WEIGHTED sources
        # weighted_sources is the core — recent events with many sources dominate
        # |GoldsteinScale| boosts impact; |AvgTone| boosts emotional charge
        raw_intensity = (
            c["weighted_sources"]
            * (1 + abs(avg_goldstein) / 10)
            * (1 + abs(avg_tone) / 10)
        )

        # How old is the newest event in this cluster (for display)
        hours_ago = None
        if c["newest_time"]:
            hours_ago = round((datetime.now(timezone.utc) - c["newest_time"]).total_seconds() / 3600, 1)

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
            "hoursAgo": hours_ago,
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

    print(f"  ✓ Clustered into {len(hotspots):,} hotspots")
    return hotspots


# ── Main Entry Point ────────────────────────────────────────────

def collect(min_sources=5, max_age_hours=36, num_days=3):
    """
    Main collection pipeline. Returns list of hotspot dicts.

    Args:
        min_sources: Minimum NumSources for an event (default 5).
        max_age_hours: Not used for filtering anymore (kept for config compat).
        num_days: Target number of daily files to successfully fetch (default 3).
    """
    print("[GDELT Collector]")
    print(f"  Strategy: last {num_days} available daily files, min {min_sources} sources per event")
    print()

    file_data = fetch_gdelt_daily(num_days=5, target_files=num_days)
    if not file_data:
        return []

    print()
    events = parse_events(file_data, min_sources=min_sources)
    if not events:
        # Retry with lower threshold
        print(f"\n  No events at {min_sources}+ sources. Retrying with min_sources=2...")
        events = parse_events(file_data, min_sources=2)
        if not events:
            return []

    print()
    hotspots = cluster_events(events)
    return hotspots


if __name__ == "__main__":
    hotspots = collect(min_sources=5, max_age_hours=36)
    print(f"\nTop 15 hotspots:")
    for h in hotspots[:15]:
        age = f"{h['hoursAgo']}h ago" if h['hoursAgo'] else "unknown"
        print(f"  {h['city']}, {h['country']} — intensity: {h['intensity']}, "
              f"sources: {h['numSources']}, events: {h['eventCount']}, "
              f"latest: {age}, cats: {h['categories']}")
