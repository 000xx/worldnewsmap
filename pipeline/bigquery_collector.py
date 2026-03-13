"""
BigQuery GKG Collector — Phase 4
Replaces both gdelt_collector.py and article_enricher.py.

Pulls GKG records from GDELT's public BigQuery tables, extracting:
- Geolocated articles with real headlines and URLs
- Themes, persons, organizations
- Counts (killed, wounded, arrested, protesters, etc.)
- Tone scores

Two modes:
  FULL  — last 36 hours (first run or recovery)
  DELTA — last 30 minutes (every subsequent run)

The orchestrator decides which mode by checking for an existing state file.
Output is a single JSON with all location-grouped data, ready for the frontend.
Groq generates summaries on-click in the browser — we just ship raw data.

Requires: google-cloud-bigquery
Auth: GOOGLE_APPLICATION_CREDENTIALS env var pointing to a service account JSON,
      or GCP_SA_KEY env var containing the JSON string (for GitHub Actions).
"""

import json
import os
import re
import html
import math
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from google.cloud import bigquery


# ── BigQuery Config ──────────────────────────────────────────────

# The query runs under YOUR project (for billing), but reads from gdelt-bq's public tables.
# Set NEWSMAP_GCP_PROJECT to your GCP project ID, or it will use the default from credentials.
GKG_TABLE = "gdelt-bq.gdeltv2.gkg_partitioned"


def get_bq_client():
    """
    Create a BigQuery client. Handles two auth patterns:
    1. GCP_SA_KEY env var (JSON string) — used in GitHub Actions
    2. GOOGLE_APPLICATION_CREDENTIALS file path — local dev

    The client project (for billing) comes from the service account's project
    or from NEWSMAP_GCP_PROJECT env var.
    """
    sa_key = os.environ.get("GCP_SA_KEY", "")
    if sa_key:
        # Write temp credentials file from secret
        cred_path = "/tmp/gcp_sa_key.json"
        with open(cred_path, "w") as f:
            f.write(sa_key)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

    # Use explicit project if set, otherwise let credentials determine it
    project = os.environ.get("NEWSMAP_GCP_PROJECT", None)
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client()


# ── SQL Queries ──────────────────────────────────────────────────

def build_gkg_query(since_timestamp, until_timestamp=None):
    """
    Build SQL to fetch GKG records with locations from the partitioned table.

    Key fields:
    - Extras: contains <PAGE_TITLE>headline</PAGE_TITLE> — the actual article headline
    - V2Locations: semicolon-delimited geolocated mentions
    - V2Counts: structured counts (killed, wounded, etc.)
    - V2Themes, V2Persons: thematic and entity context
    - V2Tone: sentiment scores

    Uses DATE(_PARTITIONTIME) for partition pruning (Standard SQL).
    DATE field is YYYYMMDDHHMMSS as integer.
    """
    since_int = int(since_timestamp.strftime("%Y%m%d%H%M%S"))
    partition_since = since_timestamp.strftime("%Y-%m-%d")

    until_clause = ""
    partition_until = ""
    if until_timestamp:
        until_int = int(until_timestamp.strftime("%Y%m%d%H%M%S"))
        until_clause = f"AND DATE <= {until_int}"
        partition_until = f'AND DATE(_PARTITIONTIME) <= "{until_timestamp.strftime("%Y-%m-%d")}"'

    query = f"""
    SELECT
        DATE,
        DocumentIdentifier,
        SourceCommonName,
        V2Locations,
        V2Counts,
        V2Themes,
        V2Persons,
        V2Organizations,
        V2Tone,
        REGEXP_EXTRACT(Extras, r'<PAGE_TITLE>(.*?)</PAGE_TITLE>') AS PageTitle
    FROM `{GKG_TABLE}`
    WHERE DATE >= {since_int}
        {until_clause}
        AND DATE(_PARTITIONTIME) >= "{partition_since}"
        {partition_until}
        AND V2Locations IS NOT NULL
        AND LENGTH(V2Locations) > 5
    """
    return query


# ── GKG Field Parsers ────────────────────────────────────────────

def parse_locations(v2locations):
    """
    Parse V2Locations (V2EnhancedLocations) field.

    V2Enhanced format (GKG 2.1+):
      TYPE#FULLNAME#COUNTRYCODE#ADM1CODE#ADM2CODE#LAT#LONG#FEATUREID#CHAROFFSET
      [0]   [1]       [2]        [3]      [4]    [5] [6]    [7]        [8]

    Note: V2 added ADM2CODE at index 4, shifting LAT to 5 and LONG to 6.
    Each location is semicolon-delimited, fields are #-delimited.
    We only want city-level or better (type 3, 4).
    """
    if not v2locations:
        return []

    locations = []
    for loc_str in v2locations.split(";"):
        loc_str = loc_str.strip()
        if not loc_str:
            continue
        parts = loc_str.split("#")
        if len(parts) < 7:
            continue

        try:
            geo_type = int(parts[0]) if parts[0] else 0
        except ValueError:
            geo_type = 0

        # Skip country-level matches (type 1) — too imprecise
        if geo_type < 2:
            continue

        fullname = parts[1].strip()
        country_code = parts[2].strip()

        # Detect V2Enhanced (has ADM2CODE) vs V1 format
        # V2Enhanced has 9 fields, V1 has 7
        # Safe detection: if there are 8+ parts, it's V2Enhanced
        if len(parts) >= 9:
            # V2Enhanced: TYPE#FULLNAME#CC#ADM1#ADM2#LAT#LONG#FEATUREID#OFFSET
            lat = _safe_float(parts[5])
            lng = _safe_float(parts[6])
        elif len(parts) >= 7:
            # V1: TYPE#FULLNAME#CC#ADM1#LAT#LONG#FEATUREID
            lat = _safe_float(parts[4])
            lng = _safe_float(parts[5])
        else:
            continue

        if lat == 0.0 and lng == 0.0:
            continue
        # Sanity check lat/lng ranges
        if lat < -90 or lat > 90 or lng < -180 or lng > 180:
            continue

        # Parse city and country from fullname
        name_parts = [p.strip() for p in fullname.split(",")]
        city = name_parts[0] if name_parts else fullname
        country = name_parts[-1] if len(name_parts) > 1 else country_code

        locations.append({
            "geo_type": geo_type,
            "fullname": fullname,
            "city": city,
            "country": country,
            "country_code": country_code,
            "lat": lat,
            "lng": lng,
        })

    return locations


def parse_counts(v2counts):
    """
    Parse V2Counts field.
    Format: COUNTTYPE#NUMBER#OBJECTTYPE#GEO_TYPE#GEO_FULLNAME#GEO_COUNTRYCODE#
            GEO_ADM1CODE#GEO_LAT#GEO_LONG#GEO_FEATUREID#OFFSET;...

    Returns list of {count_type, number, object_type, lat, lng}.
    """
    if not v2counts:
        return []

    counts = []
    for count_str in v2counts.split(";"):
        parts = count_str.split("#")
        if len(parts) < 9:
            continue

        count_type = parts[0].strip().upper()
        number = _safe_int(parts[1])
        if number <= 0:
            continue

        object_type = parts[2].strip() if len(parts) > 2 else ""
        lat = _safe_float(parts[7]) if len(parts) > 7 else 0.0
        lng = _safe_float(parts[8]) if len(parts) > 8 else 0.0

        counts.append({
            "count_type": count_type,
            "number": number,
            "object_type": object_type,
            "lat": lat,
            "lng": lng,
        })

    return counts


def parse_themes(v2themes):
    """
    Parse V2Themes. Format: THEME,OFFSET;THEME,OFFSET;...
    Returns list of theme strings (without offsets).
    """
    if not v2themes:
        return []
    themes = []
    for t in v2themes.split(";"):
        parts = t.split(",")
        if parts and parts[0].strip():
            themes.append(parts[0].strip())
    return themes


def parse_persons(v2persons):
    """
    Parse V2Persons. Format: NAME,OFFSET;NAME,OFFSET;...
    Returns deduplicated list of person names.
    """
    if not v2persons:
        return []
    seen = set()
    persons = []
    for p in v2persons.split(";"):
        parts = p.split(",")
        name = parts[0].strip() if parts else ""
        if name and len(name) > 2 and name not in seen:
            seen.add(name)
            persons.append(name)
    return persons


def parse_tone(v2tone):
    """
    Parse V2Tone. Format: TONE,POS_SCORE,NEG_SCORE,POLARITY,ACTIVITY_REF_DENSITY,
                          SELF_GROUP_REF_DENSITY,WORD_COUNT
    Returns tone (float) and word_count (int).
    """
    if not v2tone:
        return 0.0, 0
    parts = v2tone.split(",")
    tone = _safe_float(parts[0]) if parts else 0.0
    word_count = _safe_int(parts[6]) if len(parts) > 6 else 0
    return tone, word_count


# ── Category Classification ──────────────────────────────────────

THEME_TO_CATEGORY = {
    # Conflict
    "KILL": "conflict", "WOUND": "conflict", "TERROR": "conflict",
    "MILITARY": "conflict", "ARMED_CONFLICT": "conflict", "WAR": "conflict",
    "ARMEDCONFLICT": "conflict", "ARREST": "conflict",
    "PROTEST": "conflict", "REBELLION": "conflict", "COUP": "conflict",
    # Politics
    "ELECTION": "politics", "LEGISLATION": "politics", "GOVERN": "politics",
    "ECON_TAXATION": "politics", "DIPLOMACY": "politics", "PARLIAMENT": "politics",
    "LEADER": "politics", "DEMOCRACY": "politics",
    "GENERAL_GOVERNMENT": "politics", "POLITICAL_TURMOIL": "politics",
    # Economy
    "ECON_": "economy", "TRADE": "economy", "MARKET": "economy",
    "INFLATION": "economy", "UNEMPLOYMENT": "economy", "BANKRUPTCY": "economy",
    "FINANCE": "economy", "SANCTIONS": "economy",
    # Environment
    "ENV_": "environment", "CLIMATE": "environment", "FLOOD": "environment",
    "DROUGHT": "environment", "EARTHQUAKE": "environment", "HURRICANE": "environment",
    "WILDFIRE": "environment", "TSUNAMI": "environment",
    "NATURAL_DISASTER": "environment",
    # Humanitarian
    "REFUGEE": "humanitarian", "FAMINE": "humanitarian", "DISPLACED": "humanitarian",
    "HUMANITARIAN": "humanitarian", "FOOD_SECURITY": "humanitarian",
    "POVERTY": "humanitarian",
    # Health
    "HEALTH_": "health", "PANDEMIC": "health", "DISEASE": "health",
    "MEDICAL": "health", "VACCINE": "health", "EPIDEMIC": "health",
    # Positive
    "PEACE": "positive", "CEASEFIRE": "positive", "COOPERATION": "positive",
    "ACHIEVEMENT": "positive", "BREAKTHROUGH": "positive",
}


def classify_themes(themes, tone=0.0):
    """Classify a list of GKG themes into our category system."""
    category_scores = defaultdict(int)

    for theme in themes:
        theme_upper = theme.upper()
        for pattern, cat in THEME_TO_CATEGORY.items():
            if pattern in theme_upper:
                category_scores[cat] += 1
                break

    # Positive tone override
    if tone > 5.0:
        category_scores["positive"] = max(category_scores.get("positive", 0), 3)

    if not category_scores:
        category_scores["politics"] = 1

    # Sort by score, return as list
    sorted_cats = sorted(category_scores.items(), key=lambda x: -x[1])
    return [cat for cat, _ in sorted_cats]


# ── Spatial Grouping ─────────────────────────────────────────────

def grid_key(lat, lng, resolution=0.5):
    """Quantize to ~55km grid cells."""
    return (round(lat / resolution) * resolution, round(lng / resolution) * resolution)


# ── Country Centroid Detection ───────────────────────────────────

COUNTRY_CENTROIDS = {
    (60.0, 100.0), (35.0, 105.0), (20.0, 77.0), (64.0, 26.0),
    (-25.0, 135.0), (56.0, -106.0), (39.0, -98.0), (-10.0, -55.0),
    (-35.0, -65.0), (47.0, 2.0), (51.0, 9.0), (46.0, 25.0),
    (52.0, 20.0), (49.0, 32.0), (23.0, 45.0), (32.0, 54.0),
    (34.0, 44.0), (15.0, 30.0), (10.0, 8.0), (1.0, 38.0),
    (-2.0, 30.0), (15.0, 102.0), (-5.0, 120.0), (13.0, 122.0),
    (36.0, 128.0), (36.0, 138.0), (48.0, 68.0), (41.0, 64.0),
    (29.0, 84.0), (7.0, 81.0), (22.0, 98.0), (24.0, 90.0),
    (28.0, 3.0), (34.0, 9.0), (32.0, 17.0), (27.0, 30.0),
    (9.0, 42.0), (-6.0, 35.0), (-13.0, 34.0), (-22.0, 24.0),
    (-29.0, 24.0), (42.0, 44.0), (40.0, 50.0), (40.0, 45.0),
}


def is_country_centroid(lat, lng):
    return (round(lat), round(lng)) in COUNTRY_CENTROIDS


# ── Country to Continent ─────────────────────────────────────────

COUNTRY_TO_CONTINENT = {
    "Algeria": "Africa", "Angola": "Africa", "Benin": "Africa", "Botswana": "Africa",
    "Burkina Faso": "Africa", "Burundi": "Africa", "Cameroon": "Africa",
    "Central African Republic": "Africa", "Chad": "Africa",
    "Democratic Republic of the Congo": "Africa", "DR Congo": "Africa", "Congo": "Africa",
    "Djibouti": "Africa", "Egypt": "Africa", "Eritrea": "Africa",
    "Ethiopia": "Africa", "Gabon": "Africa", "Gambia": "Africa",
    "Ghana": "Africa", "Guinea": "Africa", "Ivory Coast": "Africa",
    "Kenya": "Africa", "Liberia": "Africa", "Libya": "Africa",
    "Madagascar": "Africa", "Mali": "Africa", "Mauritania": "Africa",
    "Morocco": "Africa", "Mozambique": "Africa", "Namibia": "Africa",
    "Niger": "Africa", "Nigeria": "Africa", "Rwanda": "Africa", "Senegal": "Africa",
    "Sierra Leone": "Africa", "Somalia": "Africa", "South Africa": "Africa",
    "South Sudan": "Africa", "Sudan": "Africa", "Tanzania": "Africa",
    "Tunisia": "Africa", "Uganda": "Africa", "Zambia": "Africa", "Zimbabwe": "Africa",
    "Afghanistan": "Asia", "Armenia": "Asia", "Azerbaijan": "Asia", "Bahrain": "Asia",
    "Bangladesh": "Asia", "Cambodia": "Asia", "China": "Asia", "Georgia": "Asia",
    "India": "Asia", "Indonesia": "Asia", "Iran": "Asia", "Iraq": "Asia",
    "Israel": "Asia", "Japan": "Asia", "Jordan": "Asia", "Kazakhstan": "Asia",
    "Kuwait": "Asia", "Kyrgyzstan": "Asia", "Laos": "Asia", "Lebanon": "Asia",
    "Malaysia": "Asia", "Mongolia": "Asia", "Myanmar": "Asia", "Nepal": "Asia",
    "North Korea": "Asia", "Oman": "Asia", "Pakistan": "Asia", "Palestine": "Asia",
    "Philippines": "Asia", "Qatar": "Asia", "Saudi Arabia": "Asia", "Singapore": "Asia",
    "South Korea": "Asia", "Sri Lanka": "Asia", "Syria": "Asia", "Taiwan": "Asia",
    "Thailand": "Asia", "Turkey": "Asia", "United Arab Emirates": "Asia",
    "Uzbekistan": "Asia", "Vietnam": "Asia", "Yemen": "Asia",
    "Albania": "Europe", "Austria": "Europe", "Belarus": "Europe",
    "Belgium": "Europe", "Bosnia and Herzegovina": "Europe", "Bulgaria": "Europe",
    "Croatia": "Europe", "Czech Republic": "Europe", "Denmark": "Europe",
    "Estonia": "Europe", "Finland": "Europe", "France": "Europe", "Germany": "Europe",
    "Greece": "Europe", "Hungary": "Europe", "Iceland": "Europe", "Ireland": "Europe",
    "Italy": "Europe", "Kosovo": "Europe", "Latvia": "Europe", "Lithuania": "Europe",
    "Luxembourg": "Europe", "Moldova": "Europe", "Montenegro": "Europe",
    "Netherlands": "Europe", "North Macedonia": "Europe", "Norway": "Europe",
    "Poland": "Europe", "Portugal": "Europe", "Romania": "Europe", "Russia": "Europe",
    "Serbia": "Europe", "Slovakia": "Europe", "Slovenia": "Europe", "Spain": "Europe",
    "Sweden": "Europe", "Switzerland": "Europe", "Ukraine": "Europe",
    "United Kingdom": "Europe",
    "Canada": "North America", "Costa Rica": "North America", "Cuba": "North America",
    "Dominican Republic": "North America", "El Salvador": "North America",
    "Guatemala": "North America", "Haiti": "North America", "Honduras": "North America",
    "Jamaica": "North America", "Mexico": "North America", "Nicaragua": "North America",
    "Panama": "North America", "Trinidad and Tobago": "North America",
    "United States": "North America",
    "Argentina": "South America", "Bolivia": "South America", "Brazil": "South America",
    "Chile": "South America", "Colombia": "South America", "Ecuador": "South America",
    "Paraguay": "South America", "Peru": "South America",
    "Uruguay": "South America", "Venezuela": "South America",
    "Australia": "Oceania", "Fiji": "Oceania", "New Zealand": "Oceania",
    "Papua New Guinea": "Oceania",
}


# ── Reputable Sources ────────────────────────────────────────────

REPUTABLE_SOURCES = {
    "reuters", "bbc", "associated press", "ap news", "al jazeera",
    "the guardian", "new york times", "washington post", "france 24",
    "dw", "nhk", "cnn", "bloomberg", "financial times",
    "the economist", "abc news", "afp",
}


def is_reputable(source_name):
    if not source_name:
        return False
    lower = source_name.lower()
    return any(r in lower for r in REPUTABLE_SOURCES)


# ── Main Collection Pipeline ─────────────────────────────────────

def collect_from_bigquery(since_hours=36, until_timestamp=None):
    """
    Pull GKG records from BigQuery and group into location-based hotspots.

    Returns list of hotspot dicts ready for the frontend, with raw article
    data for Groq to summarize on-click.
    """
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=since_hours)

    print(f"[BigQuery GKG Collector]")
    print(f"  Window: {since.strftime('%Y-%m-%d %H:%M')} → {now.strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"  ({since_hours} hours)")

    client = get_bq_client()
    query = build_gkg_query(since, until_timestamp)

    print(f"  Running BigQuery query...")
    query_job = client.query(query)
    rows = list(query_job.result())
    print(f"  ✓ Received {len(rows):,} GKG records")

    if not rows:
        return []

    # Group articles by grid cell
    cell_data = defaultdict(lambda: {
        "articles": [],
        "counts": [],
        "themes_all": [],
        "persons_all": [],
        "locations": [],
        "tones": [],
        "word_counts": [],
    })

    parsed = 0
    skipped_geo = 0

    for row in rows:
        locations = parse_locations(row.V2Locations)
        if not locations:
            skipped_geo += 1
            continue

        counts = parse_counts(row.V2Counts)
        themes = parse_themes(row.V2Themes)
        persons = parse_persons(row.V2Persons)
        tone, word_count = parse_tone(row.V2Tone)

        url = row.DocumentIdentifier or ""
        source = row.SourceCommonName or ""
        date_int = row.DATE

        # Extract real article headline from Extras → PAGE_TITLE
        title = ""
        try:
            raw_title = row.PageTitle or ""
            if raw_title:
                # Unescape HTML entities (&#x26; → &, etc.)
                title = html.unescape(raw_title).strip()
                # Skip if title is just the domain or too short
                if len(title) < 10 or title.lower() == source.lower():
                    title = ""
        except Exception:
            title = ""

        # Use the FIRST city-level location as primary
        primary_loc = None
        for loc in locations:
            if loc["geo_type"] >= 3 and not is_country_centroid(loc["lat"], loc["lng"]):
                primary_loc = loc
                break
        if not primary_loc:
            for loc in locations:
                if not is_country_centroid(loc["lat"], loc["lng"]):
                    primary_loc = loc
                    break
        if not primary_loc:
            skipped_geo += 1
            continue

        key = grid_key(primary_loc["lat"], primary_loc["lng"])
        cell = cell_data[key]

        cell["articles"].append({
            "url": url,
            "source": source,
            "title": title,
            "tone": tone,
            "word_count": word_count,
            "date": date_int,
            "reputable": is_reputable(source),
        })
        cell["counts"].extend(counts)
        cell["themes_all"].extend(themes)
        cell["persons_all"].extend(persons)
        cell["locations"].append(primary_loc)
        cell["tones"].append(tone)
        cell["word_counts"].append(word_count)
        parsed += 1

    print(f"  ✓ Parsed {parsed:,} records into {len(cell_data):,} grid cells")
    print(f"    Skipped: {skipped_geo:,} (no valid geo)")

    # Convert grid cells to hotspots
    hotspots = []
    for key, cell in cell_data.items():
        articles = cell["articles"]
        if not articles:
            continue

        # Pick representative location (most common city in cell)
        loc_counts = defaultdict(int)
        loc_map = {}
        for loc in cell["locations"]:
            loc_key = (loc["city"], loc["country"])
            loc_counts[loc_key] += 1
            loc_map[loc_key] = loc
        best_loc_key = max(loc_counts, key=loc_counts.get)
        best_loc = loc_map[best_loc_key]

        # Classify by themes
        categories = classify_themes(
            cell["themes_all"][:100],  # Cap to avoid huge lists
            tone=sum(cell["tones"]) / len(cell["tones"]) if cell["tones"] else 0
        )

        # Aggregate counts
        count_agg = defaultdict(lambda: {"number": 0, "object_type": ""})
        for c in cell["counts"]:
            ct = c["count_type"]
            if c["number"] > count_agg[ct]["number"]:
                count_agg[ct] = {"number": c["number"], "object_type": c["object_type"]}

        # Top persons (deduplicated, by frequency)
        person_freq = defaultdict(int)
        for p in cell["persons_all"]:
            person_freq[p] += 1
        top_persons = sorted(person_freq.items(), key=lambda x: -x[1])[:5]
        top_persons = [p for p, _ in top_persons]

        # Top themes (deduplicated, by frequency)
        theme_freq = defaultdict(int)
        for t in cell["themes_all"]:
            theme_freq[t] += 1
        top_themes = sorted(theme_freq.items(), key=lambda x: -x[1])[:10]
        top_themes = [t for t, _ in top_themes]

        # Intensity score
        num_articles = len(articles)
        avg_tone = sum(cell["tones"]) / len(cell["tones"]) if cell["tones"] else 0
        reputable_count = sum(1 for a in articles if a["reputable"])
        intensity_raw = (
            num_articles
            * (1 + reputable_count)
            * (1 + abs(avg_tone) / 10)
        )

        # Select top articles — prefer those with real headlines, reputable, recent
        sorted_articles = sorted(
            articles,
            key=lambda a: (bool(a.get("title")), a["reputable"], a["date"]),
            reverse=True,
        )
        display_articles = []
        seen_sources = set()
        seen_titles = set()
        for a in sorted_articles[:20]:
            if a["source"] in seen_sources:
                continue
            title = a.get("title", "")
            # Dedupe by title prefix
            title_key = title.lower()[:50] if title else a["url"]
            if title_key in seen_titles:
                continue
            seen_sources.add(a["source"])
            seen_titles.add(title_key)
            display_articles.append({
                "url": a["url"],
                "source": a["source"],
                "title": title,
                "tone": round(a["tone"], 1),
            })
            if len(display_articles) >= 5:
                break

        # Collect all real headlines for Groq context
        all_headlines = [a.get("title", "") for a in articles if a.get("title")]
        # Deduplicate and take top 10
        unique_headlines = list(dict.fromkeys(all_headlines))[:10]

        continent = COUNTRY_TO_CONTINENT.get(best_loc["country"], "Other")

        hotspots.append({
            "lat": best_loc["lat"],
            "lng": best_loc["lng"],
            "city": best_loc["city"],
            "country": best_loc["country"],
            "continent": continent,
            "categories": categories,
            "intensity_raw": intensity_raw,
            "numArticles": num_articles,
            "avgTone": round(avg_tone, 2),
            "articles": display_articles,
            "headlines": unique_headlines,
            "counts": dict(count_agg),
            "persons": top_persons,
            "themes": top_themes,
        })

    # Normalize intensity
    if hotspots:
        max_raw = max(h["intensity_raw"] for h in hotspots)
        for h in hotspots:
            h["intensity"] = round((h["intensity_raw"] / max_raw) * 100) if max_raw > 0 else 0
            del h["intensity_raw"]

    hotspots.sort(key=lambda h: -h["intensity"])
    print(f"  ✓ {len(hotspots):,} hotspots created")
    return hotspots


# ── State Management (for incremental mode) ──────────────────────

STATE_FILE = "data/collector_state.json"


def load_state():
    """Load last collection timestamp."""
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        return datetime.fromisoformat(state["last_collected"])
    except (FileNotFoundError, KeyError, ValueError):
        return None


def save_state(timestamp):
    """Save collection timestamp."""
    os.makedirs(os.path.dirname(STATE_FILE) if os.path.dirname(STATE_FILE) else ".", exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_collected": timestamp.isoformat()}, f)


def merge_hotspots(existing, new_data, max_age_hours=36):
    """
    Merge new hotspots into existing data:
    - Add new grid cells
    - Update existing cells with fresh articles
    - Drop cells older than max_age_hours
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=max_age_hours)

    # Index existing by grid key
    existing_map = {}
    for h in existing:
        key = grid_key(h["lat"], h["lng"])
        existing_map[key] = h

    # Merge in new data
    for h in new_data:
        key = grid_key(h["lat"], h["lng"])
        if key in existing_map:
            old = existing_map[key]
            # Merge articles (dedupe by URL)
            old_urls = {a["url"] for a in old.get("articles", [])}
            for a in h.get("articles", []):
                if a["url"] not in old_urls:
                    old["articles"].append(a)
                    old_urls.add(a["url"])
            # Update counts (keep larger values)
            for ct, cv in h.get("counts", {}).items():
                if ct not in old.get("counts", {}) or cv["number"] > old["counts"].get(ct, {}).get("number", 0):
                    old.setdefault("counts", {})[ct] = cv
            # Update persons/themes
            old_persons = set(old.get("persons", []))
            for p in h.get("persons", []):
                if p not in old_persons:
                    old["persons"].append(p)
            # Update intensity
            old["numArticles"] = old.get("numArticles", 0) + h.get("numArticles", 0)
        else:
            existing_map[key] = h

    # Re-normalize intensity
    merged = list(existing_map.values())
    if merged:
        max_arts = max(h.get("numArticles", 1) for h in merged)
        for h in merged:
            h["intensity"] = round((h.get("numArticles", 1) / max_arts) * 100) if max_arts > 0 else 0

    merged.sort(key=lambda h: -h.get("intensity", 0))
    return merged


# ── Helpers ──────────────────────────────────────────────────────

def _safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ── CLI ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    hotspots = collect_from_bigquery(since_hours=36)
    print(f"\nTotal: {len(hotspots):,} hotspots")
    for h in hotspots[:10]:
        print(f"  {h['city']}, {h['country']} — {h['intensity']}% "
              f"({h['numArticles']} articles, cats: {h['categories'][:2]})")
        if h.get("persons"):
            print(f"    Persons: {', '.join(h['persons'][:3])}")
        if h.get("counts"):
            print(f"    Counts: {h['counts']}")
