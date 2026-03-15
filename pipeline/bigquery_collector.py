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
    Build SQL that does ALL heavy lifting inside BigQuery:
    1. Extracts first city-level location from V2Locations
    2. Extracts PAGE_TITLE from Extras
    3. Groups by 0.5° grid cell
    4. Aggregates article count, headlines, persons, themes, counts, tone

    Returns ~2,000-5,000 rows (grid cells) instead of 400,000+ raw rows.
    This cuts the pipeline from 2+ minutes to ~15 seconds.

    V2EnhancedLocations format:
      TYPE#FULLNAME#CC#ADM1#ADM2#LAT#LONG#FEATUREID#OFFSET
      [0]   [1]     [2] [3] [4] [5] [6]   [7]       [8]
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
    WITH parsed AS (
      SELECT
        -- Extract first location entry from V2Locations
        SPLIT(SPLIT(V2Locations, ';')[OFFSET(0)], '#') AS loc_parts,
        DocumentIdentifier,
        SourceCommonName,
        V2Counts,
        V2Themes,
        V2Persons,
        V2Tone,
        REGEXP_EXTRACT(Extras, r'<PAGE_TITLE>(.*?)</PAGE_TITLE>') AS PageTitle
      FROM `{GKG_TABLE}`
      WHERE DATE >= {since_int}
        {until_clause}
        AND DATE(_PARTITIONTIME) >= "{partition_since}"
        {partition_until}
        AND V2Locations IS NOT NULL
        AND LENGTH(V2Locations) > 5
    ),
    located AS (
      SELECT
        *,
        SAFE_CAST(loc_parts[SAFE_OFFSET(0)] AS INT64) AS geo_type,
        loc_parts[SAFE_OFFSET(1)] AS fullname,
        loc_parts[SAFE_OFFSET(2)] AS country_code,
        -- V2Enhanced has ADM2 at index 4, so lat=5, lng=6
        SAFE_CAST(loc_parts[SAFE_OFFSET(5)] AS FLOAT64) AS lat,
        SAFE_CAST(loc_parts[SAFE_OFFSET(6)] AS FLOAT64) AS lng
      FROM parsed
      WHERE ARRAY_LENGTH(loc_parts) >= 8
    ),
    filtered AS (
      SELECT *
      FROM located
      WHERE geo_type IN (3, 4)
        AND lat IS NOT NULL AND lng IS NOT NULL
        AND lat BETWEEN -90 AND 90
        AND lng BETWEEN -180 AND 180
        AND NOT (lat = 0 AND lng = 0)
    ),
    gridded AS (
      SELECT
        *,
        -- 0.5 degree grid cell (~55km)
        ROUND(lat * 2) / 2 AS grid_lat,
        ROUND(lng * 2) / 2 AS grid_lng
      FROM filtered
    )
    SELECT
      grid_lat,
      grid_lng,
      -- Representative location: most common fullname in cell
      APPROX_TOP_COUNT(fullname, 1)[OFFSET(0)].value AS top_fullname,
      APPROX_TOP_COUNT(country_code, 1)[OFFSET(0)].value AS top_country_code,
      COUNT(*) AS num_articles,
      AVG(SAFE_CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(0)] AS FLOAT64)) AS avg_tone,
      -- Collect up to 10 real article headlines
      ARRAY_AGG(STRUCT(
        PageTitle AS title,
        DocumentIdentifier AS url,
        SourceCommonName AS source
      ) ORDER BY
        IF(PageTitle IS NOT NULL AND LENGTH(PageTitle) > 10, 1, 0) DESC,
        IF(SourceCommonName IN ('Reuters', 'BBC News', 'Associated Press', 'Al Jazeera',
          'The Guardian', 'CNN', 'Bloomberg', 'France 24', 'DW'), 1, 0) DESC
      LIMIT 10) AS articles,
      -- Top persons
      APPROX_TOP_COUNT(
        SPLIT(REGEXP_REPLACE(V2Persons, r',\\d+', ''), ';')[SAFE_OFFSET(0)], 5
      ) AS top_persons,
      -- Top themes
      APPROX_TOP_COUNT(
        SPLIT(REGEXP_REPLACE(V2Themes, r',\\d+', ''), ';')[SAFE_OFFSET(0)], 10
      ) AS top_themes,
      -- Aggregate counts (just pass the raw strings, parse in Python)
      ARRAY_AGG(V2Counts IGNORE NULLS LIMIT 5) AS count_samples
    FROM gridded
    GROUP BY grid_lat, grid_lng
    HAVING num_articles >= 2
    ORDER BY num_articles DESC
    LIMIT 5000
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


# ── Event Clustering ─────────────────────────────────────────────

# Words to ignore when comparing headlines for clustering
STOP_WORDS = {
    "the", "a", "an", "in", "on", "at", "to", "for", "of", "and", "or", "is",
    "was", "are", "were", "be", "been", "has", "have", "had", "with", "by",
    "from", "as", "that", "this", "it", "its", "but", "not", "no", "after",
    "over", "into", "about", "up", "out", "new", "says", "said", "will",
    "could", "would", "may", "more", "than", "also", "how", "what", "when",
    "who", "why", "all", "been", "being", "do", "does", "did", "just", "get",
    "got", "can", "one", "two", "three", "first", "last", "most", "some",
    "other", "each", "every", "both", "few", "many", "much", "own", "same",
    "so", "if", "then", "here", "there", "where", "which", "while",
}


def headline_words(title):
    """Extract significant lowercase words from a headline."""
    if not title:
        return set()
    words = re.findall(r'[a-zA-Z]{3,}', title.lower())
    return {w for w in words if w not in STOP_WORDS}


def cluster_articles_by_event(articles, headlines):
    """
    Group articles within a grid cell into event clusters.
    Articles sharing 3+ significant headline words are the same event.
    Returns list of clusters, each a dict with articles list and headlines list.
    """
    if not headlines:
        # No headlines to cluster on — return as single event
        return [{"articles": articles, "headlines": []}]

    # Build headline → word set mapping
    hl_words = [(h, headline_words(h)) for h in headlines]

    # Greedy clustering: for each headline, find or create a cluster
    clusters = []  # list of {"word_set": set, "headline_indices": [int], ...}

    for i, (hl, words) in enumerate(hl_words):
        if not words:
            continue
        best_cluster = None
        best_overlap = 0
        for cl in clusters:
            overlap = len(words & cl["word_set"])
            if overlap >= 3 and overlap > best_overlap:
                best_cluster = cl
                best_overlap = overlap
        if best_cluster:
            best_cluster["headline_indices"].append(i)
            best_cluster["word_set"] |= words
        else:
            clusters.append({
                "word_set": set(words),
                "headline_indices": [i],
            })

    if not clusters:
        return [{"articles": articles, "headlines": headlines[:5]}]

    # Map articles to clusters by matching their title to cluster headlines
    result = []
    used_articles = set()

    for cl in clusters:
        cl_headlines = [headlines[i] for i in cl["headline_indices"]]
        cl_articles = []
        for j, art in enumerate(articles):
            if j in used_articles:
                continue
            art_title = art.get("title", "")
            if art_title:
                art_words = headline_words(art_title)
                if len(art_words & cl["word_set"]) >= 2:
                    cl_articles.append(art)
                    used_articles.add(j)
            elif not used_articles:
                # No title — assign to first cluster
                cl_articles.append(art)
                used_articles.add(j)

        if cl_headlines:  # Only create event if it has headlines
            result.append({
                "articles": cl_articles if cl_articles else articles[:2],
                "headlines": cl_headlines[:5],
            })

    # Any leftover articles without a cluster
    leftover = [art for j, art in enumerate(articles) if j not in used_articles]
    if leftover and not result:
        result.append({"articles": leftover, "headlines": headlines[:3]})
    elif leftover and result:
        # Add leftovers to the largest cluster
        result[0]["articles"].extend(leftover)

    return result if result else [{"articles": articles, "headlines": headlines[:5]}]


# ── Main Collection Pipeline ─────────────────────────────────────

def collect_from_bigquery(since_hours=36, until_timestamp=None):
    """
    Pull pre-aggregated GKG hotspots from BigQuery, then cluster by event.

    Each grid cell's articles get sub-clustered by headline similarity.
    Each cluster becomes its own hotspot (event) on the map.
    """
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=since_hours)

    print(f"[BigQuery GKG Collector]")
    print(f"  Window: {since.strftime('%Y-%m-%d %H:%M')} → {now.strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"  ({since_hours} hours)")

    client = get_bq_client()
    query = build_gkg_query(since, until_timestamp)

    print(f"  Running BigQuery query (aggregated)...")
    query_job = client.query(query)
    rows = list(query_job.result())
    print(f"  ✓ Received {len(rows):,} grid cells (pre-aggregated)")

    if not rows:
        return []

    hotspots = []
    total_events = 0

    for row in rows:
        lat = row.grid_lat
        lng = row.grid_lng
        num_articles = row.num_articles
        avg_tone = row.avg_tone or 0.0

        if lat is None or lng is None:
            continue
        if is_country_centroid(lat, lng):
            continue

        # Parse fullname → city, country
        fullname = row.top_fullname or ""
        name_parts = [p.strip() for p in fullname.split(",")]
        city = name_parts[0] if name_parts else "Unknown"
        country = name_parts[-1] if len(name_parts) > 1 else (row.top_country_code or "")
        continent = COUNTRY_TO_CONTINENT.get(country, "Other")

        # Process articles from SQL
        all_articles = []
        all_headlines = []
        seen_sources = set()
        for art in (row.articles or []):
            title = ""
            if art.get("title"):
                try:
                    title = html.unescape(art["title"]).strip()
                    if len(title) < 10:
                        title = ""
                except Exception:
                    title = ""
            source = art.get("source", "") or ""
            url = art.get("url", "") or ""

            if title and title not in all_headlines:
                all_headlines.append(title)

            all_articles.append({
                "url": url,
                "source": source,
                "title": title,
            })

        # Process persons
        persons = []
        for p in (row.top_persons or []):
            name = p.get("value", "") or ""
            if name and len(name) > 2 and name not in persons:
                persons.append(name)

        # Process themes
        themes_raw = []
        for t in (row.top_themes or []):
            theme = t.get("value", "") or ""
            if theme and theme not in themes_raw:
                themes_raw.append(theme)

        # Classify categories
        categories = classify_themes(themes_raw[:50], tone=avg_tone)

        # Parse counts
        count_agg = {}
        for count_str in (row.count_samples or []):
            if not count_str:
                continue
            for parsed_count in parse_counts(count_str):
                ct = parsed_count["count_type"]
                if ct not in count_agg or parsed_count["number"] > count_agg[ct]["number"]:
                    count_agg[ct] = {
                        "number": parsed_count["number"],
                        "object_type": parsed_count["object_type"],
                    }

        # ── Event clustering ──
        event_clusters = cluster_articles_by_event(all_articles, all_headlines)

        for cluster in event_clusters:
            cl_articles = cluster["articles"]
            cl_headlines = cluster["headlines"]

            # Dedupe articles by source for display
            display_articles = []
            seen = set()
            for a in cl_articles:
                if a["source"] and a["source"] not in seen:
                    seen.add(a["source"])
                    display_articles.append({
                        "url": a["url"],
                        "source": a["source"],
                    })
                if len(display_articles) >= 5:
                    break

            if not display_articles and all_articles:
                display_articles = [{"url": all_articles[0]["url"],
                                     "source": all_articles[0]["source"]}]

            n_sources = len(display_articles)
            intensity_raw = n_sources * (1 + abs(avg_tone) / 10)

            hotspots.append({
                "lat": lat,
                "lng": lng,
                "city": city,
                "country": country,
                "continent": continent,
                "categories": categories,
                "intensity_raw": intensity_raw,
                "numSources": n_sources,
                "articles": display_articles,
                "headlines": cl_headlines[:5],
                "metadata": {
                    "persons": persons[:5],
                    "themes": themes_raw[:8],
                    "counts": count_agg,
                    "avgTone": round(avg_tone, 2),
                },
            })
            total_events += 1

    # Normalize intensity
    if hotspots:
        max_raw = max(h["intensity_raw"] for h in hotspots)
        for h in hotspots:
            h["intensity"] = round((h["intensity_raw"] / max_raw) * 100) if max_raw > 0 else 0
            del h["intensity_raw"]

    hotspots.sort(key=lambda h: -h["intensity"])
    print(f"  ✓ {len(rows):,} grid cells → {total_events:,} events")
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
    Merge new event hotspots into existing data.
    Uses lat+lng+first_headline as key to identify same events.
    """
    # Index existing by a compound key
    existing_map = {}
    for h in existing:
        hl = (h.get("headlines") or [""])[0][:40]
        key = f"{h['lat']},{h['lng']},{hl}"
        existing_map[key] = h

    # Merge in new data
    for h in new_data:
        hl = (h.get("headlines") or [""])[0][:40]
        key = f"{h['lat']},{h['lng']},{hl}"
        if key in existing_map:
            old = existing_map[key]
            # Merge articles (dedupe by source)
            old_sources = {a.get("source") for a in old.get("articles", [])}
            for a in h.get("articles", []):
                if a.get("source") not in old_sources:
                    old["articles"].append(a)
            old["numSources"] = len(old.get("articles", []))
        else:
            existing_map[key] = h

    merged = list(existing_map.values())
    if merged:
        max_src = max(h.get("numSources", 1) for h in merged)
        for h in merged:
            h["intensity"] = round((h.get("numSources", 1) / max_src) * 100) if max_src > 0 else 0

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
