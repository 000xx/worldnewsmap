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
    "06": "politics",       # Engage in material cooperation
    "07": "economy",        # Provide aid
    "08": "politics",       # Yield / Concede
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

    # POSITIVE: Only if the actual article tone is genuinely positive.
    # AvgTone > 5 is truly upbeat coverage (not just "not negative").
    # Also require goldstein > 0 to avoid positive-toned articles about bad events.
    if avg_tone > 5.0 and goldstein > 0:
        # Replace primary category with positive if tone is very high
        categories = ["positive"] + [c for c in categories if c != "positive"]
    elif avg_tone > 3.0 and goldstein > 3.0:
        # Add as secondary if moderately positive
        if "positive" not in categories:
            categories.append("positive")

    # CONFLICT: escalate if goldstein is very negative
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


# ── Location Correction ─────────────────────────────────────────
# GDELT often geocodes to country centroids when it can't find a city.
# These coordinates land in the middle of nowhere (center of Russia,
# center of China, etc). We detect and flag these.

# Known country centroid coordinates (lat, lng) rounded to 1 decimal.
# If an event's coordinates match one of these, it was geocoded to the
# country level, not a specific city.
COUNTRY_CENTROIDS = {
    (60.0, 100.0): "Russia",
    (35.0, 105.0): "China",
    (20.0, 77.0): "India",
    (64.0, 26.0): "Finland",
    (-25.0, 135.0): "Australia",
    (56.0, -106.0): "Canada",
    (39.0, -98.0): "United States",
    (-10.0, -55.0): "Brazil",
    (-35.0, -65.0): "Argentina",
    (47.0, 2.0): "France",
    (51.0, 9.0): "Germany",
    (46.0, 25.0): "Romania",
    (52.0, 20.0): "Poland",
    (49.0, 32.0): "Ukraine",
    (23.0, 45.0): "Saudi Arabia",
    (32.0, 54.0): "Iran",
    (34.0, 44.0): "Iraq",
    (15.0, 30.0): "Sudan",
    (10.0, 8.0): "Nigeria",
    (1.0, 38.0): "Kenya",
    (-2.0, 30.0): "DR Congo",
    (15.0, 102.0): "Thailand",
    (-5.0, 120.0): "Indonesia",
    (13.0, 122.0): "Philippines",
    (36.0, 128.0): "South Korea",
    (36.0, 138.0): "Japan",
    (48.0, 68.0): "Kazakhstan",
    (41.0, 64.0): "Uzbekistan",
    (29.0, 84.0): "Nepal",
    (7.0, 81.0): "Sri Lanka",
    (22.0, 98.0): "Myanmar",
    (24.0, 90.0): "Bangladesh",
    (28.0, 3.0): "Algeria",
    (34.0, 9.0): "Tunisia",
    (32.0, 17.0): "Libya",
    (27.0, 30.0): "Egypt",
    (9.0, 42.0): "Ethiopia",
    (-6.0, 35.0): "Tanzania",
    (-13.0, 34.0): "Malawi",
    (-22.0, 24.0): "Botswana",
    (-29.0, 24.0): "South Africa",
    (42.0, 44.0): "Georgia",
    (40.0, 50.0): "Azerbaijan",
    (40.0, 45.0): "Armenia",
    (12.0, 15.0): "Chad",
    (17.0, -4.0): "Mali",
    (14.0, -14.0): "Senegal",
    (8.0, -2.0): "Ghana",
    (6.0, -5.0): "Ivory Coast",
}


def is_country_centroid(lat, lng):
    """Check if coordinates match a known country centroid (imprecise geocoding)."""
    rounded = (round(lat), round(lng))
    return rounded in COUNTRY_CENTROIDS


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

            # Skip country centroids (imprecise geocoding to middle of nowhere)
            if is_country_centroid(lat, lng):
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


# ── Summary Generation ──────────────────────────────────────────

# More specific CAMEO descriptions at the base code level (2-3 digits)
CAMEO_DETAIL = {
    # Verbal Cooperation
    "010": "made a public statement",
    "011": "declined to comment",
    "012": "made a pessimistic comment",
    "013": "made an optimistic comment",
    "014": "considered a policy option",
    "015": "acknowledged responsibility",
    "016": "denied responsibility",
    "017": "engaged in diplomatic talks",
    "018": "made a call or visit",
    "019": "expressed confidence",
    "020": "made an appeal",
    "021": "appealed for material cooperation",
    "023": "appealed for aid",
    "024": "appealed for political reform",
    "025": "appealed for policy change",
    "026": "appealed to others to meet or negotiate",
    "027": "appealed to others to settle a dispute",
    "028": "appealed for de-escalation",
    "030": "expressed intent to cooperate",
    "031": "expressed intent to engage diplomatically",
    "033": "expressed intent to provide aid",
    "034": "expressed intent to institute political reform",
    "035": "expressed intent to yield territory or authority",
    "036": "expressed intent to meet or negotiate",
    "040": "held a consultation",
    "041": "discussed governance issues",
    "042": "made a diplomatic visit",
    "043": "hosted a diplomatic meeting",
    "044": "mediated in a dispute",
    "045": "provided diplomatic recognition",
    "046": "held bilateral or multilateral talks",
    "050": "engaged in diplomatic cooperation",
    "051": "praised or endorsed",
    "052": "defended policies or actions",
    "053": "rallied political support",
    "054": "signed a formal agreement",
    "055": "provided diplomatic support",
    "056": "formed an alliance or partnership",
    "057": "established diplomatic relations",
    "060": "engaged in material cooperation",
    "061": "provided economic cooperation or support",
    "062": "provided military cooperation or support",
    "063": "provided humanitarian cooperation or support",
    "064": "provided judicial cooperation",
    "070": "provided aid",
    "071": "provided economic aid",
    "072": "provided military aid",
    "073": "provided humanitarian aid",
    "074": "provided development aid",
    "075": "granted asylum or refugee status",
    "080": "made a concession or yielded",
    "081": "eased political restrictions",
    "082": "eased economic or military restrictions",
    "083": "allowed international involvement",
    "084": "eased sanctions or embargoes",
    "086": "returned territory or persons",
    "087": "yielded in a negotiation",
    # Verbal Conflict
    "090": "launched an investigation",
    "091": "investigated alleged crimes or corruption",
    "092": "investigated human rights violations",
    "093": "investigated military actions",
    "094": "investigated war crimes",
    "100": "made demands",
    "101": "demanded political reform",
    "102": "demanded policy change",
    "103": "demanded rights or territory",
    "104": "demanded economic reform",
    "105": "demanded that sanctions be lifted",
    "106": "demanded a meeting or negotiation",
    "107": "demanded the release of persons or property",
    "108": "demanded disarmament",
    "110": "expressed disapproval",
    "111": "criticized publicly",
    "112": "accused of wrongdoing",
    "113": "filed a formal complaint",
    "114": "issued a warning",
    "115": "brought a lawsuit",
    "116": "found guilty in a legal ruling",
    "120": "rejected or refused",
    "121": "rejected a proposal or plan",
    "122": "refused to cooperate",
    "123": "rejected a request for aid",
    "124": "refused to allow access",
    "125": "rejected a peace plan",
    "126": "defied international norms",
    "127": "vetoed a resolution",
    "128": "rejected ceasefire terms",
    "129": "walked out of talks",
    "130": "issued a threat",
    "131": "threatened with non-force actions",
    "132": "threatened with economic sanctions",
    "133": "threatened to cut off relations",
    "134": "threatened to boycott",
    "135": "threatened with military action",
    "136": "threatened to use weapons of mass destruction",
    "137": "threatened to attack",
    "138": "threatened to use nuclear weapons",
    "139": "issued an ultimatum",
    "140": "held a protest or demonstration",
    "141": "demonstrated or rallied",
    "142": "conducted a hunger strike",
    "143": "conducted a strike or boycott",
    "144": "obstructed passage or blocked movement",
    "145": "staged a political protest",
    # Material Conflict
    "150": "demonstrated military strength",
    "151": "increased military alert level",
    "152": "mobilized armed forces",
    "153": "placed troops on alert",
    "154": "conducted a military exercise",
    "155": "mobilized police forces",
    "160": "reduced or severed relations",
    "161": "reduced diplomatic relations",
    "162": "reduced or suspended aid",
    "163": "expelled or withdrew diplomatic personnel",
    "164": "cut off diplomatic communications",
    "165": "severed diplomatic relations",
    "166": "expelled from an international organization",
    "170": "engaged in coercion",
    "171": "seized or detained persons",
    "172": "imposed administrative sanctions",
    "173": "arrested or detained suspects",
    "174": "expelled or deported individuals",
    "175": "imposed economic sanctions",
    "176": "imposed a blockade or embargo",
    "180": "carried out an assault",
    "181": "used chemical, biological, or radiological weapons",
    "182": "carried out a suicide bombing",
    "183": "used improvised explosive device",
    "184": "engaged in a violent clash",
    "185": "attempted assassination",
    "186": "committed an assassination",
    "190": "used conventional military force",
    "191": "imposed a no-fly zone",
    "192": "imposed a blockade",
    "193": "conducted an air or missile strike",
    "194": "seized or occupied territory",
    "195": "used unconventional violence",
    "196": "carried out a bombing",
    "200": "used unconventional mass violence",
    "201": "engaged in mass expulsion",
    "202": "engaged in ethnic cleansing",
    "203": "engaged in mass killings",
}

# Fallback root-level descriptions
CAMEO_ROOT = {
    "01": "made a public statement",
    "02": "issued an appeal",
    "03": "expressed intent to cooperate",
    "04": "held consultations",
    "05": "engaged in diplomatic cooperation",
    "06": "provided material cooperation",
    "07": "provided aid",
    "08": "made concessions",
    "09": "launched an investigation",
    "10": "made demands",
    "11": "expressed disapproval",
    "12": "rejected proposals",
    "13": "issued threats",
    "14": "held protests",
    "15": "demonstrated military strength",
    "16": "reduced relations",
    "17": "engaged in coercion",
    "18": "carried out an assault",
    "19": "used military force",
    "20": "engaged in mass violence",
}

CATEGORY_LABELS = {
    "conflict": "Conflict",
    "politics": "Politics",
    "economy": "Economy",
    "environment": "Environment",
    "humanitarian": "Humanitarian",
    "health": "Health",
    "positive": "Positive",
}


def clean_actor_name(name):
    """Clean up GDELT actor names for display."""
    if not name:
        return ""
    # Title case, but preserve known acronyms
    name = name.strip()
    # Common patterns
    if name.isupper() and len(name) <= 5:
        return name  # Likely an acronym like NATO, EU, IMF
    return name.title()


def generate_event_summary(ev):
    """
    Generate a natural human-readable summary for a single event.
    Reads like a brief news headline or subheading.
    """
    # Get the most specific action description available
    event_code = ev["event_code"].strip()
    action = None

    # Try 3-digit base code first, then 2-digit root
    if len(event_code) >= 3:
        action = CAMEO_DETAIL.get(event_code[:3])
    if not action and len(event_code) >= 2:
        action = CAMEO_ROOT.get(event_code[:2])
    if not action:
        action = "was involved in a notable event"

    # Build actor string
    a1 = clean_actor_name(ev["actor1_name"])
    a2 = clean_actor_name(ev["actor2_name"])

    # Construct the summary as a readable sentence
    if a1 and a2:
        summary = f"{a1} {action} regarding {a2}"
    elif a1:
        summary = f"{a1} {action}"
    elif a2:
        summary = f"Actions directed at {a2}: {action}"
    else:
        # No actors — use location as subject
        summary = f"{ev['city']}: {action}"

    # Add source count for credibility context
    sources = ev["num_sources"]
    if sources >= 50:
        summary += f" — major global coverage ({sources} sources)"
    elif sources >= 20:
        summary += f" — widespread coverage ({sources} sources)"
    elif sources >= 10:
        summary += f" — significant coverage ({sources} sources)"
    else:
        summary += f" — {sources} sources"

    # Add tone indicator
    tone = ev["avg_tone"]
    if tone < -7:
        summary += ", extremely negative tone"
    elif tone < -4:
        summary += ", strongly negative tone"
    elif tone > 5:
        summary += ", strongly positive tone"
    elif tone > 2:
        summary += ", positive tone"

    return summary


# ── Search Query Builder (for DOC API enrichment) ───────────────

# CAMEO code to search keyword mapping
CAMEO_SEARCH_KEYWORDS = {
    "01": "", "02": "appeal", "03": "cooperation", "04": "talks",
    "05": "diplomacy", "06": "cooperation", "07": "aid",
    "08": "agreement", "09": "investigation", "10": "demand",
    "11": "criticism", "12": "rejection", "13": "threat",
    "14": "protest", "15": "military", "16": "sanctions",
    "17": "sanctions", "18": "attack", "19": "military strike",
    "20": "violence",
}

# Skip these generic actor names in search queries
GENERIC_ACTORS = {
    "", "UNITED STATES", "CHINA", "RUSSIA", "GOVERNMENT", "PRESIDENT",
    "POLICE", "MILITARY", "CITIZEN", "MEDIA", "AUTHORITIES",
}


def build_search_query(ev):
    """Build a DOC API search query from an event's metadata."""
    parts = []

    # Always include city
    if ev["city"] and len(ev["city"]) > 2:
        parts.append(ev["city"])

    # Add meaningful actors
    for name in [ev["actor1_name"], ev["actor2_name"]]:
        cleaned = name.strip().upper()
        if cleaned and cleaned not in GENERIC_ACTORS and len(cleaned) > 2:
            parts.append(name.strip().title())
            if len(parts) >= 3:
                break

    # Add event-type keyword
    root = ev["event_code"][:2]
    kw = CAMEO_SEARCH_KEYWORDS.get(root, "")
    if kw and len(parts) < 4:
        parts.append(kw)

    # Cap at 5 words, join
    query = " ".join(parts[:5])
    return query if len(query) > 3 else f"{ev['city']} {ev['country']} news"


# ── Country to Continent Mapping ────────────────────────────────

COUNTRY_TO_CONTINENT = {
    # Africa
    "Algeria": "Africa", "Angola": "Africa", "Benin": "Africa", "Botswana": "Africa",
    "Burkina Faso": "Africa", "Burundi": "Africa", "Cameroon": "Africa", "Cape Verde": "Africa",
    "Central African Republic": "Africa", "Chad": "Africa", "Comoros": "Africa",
    "Democratic Republic of the Congo": "Africa", "DR Congo": "Africa", "Congo": "Africa",
    "Djibouti": "Africa", "Egypt": "Africa", "Equatorial Guinea": "Africa", "Eritrea": "Africa",
    "Eswatini": "Africa", "Ethiopia": "Africa", "Gabon": "Africa", "Gambia": "Africa",
    "Ghana": "Africa", "Guinea": "Africa", "Guinea-Bissau": "Africa", "Ivory Coast": "Africa",
    "Kenya": "Africa", "Lesotho": "Africa", "Liberia": "Africa", "Libya": "Africa",
    "Madagascar": "Africa", "Malawi": "Africa", "Mali": "Africa", "Mauritania": "Africa",
    "Mauritius": "Africa", "Morocco": "Africa", "Mozambique": "Africa", "Namibia": "Africa",
    "Niger": "Africa", "Nigeria": "Africa", "Rwanda": "Africa", "Senegal": "Africa",
    "Sierra Leone": "Africa", "Somalia": "Africa", "South Africa": "Africa",
    "South Sudan": "Africa", "Sudan": "Africa", "Tanzania": "Africa", "Togo": "Africa",
    "Tunisia": "Africa", "Uganda": "Africa", "Zambia": "Africa", "Zimbabwe": "Africa",
    # Asia
    "Afghanistan": "Asia", "Armenia": "Asia", "Azerbaijan": "Asia", "Bahrain": "Asia",
    "Bangladesh": "Asia", "Bhutan": "Asia", "Brunei": "Asia", "Cambodia": "Asia",
    "China": "Asia", "Cyprus": "Asia", "Georgia": "Asia", "India": "Asia",
    "Indonesia": "Asia", "Iran": "Asia", "Iraq": "Asia", "Israel": "Asia",
    "Japan": "Asia", "Jordan": "Asia", "Kazakhstan": "Asia", "Kuwait": "Asia",
    "Kyrgyzstan": "Asia", "Laos": "Asia", "Lebanon": "Asia", "Malaysia": "Asia",
    "Maldives": "Asia", "Mongolia": "Asia", "Myanmar": "Asia", "Nepal": "Asia",
    "North Korea": "Asia", "Oman": "Asia", "Pakistan": "Asia", "Palestine": "Asia",
    "Philippines": "Asia", "Qatar": "Asia", "Saudi Arabia": "Asia", "Singapore": "Asia",
    "South Korea": "Asia", "Sri Lanka": "Asia", "Syria": "Asia", "Taiwan": "Asia",
    "Tajikistan": "Asia", "Thailand": "Asia", "Timor-Leste": "Asia", "Turkey": "Asia",
    "Turkmenistan": "Asia", "United Arab Emirates": "Asia", "Uzbekistan": "Asia",
    "Vietnam": "Asia", "Yemen": "Asia",
    # Europe
    "Albania": "Europe", "Andorra": "Europe", "Austria": "Europe", "Belarus": "Europe",
    "Belgium": "Europe", "Bosnia and Herzegovina": "Europe", "Bulgaria": "Europe",
    "Croatia": "Europe", "Czech Republic": "Europe", "Denmark": "Europe", "Estonia": "Europe",
    "Finland": "Europe", "France": "Europe", "Germany": "Europe", "Greece": "Europe",
    "Hungary": "Europe", "Iceland": "Europe", "Ireland": "Europe", "Italy": "Europe",
    "Kosovo": "Europe", "Latvia": "Europe", "Lithuania": "Europe", "Luxembourg": "Europe",
    "Malta": "Europe", "Moldova": "Europe", "Monaco": "Europe", "Montenegro": "Europe",
    "Netherlands": "Europe", "North Macedonia": "Europe", "Norway": "Europe", "Poland": "Europe",
    "Portugal": "Europe", "Romania": "Europe", "Russia": "Europe", "Serbia": "Europe",
    "Slovakia": "Europe", "Slovenia": "Europe", "Spain": "Europe", "Sweden": "Europe",
    "Switzerland": "Europe", "Ukraine": "Europe", "United Kingdom": "Europe",
    # North America
    "Canada": "North America", "Costa Rica": "North America", "Cuba": "North America",
    "Dominican Republic": "North America", "El Salvador": "North America",
    "Guatemala": "North America", "Haiti": "North America", "Honduras": "North America",
    "Jamaica": "North America", "Mexico": "North America", "Nicaragua": "North America",
    "Panama": "North America", "Trinidad and Tobago": "North America",
    "United States": "North America",
    # South America
    "Argentina": "South America", "Bolivia": "South America", "Brazil": "South America",
    "Chile": "South America", "Colombia": "South America", "Ecuador": "South America",
    "Guyana": "South America", "Paraguay": "South America", "Peru": "South America",
    "Suriname": "South America", "Uruguay": "South America", "Venezuela": "South America",
    # Oceania
    "Australia": "Oceania", "Fiji": "Oceania", "New Zealand": "Oceania",
    "Papua New Guinea": "Oceania",
}


def get_continent(country):
    """Look up continent for a country name. Returns 'Other' if unknown."""
    return COUNTRY_TO_CONTINENT.get(country, "Other")


# ── Format Events as Hotspots ───────────────────────────────────

def events_to_hotspots(events, max_hotspots=None):
    """
    Convert individual events directly into hotspot dicts for the frontend.
    No clustering — each event is its own dot on the map.
    Sorted by recency-weighted intensity.
    """
    hotspots = []

    for ev in events:
        # Composite score for sorting: sources * impact * tone * recency
        raw_intensity = (
            ev["num_sources"]
            * (1 + abs(ev["goldstein"]) / 10)
            * (1 + abs(ev["avg_tone"]) / 10)
            * ev["recency_weight"]
        )

        summary = generate_event_summary(ev)
        search_query = build_search_query(ev)
        continent = get_continent(ev["country"])

        hotspots.append({
            "lat": ev["lat"],
            "lng": ev["lng"],
            "city": ev["city"],
            "country": ev["country"],
            "continent": continent,
            "categories": ev["categories"],
            "intensity_raw": raw_intensity,
            "numSources": ev["num_sources"],
            "numMentions": ev["num_mentions"],
            "numArticles": ev["num_articles"],
            "avgTone": round(ev["avg_tone"], 2),
            "avgGoldstein": round(ev["goldstein"], 2),
            "eventCount": 1,
            "hoursAgo": None,
            "summary": summary,
            "searchQuery": search_query,
            "sourceUrls": [ev["source_url"]] if ev["source_url"] else [],
        })

    # Normalize intensity to 0-100
    if hotspots:
        max_raw = max(h["intensity_raw"] for h in hotspots)
        for h in hotspots:
            h["intensity"] = round((h["intensity_raw"] / max_raw) * 100) if max_raw > 0 else 0
            del h["intensity_raw"]

    # Sort by intensity descending
    hotspots.sort(key=lambda h: -h["intensity"])

    if max_hotspots and len(hotspots) > max_hotspots:
        hotspots = hotspots[:max_hotspots]

    print(f"  ✓ Formatted {len(hotspots):,} individual events as hotspots")
    return hotspots


# ── Main Entry Point ────────────────────────────────────────────

def collect(min_sources=3, max_age_hours=36, num_days=3):
    """
    Main collection pipeline. Returns list of hotspot dicts.
    Each GDELT event with 3+ sources becomes its own hotspot.
    """
    print("[GDELT Collector]")
    print(f"  Strategy: every event with {min_sources}+ sources from last {num_days} available daily files")
    print()

    file_data = fetch_gdelt_daily(num_days=5, target_files=num_days)
    if not file_data:
        return []

    print()
    events = parse_events(file_data, min_sources=min_sources)
    if not events:
        print(f"\n  No events at {min_sources}+ sources. Retrying with min_sources=1...")
        events = parse_events(file_data, min_sources=1)
        if not events:
            return []

    print()
    hotspots = events_to_hotspots(events)
    return hotspots


if __name__ == "__main__":
    hotspots = collect(min_sources=3, max_age_hours=36)
    print(f"\nTotal: {len(hotspots):,} events")
    print(f"\nTop 15:")
    for h in hotspots[:15]:
        print(f"  {h['city']}, {h['country']} ({h['continent']}) — intensity: {h['intensity']}, "
              f"sources: {h['numSources']}, cats: {h['categories']}")
        print(f"    {h['summary']}")
        print(f"    Query: {h['searchQuery']}")
