"""
Article Enricher — Phase 3 (GKG-based)
Replaces the slow DOC 2.0 API approach with two fast bulk data sources:

1. GKG GeoJSON API — one HTTP call returns up to 45,000 geolocated articles
   from the last 24 hours with titles, URLs, domains, tone, themes, and names.
   We build a coordinate-keyed spatial index for fast proximity lookups.

2. GKG Counts File — daily CSV with structured counts (KILL, PROTEST, ARREST,
   WOUND, AFFECT, KIDNAP, SEIZE) tied to coordinates. Enriches summaries with
   concrete numbers ("47 killed", "200 protesters").

Total enrichment time: ~15-30 seconds instead of 20+ minutes.
"""

import csv
import io
import json
import math
import re
import zipfile
import urllib.request
import urllib.parse
from collections import defaultdict
from datetime import datetime, timedelta, timezone


# ── GKG GeoJSON API ─────────────────────────────────────────────

GKG_GEOJSON_URL = (
    "http://api.gdeltproject.org/api/v1/gkg_geojson"
    "?QUERY="
    "&OUTPUTFIELDS=name,url,domain,tone,themes,names"
    "&MAXROWS=45000"
    "&TIMESPAN=1440"
)


def fetch_gkg_geojson():
    """
    Fetch the GKG GeoJSON feed — all geolocated articles from the last 24h.
    Returns a list of dicts: {lat, lng, name, url, domain, tone, themes, names}.
    One HTTP call, typically 2-8 MB of JSON.
    """
    print("  Fetching GKG GeoJSON API (last 24h, up to 45k articles)...")
    try:
        req = urllib.request.Request(
            GKG_GEOJSON_URL,
            headers={"User-Agent": "WorldNewsMap/2.0"}
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()

        data = json.loads(raw.decode("utf-8", errors="replace"))
        features = data.get("features", [])
        print(f"    ✓ Received {len(features):,} geolocated articles")

        articles = []
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry", {})
            coords = geom.get("coordinates", [])
            if len(coords) < 2:
                continue

            lng, lat = coords[0], coords[1]
            if lat == 0.0 and lng == 0.0:
                continue

            name = props.get("name", "").strip()
            url = props.get("url", "").strip()
            domain = props.get("domain", "").strip()
            tone = _safe_float(props.get("tone", "0"))
            themes = props.get("themes", "")
            names = props.get("names", "")

            if not name or len(name) < 10:
                continue
            if not url:
                continue

            articles.append({
                "lat": lat,
                "lng": lng,
                "name": name,
                "url": url,
                "domain": domain,
                "tone": tone,
                "themes": themes if isinstance(themes, str) else "",
                "names": names if isinstance(names, str) else "",
            })

        print(f"    ✓ Parsed {len(articles):,} valid articles with coordinates")
        return articles

    except Exception as e:
        print(f"    ✗ GKG GeoJSON fetch failed: {e}")
        return []


# ── GKG Counts File ──────────────────────────────────────────────

def get_gkg_counts_urls(num_days=3):
    """Build URLs for GKG daily counts files (posted next morning ~6AM EST)."""
    now = datetime.now(timezone.utc)
    urls = []
    for offset in range(1, num_days + 1):
        day = now - timedelta(days=offset)
        stamp = day.strftime("%Y%m%d")
        urls.append(f"http://data.gdeltproject.org/gkg/{stamp}.gkgcounts.csv.zip")
    return urls


def fetch_gkg_counts(num_days=2):
    """
    Download GKG Counts files. Each is ~2-3 MB zipped.
    Returns list of count dicts with coordinates and structured data.

    Counts file columns (tab-delimited):
      0: DATE, 1: NUMARTS, 2: COUNTTYPE, 3: NUMBER, 4: OBJECTTYPE,
      5: GEO_TYPE, 6: GEO_FULLNAME, 7: GEO_COUNTRYCODE, 8: GEO_ADM1CODE,
      9: GEO_LAT, 10: GEO_LONG, 11: GEO_FEATUREID,
      12: CAMEOEVENTIDS, 13: SOURCES, 14: SOURCEURLS
    """
    urls = get_gkg_counts_urls(num_days)
    all_counts = []

    for url in urls:
        filename = url.split("/")[-1]
        print(f"  Fetching {filename}...")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "WorldNewsMap/2.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = resp.read()

            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as f:
                    text = f.read().decode("utf-8", errors="replace")

            rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
            file_counts = 0

            for row in rows:
                if len(row) < 11:
                    continue
                lat = _safe_float(row[9])
                lng = _safe_float(row[10])
                if lat == 0.0 and lng == 0.0:
                    continue

                count_type = row[2].strip().upper()
                number = _safe_int(row[3])
                if number <= 0:
                    continue

                object_type = row[4].strip() if len(row) > 4 else ""
                geo_name = row[6].strip() if len(row) > 6 else ""
                num_arts = _safe_int(row[1])

                # Extract first source URL if available
                source_urls_raw = row[14] if len(row) > 14 else ""
                first_url = ""
                if source_urls_raw:
                    parts = source_urls_raw.split("<UDIV>")
                    for p in parts:
                        p = p.strip()
                        if p.startswith("http"):
                            first_url = p
                            break

                all_counts.append({
                    "lat": lat,
                    "lng": lng,
                    "count_type": count_type,
                    "number": number,
                    "object_type": object_type,
                    "geo_name": geo_name,
                    "num_arts": num_arts,
                    "source_url": first_url,
                })
                file_counts += 1

            print(f"    ✓ {file_counts:,} counts from {len(rows):,} rows")

        except Exception as e:
            print(f"    ✗ Failed: {e}")

    print(f"  Total: {len(all_counts):,} counts from {len(urls)} files")
    return all_counts


# ── Spatial Index ────────────────────────────────────────────────

def _grid_key(lat, lng, resolution=0.5):
    """
    Quantize coordinates to a grid cell for fast spatial lookups.
    resolution=0.5 means ~55km grid cells — good for matching events
    to nearby articles even when coordinates differ slightly.
    """
    return (round(lat / resolution) * resolution, round(lng / resolution) * resolution)


def build_article_index(articles):
    """Build a grid-based spatial index from GKG articles."""
    index = defaultdict(list)
    for a in articles:
        key = _grid_key(a["lat"], a["lng"])
        index[key].append(a)
    return index


def build_counts_index(counts):
    """Build a grid-based spatial index from GKG counts."""
    index = defaultdict(list)
    for c in counts:
        key = _grid_key(c["lat"], c["lng"])
        index[key].append(c)
    return index


def find_nearby(index, lat, lng, radius_cells=1):
    """
    Find all items in the spatial index within radius_cells grid cells
    of the given coordinates. With resolution=0.5 and radius=1, this
    searches a ~165km radius — plenty for matching events to articles.
    """
    resolution = 0.5
    center_key = _grid_key(lat, lng, resolution)
    results = []
    for dx in range(-radius_cells, radius_cells + 1):
        for dy in range(-radius_cells, radius_cells + 1):
            key = (center_key[0] + dx * resolution, center_key[1] + dy * resolution)
            results.extend(index.get(key, []))
    return results


# ── Article Scoring and Selection ────────────────────────────────

# Reputable sources get priority
REPUTABLE_DOMAINS = {
    "reuters.com", "bbc.co.uk", "bbc.com", "aljazeera.com", "apnews.com",
    "theguardian.com", "nytimes.com", "washingtonpost.com", "france24.com",
    "dw.com", "nhk.or.jp", "thehindu.com", "scmp.com", "cnn.com",
    "bloomberg.com", "ft.com", "economist.com", "abc.net.au",
    "afp.com", "elpais.com", "lemonde.fr",
    "spiegel.de", "corriere.it", "asahi.com", "timesofindia.com",
}

# Clickbait / low-quality signals
CLICKBAIT_PATTERNS = [
    "you won't believe", "shocking", "click here", "subscribe",
    "watch:", "live:", "update:", "breaking:", "just in:",
    "sponsored", "promoted", "advertisement",
]


def score_article(article, event_categories=None):
    """
    Score an article for quality and relevance.
    Higher = better headline for display.
    """
    name = article["name"]
    domain = article.get("domain", "").lower()
    score = 0.0

    # Length: prefer informative headlines (40-120 chars)
    length = len(name)
    if 40 <= length <= 120:
        score += 20
    elif 20 <= length <= 150:
        score += 10
    elif length > 150:
        score -= 10

    # Reputable source bonus
    for rep in REPUTABLE_DOMAINS:
        if rep in domain:
            score += 30
            break

    # Penalize clickbait
    lower = name.lower()
    for bait in CLICKBAIT_PATTERNS:
        if bait in lower:
            score -= 40

    # Penalize if title == domain (not a real headline)
    if name.lower().strip() == domain.lower().strip():
        score -= 100

    # Tone intensity bonus: high-emotion articles are often more newsworthy
    tone = abs(article.get("tone", 0))
    score += min(tone * 2, 15)

    # Theme relevance bonus if we know the event's categories
    if event_categories and article.get("themes"):
        themes_lower = article["themes"].lower()
        for cat in event_categories:
            cat_themes = CATEGORY_THEME_MAP.get(cat, [])
            for t in cat_themes:
                if t in themes_lower:
                    score += 15
                    break

    return score


# Map our categories to GKG theme keywords for relevance matching
CATEGORY_THEME_MAP = {
    "conflict": ["conflict", "military", "war", "terror", "armed", "attack", "violence"],
    "politics": ["politic", "govern", "elect", "legislat", "parliament", "diplomat"],
    "economy": ["econ", "trade", "market", "financ", "bank", "gdp", "inflat"],
    "environment": ["environ", "climate", "disaster", "flood", "drought", "earthquake"],
    "humanitarian": ["humanitarian", "refugee", "displace", "famine", "aid", "crisis"],
    "health": ["health", "medical", "disease", "pandemic", "vaccine", "hospital"],
    "positive": ["peace", "agree", "cooperat", "progress", "achiev", "breakthrough"],
}


def pick_best_articles(nearby_articles, event_categories=None, max_articles=5):
    """
    From a list of nearby articles, score them and return the top ones.
    Returns (best_title, article_list).
    """
    if not nearby_articles:
        return None, []

    # Score and sort
    scored = []
    seen_titles = set()
    for a in nearby_articles:
        # Deduplicate by normalized title prefix
        title_key = a["name"].lower().strip()[:60]
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)

        s = score_article(a, event_categories)
        scored.append((s, a))

    scored.sort(key=lambda x: -x[0])

    # Build article list
    articles = []
    for _, a in scored[:max_articles]:
        articles.append({
            "title": a["name"],
            "url": a["url"],
            "source": a["domain"],
            "lang": "en",
        })

    best_title = articles[0]["title"] if articles else None
    return best_title, articles


# ── Counts-based Summary Enhancement ────────────────────────────

COUNT_TYPE_TEMPLATES = {
    "KILL": "{number} reported killed",
    "WOUND": "{number} reported wounded",
    "ARREST": "{number} reported arrested",
    "PROTEST": "{number} protesters reported",
    "KIDNAP": "{number} reported kidnapped",
    "AFFECT": "{number} reported affected",
    "SEIZE": "{number} seized",
}


def build_counts_summary(nearby_counts):
    """
    Build a human-readable snippet from nearby GKG counts.
    e.g. "47 reported killed, 200 protesters reported"
    """
    if not nearby_counts:
        return ""

    # Aggregate by count_type, keeping the largest number
    best_by_type = {}
    for c in nearby_counts:
        ct = c["count_type"]
        if ct not in best_by_type or c["number"] > best_by_type[ct]["number"]:
            best_by_type[ct] = c

    # Build snippets in priority order
    priority = ["KILL", "WOUND", "PROTEST", "ARREST", "KIDNAP", "AFFECT", "SEIZE"]
    snippets = []
    for ct in priority:
        if ct in best_by_type:
            c = best_by_type[ct]
            template = COUNT_TYPE_TEMPLATES.get(ct)
            if template:
                snippet = template.format(number=c["number"])
                if c["object_type"]:
                    snippet += f" ({c['object_type']})"
                snippets.append(snippet)
        if len(snippets) >= 2:
            break

    return "; ".join(snippets)


# ── Names Extraction ─────────────────────────────────────────────

def extract_notable_names(nearby_articles, max_names=3):
    """
    Extract the most frequently mentioned person/org names from
    nearby GKG articles. Returns a list of cleaned name strings.
    """
    name_counts = defaultdict(int)
    for a in nearby_articles:
        names_str = a.get("names", "")
        if not names_str:
            continue
        # GKG names field is semicolon-delimited
        for name in names_str.split(";"):
            name = name.strip()
            if not name or len(name) < 3 or len(name) > 50:
                continue
            # Skip generic / noisy entries
            if name.upper() in _SKIP_NAMES:
                continue
            name_counts[name] += 1

    if not name_counts:
        return []

    # Sort by frequency, return top names
    sorted_names = sorted(name_counts.items(), key=lambda x: -x[1])
    return [n for n, _ in sorted_names[:max_names]]


_SKIP_NAMES = {
    "UNITED STATES", "UNITED KINGDOM", "EUROPEAN UNION", "UNITED NATIONS",
    "THE ASSOCIATED PRESS", "REUTERS", "AFP", "BBC", "CNN",
    "THE GUARDIAN", "THE NEW YORK TIMES", "WASHINGTON POST",
    "WHITE HOUSE", "PENTAGON", "KREMLIN", "GOVERNMENT",
    "PRESIDENT", "PRIME MINISTER", "POLICE", "MILITARY",
}


# ── Main Enrichment Pipeline ────────────────────────────────────

def enrich(hotspots, delay=0.5, max_minutes=5):
    """
    Enrich hotspots with real headlines and structured counts from GKG.

    Strategy:
    1. One GKG GeoJSON API call → 45k articles with coordinates
    2. One-two GKG Counts file downloads → structured counts with coordinates
    3. Build spatial indexes, match to hotspots by proximity
    4. Replace template summaries with real headlines + counts data

    Total time: ~15-30 seconds for all hotspots.

    Note: delay and max_minutes params kept for API compatibility with
    orchestrator.py but are not used in the bulk approach.
    """
    import time as _time
    start = _time.time()
    print("[Article Enricher — GKG Bulk Mode]")
    print(f"  {len(hotspots):,} hotspots to enrich")
    print()

    # Step 1: Fetch GKG GeoJSON (articles with coordinates)
    gkg_articles = fetch_gkg_geojson()
    article_index = build_article_index(gkg_articles)
    print(f"  Article index: {len(article_index):,} grid cells")
    print()

    # Step 2: Fetch GKG Counts (structured data)
    gkg_counts = fetch_gkg_counts(num_days=2)
    counts_index = build_counts_index(gkg_counts)
    print(f"  Counts index: {len(counts_index):,} grid cells")
    print()

    # Step 3: Match hotspots to nearby articles and counts
    enriched_articles = 0
    enriched_counts = 0
    enriched_names = 0

    for h in hotspots:
        lat, lng = h["lat"], h["lng"]
        categories = h.get("categories", [])

        # Find nearby GKG articles
        nearby_arts = find_nearby(article_index, lat, lng, radius_cells=1)
        best_title, article_list = pick_best_articles(nearby_arts, categories)

        if article_list:
            h["articles"] = article_list
            if best_title:
                h["summary"] = best_title
            enriched_articles += 1

        # Find nearby GKG counts
        nearby_cnts = find_nearby(counts_index, lat, lng, radius_cells=1)
        counts_snippet = build_counts_summary(nearby_cnts)
        if counts_snippet:
            # Append counts data to summary
            if h.get("summary") and not h["summary"].startswith("template"):
                h["summary"] = f"{h['summary']} — {counts_snippet}"
            else:
                h["summary"] = counts_snippet
            enriched_counts += 1

        # Extract notable names from nearby articles
        names = extract_notable_names(nearby_arts)
        if names:
            h["notable_names"] = names
            enriched_names += 1

    # Step 4: Fallback for unenriched events — keep CAMEO template summary,
    # link to GDELT source URL if available
    for h in hotspots:
        if "articles" not in h or not h["articles"]:
            articles = []
            for url in h.get("sourceUrls", [])[:1]:
                try:
                    domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                except Exception:
                    domain = "Source"
                articles.append({
                    "title": h.get("summary", "Read full report"),
                    "url": url,
                    "source": domain,
                    "lang": "en",
                })
            h["articles"] = articles

    elapsed = _time.time() - start
    print(f"  ✓ Enrichment complete in {elapsed:.1f}s")
    print(f"    {enriched_articles:,} hotspots matched to GKG articles")
    print(f"    {enriched_counts:,} hotspots matched to GKG counts")
    print(f"    {enriched_names:,} hotspots with notable names")
    unenriched = sum(1 for h in hotspots if not h.get("articles"))
    if unenriched:
        print(f"    {unenriched:,} hotspots kept template summaries (no nearby GKG data)")

    return hotspots


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


# ── CLI Test ─────────────────────────────────────────────────────

if __name__ == "__main__":
    test = [
        {
            "city": "Kyiv", "country": "Ukraine", "categories": ["conflict"],
            "intensity": 95, "lat": 50.45, "lng": 30.52,
            "summary": "template", "sourceUrls": [],
        },
        {
            "city": "Washington", "country": "United States", "categories": ["politics"],
            "intensity": 80, "lat": 38.9, "lng": -77.04,
            "summary": "template", "sourceUrls": [],
        },
        {
            "city": "Gaza", "country": "Palestine", "categories": ["conflict"],
            "intensity": 90, "lat": 31.5, "lng": 34.47,
            "summary": "template", "sourceUrls": [],
        },
    ]
    result = enrich(test, delay=0)
    for h in result:
        print(f"\n{h['city']}, {h['country']}:")
        print(f"  Summary: {h['summary']}")
        if h.get("notable_names"):
            print(f"  Names: {', '.join(h['notable_names'])}")
        for a in h.get("articles", []):
            print(f"  {a['source']}: {a['title']}")
