"""
Article Enricher — Phase 2
Uses GDELT DOC 2.0 API to fetch real article headlines for hotspots.
Deduplicates by location+category so we make ~1000-1500 API calls
instead of 60,000+, then distributes headlines to matching events.
"""

import json
import urllib.request
import urllib.parse
from time import sleep
from collections import defaultdict


DOC_API_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"


def fetch_doc_api(query, max_records=5, timespan="3d"):
    """
    Query GDELT DOC 2.0 API for articles matching a search query.
    Returns list of {title, url, source, domain, language, seendate}.
    Free, no API key needed.
    """
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": str(max_records),
        "format": "json",
        "timespan": timespan,
        "sort": "HybridRel",  # Sort by relevance
    }

    url = f"{DOC_API_BASE}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "WorldNewsMap/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        articles = []
        for item in data.get("articles", []):
            title = item.get("title", "").strip()
            # Skip empty, very short, or junk titles
            if not title or len(title) < 15:
                continue
            # Skip titles that are just the domain name
            domain = item.get("domain", "")
            if title.lower() == domain.lower():
                continue

            articles.append({
                "title": title,
                "url": item.get("url", ""),
                "source": domain,
                "lang": item.get("language", "English"),
                "seendate": item.get("seendate", ""),
            })

        return articles
    except Exception:
        return []


def build_enrichment_groups(hotspots):
    """
    Group hotspots by location+category to deduplicate API calls.
    Returns dict of (city, country, primary_category) -> list of hotspot indices.
    """
    groups = defaultdict(list)

    for i, h in enumerate(hotspots):
        city = h.get("city", "")
        country = h.get("country", "")
        cat = h["categories"][0] if h.get("categories") else "politics"
        key = (city, country, cat)
        groups[key].append(i)

    return groups


def pick_best_summary(articles):
    """
    From a list of article titles, pick the best one as the summary.
    Prefers: longer titles, from reputable sources, not clickbait.
    """
    if not articles:
        return None

    # Score each article
    scored = []
    for a in articles:
        title = a["title"]
        score = len(title)  # Longer titles tend to be more descriptive

        # Boost reputable sources
        domain = a.get("source", "").lower()
        reputable = ["reuters", "bbc", "aljazeera", "apnews", "guardian",
                     "nytimes", "washingtonpost", "france24", "dw.com",
                     "afp", "nhk", "thehindu", "scmp"]
        if any(r in domain for r in reputable):
            score += 30

        # Penalize very long titles (likely multi-headline pages)
        if len(title) > 150:
            score -= 20

        # Penalize titles with common clickbait patterns
        lower = title.lower()
        if any(p in lower for p in ["you won't believe", "shocking", "click here",
                                     "subscribe", "watch:", "live:", "update:"]):
            score -= 40

        scored.append((score, a))

    scored.sort(key=lambda x: -x[0])
    return scored[0][1]["title"] if scored else None


def enrich(hotspots, max_queries=1500, delay=0.4):
    """
    Enrich hotspots with real article headlines from GDELT DOC 2.0 API.

    Strategy:
    1. Group events by (city, country, category) to deduplicate
    2. Query DOC API once per unique group
    3. Distribute articles and best headline to all events in that group
    4. Events in groups that weren't queried keep their template summaries

    Args:
        hotspots: list of hotspot dicts from gdelt_collector
        max_queries: maximum number of API calls (default 1500)
        delay: seconds between API calls (default 0.4)
    """
    print("[Article Enricher — GDELT DOC API]")

    groups = build_enrichment_groups(hotspots)
    print(f"  {len(hotspots):,} events -> {len(groups):,} unique location+category groups")

    # Sort groups by total intensity (sum of all events in group)
    # so we prioritize the biggest stories
    group_scores = {}
    for key, indices in groups.items():
        total_intensity = sum(hotspots[i].get("intensity", 0) for i in indices)
        group_scores[key] = total_intensity

    sorted_groups = sorted(groups.items(), key=lambda x: -group_scores[x[0]])

    # Limit queries
    query_groups = sorted_groups[:max_queries]
    print(f"  Enriching top {len(query_groups):,} groups...")

    enriched = 0
    failed = 0

    for idx, (key, indices) in enumerate(query_groups):
        city, country, cat = key

        # Use the search query from the highest-intensity event in this group
        best_idx = max(indices, key=lambda i: hotspots[i].get("intensity", 0))
        query = hotspots[best_idx].get("searchQuery", f"{city} {country}")

        if not query or len(query.strip()) < 3:
            continue

        articles = fetch_doc_api(query, max_records=5, timespan="3d")

        if articles:
            best_title = pick_best_summary(articles)

            # Distribute to all events in this group
            for i in indices:
                hotspots[i]["articles"] = [
                    {
                        "title": a["title"],
                        "url": a["url"],
                        "source": a["source"],
                        "lang": a.get("lang", "en"),
                    }
                    for a in articles[:5]
                ]
                # Replace template summary with real headline if available
                if best_title:
                    hotspots[i]["summary"] = best_title

            enriched += 1
        else:
            failed += 1

        # Progress logging every 100 queries
        if (idx + 1) % 100 == 0:
            print(f"    Progress: {idx + 1}/{len(query_groups)} queries "
                  f"({enriched} enriched, {failed} no results)")

        # Rate limit
        if delay > 0:
            sleep(delay)

    # For unenriched events, keep template summary and add source URL as article
    for h in hotspots:
        if "articles" not in h or not h["articles"]:
            articles = []
            for url in h.get("sourceUrls", [])[:1]:
                try:
                    domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                except Exception:
                    domain = "Source"
                articles.append({
                    "title": "Read full report",
                    "url": url,
                    "source": domain,
                    "lang": "en",
                })
            h["articles"] = articles

    total_enriched_events = sum(
        1 for h in hotspots if h.get("articles") and h["articles"][0].get("title", "") != "Read full report"
    )

    print(f"  Enriched {enriched:,} groups ({total_enriched_events:,} events) with real headlines")
    print(f"    {failed:,} groups returned no results (kept template summaries)")

    return hotspots


if __name__ == "__main__":
    # Quick test
    test = [
        {
            "city": "Kyiv", "country": "Ukraine", "categories": ["conflict"],
            "intensity": 95, "searchQuery": "Kyiv Ukraine military strike",
            "summary": "template summary", "sourceUrls": [],
        }
    ]
    enriched = enrich(test, max_queries=1, delay=0)
    print(f"\nResult: {enriched[0]['summary']}")
    for a in enriched[0].get("articles", []):
        print(f"  {a['source']}: {a['title']}")
