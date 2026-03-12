"""
Article Enricher — Phase 2
Uses GDELT DOC 2.0 API to fetch real article headlines.

Key optimization: groups events by (COUNTRY, category) instead of (city, category).
This means "Washington DC politics" and "New York politics" share one query: "United States politics".
Reduces ~2000+ groups to ~200-400, covering ALL events in under 3 minutes.
"""

import json
import urllib.request
import urllib.parse
from time import sleep
from collections import defaultdict


DOC_API_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"


def fetch_doc_api(query, max_records=5, timespan="3d"):
    """
    Query GDELT DOC 2.0 API. Free, no key needed.
    Returns list of {title, url, source, lang, seendate}.
    """
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": str(max_records),
        "format": "json",
        "timespan": timespan,
        "sort": "HybridRel",
    }
    url = f"{DOC_API_BASE}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "WorldNewsMap/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:  # 5s timeout, not 15
            data = json.loads(resp.read().decode("utf-8"))

        articles = []
        for item in data.get("articles", []):
            title = item.get("title", "").strip()
            if not title or len(title) < 15:
                continue
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


# ── Category to search keyword ──────────────────────────────────
CAT_KEYWORDS = {
    "conflict": "conflict military attack",
    "politics": "politics government",
    "economy": "economy trade market",
    "environment": "environment climate disaster",
    "humanitarian": "humanitarian crisis aid",
    "health": "health medical disease",
    "positive": "progress achievement breakthrough",
}


def pick_best_title(articles):
    """Pick the most informative headline from a set of articles."""
    if not articles:
        return None

    scored = []
    for a in articles:
        title = a["title"]
        score = len(title)  # Longer = more descriptive

        # Boost reputable sources
        domain = a.get("source", "").lower()
        reputable = ["reuters", "bbc", "aljazeera", "apnews", "guardian",
                     "nytimes", "washingtonpost", "france24", "dw.com",
                     "afp", "nhk", "thehindu", "scmp", "cnn"]
        if any(r in domain for r in reputable):
            score += 30

        # Penalize very long or clickbait titles
        if len(title) > 150:
            score -= 20
        lower = title.lower()
        if any(p in lower for p in ["you won't believe", "shocking", "click here",
                                     "subscribe", "watch:", "live:", "update:"]):
            score -= 40

        scored.append((score, a))

    scored.sort(key=lambda x: -x[0])
    return scored[0][1]["title"] if scored else None


def enrich(hotspots, delay=0.3, max_minutes=5):
    """
    Enrich hotspots with real headlines via GDELT DOC 2.0 API.

    Groups by (country, category) — so all politics events in Kenya share
    one query. This covers ALL events with ~200-400 API calls (~2-3 min).

    Hard time limit of max_minutes prevents runaway execution.
    """
    import time as _time
    start = _time.time()
    deadline = start + max_minutes * 60

    print("[Article Enricher — GDELT DOC API]")
    print(f"  Time budget: {max_minutes} minutes")

    # Step 1: Group by country + primary category
    groups = defaultdict(list)
    for i, h in enumerate(hotspots):
        country = h.get("country", "Unknown")
        cat = h["categories"][0] if h.get("categories") else "politics"
        groups[(country, cat)].append(i)

    print(f"  {len(hotspots):,} events -> {len(groups):,} country+category groups")

    # Step 2: Sort by total intensity so biggest stories go first
    sorted_groups = sorted(
        groups.items(),
        key=lambda item: sum(hotspots[i].get("intensity", 0) for i in item[1]),
        reverse=True,
    )

    # Step 3: Query DOC API for each group (with time budget)
    enriched_groups = 0
    enriched_events = 0
    failed = 0
    timed_out = False

    for idx, ((country, cat), indices) in enumerate(sorted_groups):
        # Check time budget
        if _time.time() > deadline:
            timed_out = True
            print(f"    ⏱ Time budget reached at group {idx}/{len(sorted_groups)}")
            break

        # Build query from best event in group
        best_idx = max(indices, key=lambda i: hotspots[i].get("intensity", 0))
        query = hotspots[best_idx].get("searchQuery", "")
        if not query:
            cat_kw = CAT_KEYWORDS.get(cat, "news")
            query = f"{country} {cat_kw}"
        if len(query.strip()) < 3:
            continue

        articles = fetch_doc_api(query, max_records=5, timespan="3d")

        if articles:
            best_title = pick_best_title(articles)
            article_list = [
                {"title": a["title"], "url": a["url"], "source": a["source"], "lang": a.get("lang", "en")}
                for a in articles[:5]
            ]
            for i in indices:
                hotspots[i]["articles"] = article_list
                if best_title:
                    hotspots[i]["summary"] = best_title
            enriched_groups += 1
            enriched_events += len(indices)
        else:
            failed += 1

        # Progress every 50
        if (idx + 1) % 50 == 0:
            elapsed = _time.time() - start
            print(f"    Progress: {idx + 1}/{len(sorted_groups)} groups "
                  f"({enriched_events:,} events, {failed} failed, {elapsed:.0f}s elapsed)")

        if delay > 0:
            sleep(delay)

    # Fallback for unenriched events
    for h in hotspots:
        if "articles" not in h or not h["articles"]:
            articles = []
            for url in h.get("sourceUrls", [])[:1]:
                try:
                    domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                except Exception:
                    domain = "Source"
                articles.append({"title": "Read full report", "url": url, "source": domain, "lang": "en"})
            h["articles"] = articles

    elapsed = _time.time() - start
    print(f"  ✓ Enriched {enriched_groups:,} groups ({enriched_events:,} events) in {elapsed:.0f}s")
    if failed:
        print(f"    {failed:,} groups returned no results")
    if timed_out:
        remaining = len(sorted_groups) - idx
        print(f"    {remaining:,} groups skipped (time budget, kept template summaries)")

    return hotspots


if __name__ == "__main__":
    test = [{
        "city": "Kyiv", "country": "Ukraine", "categories": ["conflict"],
        "intensity": 95, "searchQuery": "Kyiv Ukraine military strike",
        "summary": "template", "sourceUrls": [],
    }]
    result = enrich(test, delay=0)
    print(f"\nSummary: {result[0]['summary']}")
    for a in result[0].get("articles", []):
        print(f"  {a['source']}: {a['title']}")
