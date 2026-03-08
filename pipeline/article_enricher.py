"""
Article Enricher — Phase 2
Takes GDELT hotspot clusters and enriches them with readable article
headlines from Google News RSS and (optionally) The Guardian API.
"""

import re
import xml.etree.ElementTree as ET
import urllib.request
import urllib.parse
import json
from html import unescape
from time import sleep


# ── Google News RSS ─────────────────────────────────────────────
def fetch_google_news_rss(query, max_results=5):
    """
    Fetch articles from Google News RSS for a given query string.
    Returns list of {title, url, source, published}.
    """
    encoded = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=en&gl=US&ceid=US:en"

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "WorldNewsMap/1.0",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            xml_data = resp.read().decode("utf-8", errors="replace")

        root = ET.fromstring(xml_data)
        items = root.findall(".//item")

        articles = []
        for item in items[:max_results]:
            title_el = item.find("title")
            link_el = item.find("link")
            pub_el = item.find("pubDate")
            source_el = item.find("source")

            title = unescape(title_el.text) if title_el is not None and title_el.text else ""
            link = link_el.text if link_el is not None and link_el.text else ""
            pub = pub_el.text if pub_el is not None and pub_el.text else ""
            source = source_el.text if source_el is not None and source_el.text else ""

            # Clean up title (Google News sometimes appends " - Source Name")
            title_clean = re.sub(r'\s*-\s*[^-]+$', '', title) if ' - ' in title else title

            if title_clean and link:
                articles.append({
                    "title": title_clean.strip(),
                    "url": link.strip(),
                    "source": source.strip() or "Google News",
                    "published": pub.strip(),
                    "lang": "en",
                })

        return articles
    except Exception as e:
        print(f"    ✗ RSS fetch failed for '{query}': {e}")
        return []


# ── The Guardian API ────────────────────────────────────────────
def fetch_guardian_articles(query, api_key=None, max_results=3):
    """
    Fetch articles from The Guardian's Open Platform API.
    Requires a free API key from https://open-platform.theguardian.com/
    Pass api_key=None to skip this source.
    """
    if not api_key:
        return []

    try:
        encoded = urllib.parse.quote(query)
        url = (
            f"https://content.guardianapis.com/search"
            f"?q={encoded}&page-size={max_results}"
            f"&order-by=newest&api-key={api_key}"
        )

        req = urllib.request.Request(url, headers={"User-Agent": "WorldNewsMap/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        articles = []
        for result in data.get("response", {}).get("results", []):
            articles.append({
                "title": result.get("webTitle", "").strip(),
                "url": result.get("webUrl", "").strip(),
                "source": "The Guardian",
                "published": result.get("webPublicationDate", ""),
                "lang": "en",
            })

        return articles
    except Exception as e:
        print(f"    ✗ Guardian fetch failed for '{query}': {e}")
        return []


# ── Build search queries from hotspot data ──────────────────────
def build_query(hotspot):
    """
    Construct a search query from a hotspot's location and category.
    Aims for specific, relevant results.
    """
    city = hotspot["city"]
    country = hotspot["country"]
    cat = hotspot["categories"][0] if hotspot["categories"] else ""

    # Category-specific keywords for better relevance
    cat_keywords = {
        "conflict": "conflict violence",
        "politics": "politics government",
        "economy": "economy market",
        "environment": "environment climate",
        "humanitarian": "humanitarian aid crisis",
        "health": "health medical",
        "positive": "breakthrough achievement",
    }

    keyword = cat_keywords.get(cat, "news")
    return f"{city} {country} {keyword}"


# ── Enrich hotspots with articles ───────────────────────────────
def enrich(hotspots, guardian_api_key=None, max_hotspots=50, delay=1.0):
    """
    Enrich the top N hotspots with readable articles.
    Adds an 'articles' list to each hotspot dict.

    Args:
        hotspots: list of hotspot dicts from gdelt_collector
        guardian_api_key: optional Guardian API key (free tier)
        max_hotspots: limit enrichment to top N by intensity
        delay: seconds between RSS requests to be polite
    """
    print(f"[Article Enricher]")
    print(f"  Enriching top {min(max_hotspots, len(hotspots))} hotspots...")

    enriched_count = 0

    for i, hotspot in enumerate(hotspots[:max_hotspots]):
        query = build_query(hotspot)
        articles = []

        # 1. GDELT source URLs (already have these)
        gdelt_urls = hotspot.get("sourceUrls", [])
        for url in gdelt_urls[:3]:
            # Extract domain as source name
            try:
                domain = urllib.parse.urlparse(url).netloc
                domain = re.sub(r'^www\.', '', domain)
            except:
                domain = "News Source"

            articles.append({
                "title": f"Source report from {domain}",
                "url": url,
                "source": domain,
                "lang": "en",
            })

        # 2. Google News RSS
        rss_articles = fetch_google_news_rss(query, max_results=3)
        articles.extend(rss_articles)

        # 3. Guardian API (if key provided)
        guardian_articles = fetch_guardian_articles(query, api_key=guardian_api_key, max_results=2)
        articles.extend(guardian_articles)

        # Deduplicate by URL
        seen_urls = set()
        unique_articles = []
        for a in articles:
            if a["url"] not in seen_urls:
                seen_urls.add(a["url"])
                unique_articles.append(a)

        hotspot["articles"] = unique_articles[:6]  # Cap at 6 articles per hotspot

        if unique_articles:
            enriched_count += 1

        # Rate limit
        if i < max_hotspots - 1 and (rss_articles or guardian_articles):
            sleep(delay)

    # For hotspots beyond max_hotspots, add GDELT URLs only
    for hotspot in hotspots[max_hotspots:]:
        articles = []
        for url in hotspot.get("sourceUrls", [])[:3]:
            try:
                domain = urllib.parse.urlparse(url).netloc
                domain = re.sub(r'^www\.', '', domain)
            except:
                domain = "News Source"
            articles.append({
                "title": f"Source report from {domain}",
                "url": url,
                "source": domain,
                "lang": "en",
            })
        hotspot["articles"] = articles

    print(f"  ✓ Enriched {enriched_count} hotspots with external articles")
    return hotspots


if __name__ == "__main__":
    # Quick test with fake hotspot
    test_hotspot = {
        "city": "Nairobi",
        "country": "Kenya",
        "categories": ["politics"],
        "sourceUrls": [],
    }
    enriched = enrich([test_hotspot], max_hotspots=1, delay=0)
    for a in enriched[0].get("articles", []):
        print(f"  {a['source']}: {a['title']}")
        print(f"    {a['url']}")
