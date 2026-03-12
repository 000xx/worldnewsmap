"""
Orchestrator — Phase 2
Runs the complete pipeline:
  1. Collect events from GDELT daily exports (gdelt_collector)
  2. Enrich with real headlines via GDELT DOC 2.0 API (article_enricher)
  3. Write final JSON output for the frontend
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gdelt_collector import collect
from article_enricher import enrich


CONFIG = {
    "min_sources": int(os.environ.get("NEWSMAP_MIN_SOURCES", "3")),
    "max_age_hours": int(os.environ.get("NEWSMAP_MAX_AGE_HOURS", "36")),
    "num_days": int(os.environ.get("NEWSMAP_NUM_DAYS", "3")),
    "output_path": os.environ.get("NEWSMAP_OUTPUT", "data/live.json"),
    "api_delay": float(os.environ.get("NEWSMAP_API_DELAY", "0.5")),
}


def run_pipeline():
    start_time = datetime.now(timezone.utc)
    print("=" * 60)
    print(f"World News Map Pipeline — {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)
    print()

    # Step 1: Collect from GDELT
    hotspots = collect(
        min_sources=CONFIG["min_sources"],
        max_age_hours=CONFIG["max_age_hours"],
        num_days=CONFIG["num_days"],
    )

    if not hotspots:
        print("\n✗ No hotspots collected.")
        write_output([], start_time)
        return

    print(f"\n  Total events: {len(hotspots):,}")

    # Step 2: Enrich with real headlines via DOC API
    print()
    hotspots = enrich(hotspots, delay=CONFIG["api_delay"])

    # Step 3: Write output
    print()
    output = format_output(hotspots, start_time)
    write_output(output, start_time)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"\n{'=' * 60}")
    print(f"Pipeline complete in {elapsed:.1f}s — {len(hotspots):,} events written")
    print(f"{'=' * 60}")


def format_output(hotspots, timestamp):
    output = []
    for h in hotspots:
        articles = []
        for a in h.get("articles", []):
            articles.append({
                "title": a.get("title", "Untitled"),
                "url": a.get("url", "#"),
                "source": a.get("source", "Unknown"),
                "lang": a.get("lang", "en"),
            })

        # Fallback: use GDELT source URL if no articles
        if not articles:
            for url in h.get("sourceUrls", [])[:1]:
                try:
                    import urllib.parse
                    domain = urllib.parse.urlparse(url).netloc.replace("www.", "")
                except Exception:
                    domain = "Source"
                articles.append({
                    "title": "Read full report",
                    "url": url,
                    "source": domain,
                    "lang": "en",
                })

        output.append({
            "lat": h["lat"],
            "lng": h["lng"],
            "city": h["city"],
            "country": h["country"],
            "continent": h.get("continent", "Other"),
            "categories": h["categories"],
            "intensity": h["intensity"],
            "numSources": h["numSources"],
            "numMentions": h.get("numMentions", 0),
            "numArticles": h.get("numArticles", 0),
            "avgTone": h["avgTone"],
            "avgGoldstein": h.get("avgGoldstein", 0),
            "eventCount": h.get("eventCount", 1),
            "hoursAgo": h.get("hoursAgo"),
            "summary": h.get("summary", ""),
            "notableNames": h.get("notable_names", []),
            "articles": articles,
        })
    return output


def write_output(data, timestamp):
    output_path = CONFIG["output_path"]
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    envelope = {
        "generated_at": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hotspot_count": len(data),
        "min_sources_threshold": CONFIG["min_sources"],
        "hotspots": data,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
    print(f"[Output Writer]")
    print(f"  ✓ Wrote {output_path} ({size_str}, {len(data):,} events)")


if __name__ == "__main__":
    run_pipeline()
