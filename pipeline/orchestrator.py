"""
Orchestrator — Phase 2
Runs the complete pipeline:
  1. Collect events from GDELT daily exports (gdelt_collector)
  2. Enrich hotspots with articles (article_enricher)
  3. Write final JSON output for the frontend

Configuration via environment variables or defaults.
"""

import json
import os
import sys
from datetime import datetime, timezone

# Add pipeline directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gdelt_collector import collect
from article_enricher import enrich


# ── Configuration ───────────────────────────────────────────────
CONFIG = {
    # Minimum number of unique sources for an event to be included.
    # 5 is a good balance: filters noise but keeps global coverage.
    "min_sources": int(os.environ.get("NEWSMAP_MIN_SOURCES", "5")),

    # How far back to look for events (hours). 36h covers ~1.5 news cycles.
    "max_age_hours": int(os.environ.get("NEWSMAP_MAX_AGE_HOURS", "36")),

    # Number of daily export files to fetch (3 days covers the 36h window fully).
    "num_days": int(os.environ.get("NEWSMAP_NUM_DAYS", "3")),

    # Maximum hotspots to enrich with external articles (RSS/Guardian)
    "max_enrich": int(os.environ.get("NEWSMAP_MAX_ENRICH", "50")),

    # Guardian API key (free from https://open-platform.theguardian.com/)
    "guardian_api_key": os.environ.get("GUARDIAN_API_KEY", None),

    # Output file path
    "output_path": os.environ.get("NEWSMAP_OUTPUT", "data/live.json"),

    # Maximum total hotspots in output (keeps file size manageable)
    "max_hotspots": int(os.environ.get("NEWSMAP_MAX_HOTSPOTS", "200")),

    # Delay between RSS requests (seconds)
    "rss_delay": float(os.environ.get("NEWSMAP_RSS_DELAY", "1.0")),
}


def run_pipeline():
    """Execute the full data pipeline."""
    start_time = datetime.now(timezone.utc)
    print("=" * 60)
    print(f"World News Map Pipeline — {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)
    print()

    # ── Step 1: Collect from GDELT ──
    hotspots = collect(
        min_sources=CONFIG["min_sources"],
        max_age_hours=CONFIG["max_age_hours"],
        num_days=CONFIG["num_days"],
    )

    if not hotspots:
        print("\n✗ No hotspots collected. Check GDELT availability.")
        print("  Writing empty dataset...")
        write_output([], start_time)
        return

    print(f"\n  Total hotspots: {len(hotspots)}")

    # Trim to max
    hotspots = hotspots[:CONFIG["max_hotspots"]]

    # ── Step 2: Enrich with articles ──
    print()
    hotspots = enrich(
        hotspots,
        guardian_api_key=CONFIG["guardian_api_key"],
        max_hotspots=CONFIG["max_enrich"],
        delay=CONFIG["rss_delay"],
    )

    # ── Step 3: Clean and write output ──
    print()
    output = format_output(hotspots, start_time)
    write_output(output, start_time)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"\n{'=' * 60}")
    print(f"Pipeline complete in {elapsed:.1f}s — {len(hotspots)} hotspots written")
    print(f"{'=' * 60}")


def format_output(hotspots, timestamp):
    """
    Format hotspots into the final JSON structure expected by the frontend.
    Cleans up internal fields and ensures consistent schema.
    """
    output = []

    for h in hotspots:
        # Build articles list, ensuring all have required fields
        articles = []
        for a in h.get("articles", []):
            articles.append({
                "title": a.get("title", "Untitled"),
                "url": a.get("url", "#"),
                "source": a.get("source", "Unknown"),
                "lang": a.get("lang", "en"),
            })

        output.append({
            "lat": h["lat"],
            "lng": h["lng"],
            "city": h["city"],
            "country": h["country"],
            "categories": h["categories"],
            "intensity": h["intensity"],
            "numSources": h["numSources"],
            "numMentions": h.get("numMentions", 0),
            "numArticles": h.get("numArticles", 0),
            "avgTone": h["avgTone"],
            "avgGoldstein": h.get("avgGoldstein", 0),
            "eventCount": h.get("eventCount", 1),
            "hoursAgo": h.get("hoursAgo"),
            "articles": articles,
        })

    return output


def write_output(data, timestamp):
    """Write the final JSON file for the frontend to consume."""
    output_path = CONFIG["output_path"]

    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    # Wrap in metadata envelope
    envelope = {
        "generated_at": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hotspot_count": len(data),
        "min_sources_threshold": CONFIG["min_sources"],
        "max_age_hours": CONFIG["max_age_hours"],
        "hotspots": data,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    print(f"[Output Writer]")
    print(f"  ✓ Wrote {output_path} ({size_kb:.1f} KB, {len(data)} hotspots)")


if __name__ == "__main__":
    run_pipeline()
