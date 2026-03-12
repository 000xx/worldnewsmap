"""
Orchestrator — Phase 4 (BigQuery + Groq)
Runs the pipeline:
  1. Check state: FULL (36h) or DELTA (30min) mode
  2. Query BigQuery for GKG records
  3. Merge with existing data if DELTA mode
  4. Write JSON for frontend (Groq summarizes on-click in browser)
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bigquery_collector import (
    collect_from_bigquery,
    load_state,
    save_state,
    merge_hotspots,
)


CONFIG = {
    "full_window_hours": int(os.environ.get("NEWSMAP_FULL_HOURS", "36")),
    "delta_window_minutes": int(os.environ.get("NEWSMAP_DELTA_MINUTES", "30")),
    "max_age_hours": int(os.environ.get("NEWSMAP_MAX_AGE_HOURS", "36")),
    "output_path": os.environ.get("NEWSMAP_OUTPUT", "data/live.json"),
}


def run_pipeline():
    start_time = datetime.now(timezone.utc)
    print("=" * 60)
    print(f"World News Map Pipeline — {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    # Decide: FULL or DELTA mode
    last_collected = load_state()
    output_path = CONFIG["output_path"]

    if last_collected and os.path.exists(output_path):
        # DELTA mode: only fetch last 30 minutes
        age_minutes = (start_time - last_collected).total_seconds() / 60
        # If more than 2 hours since last run, do full refresh
        if age_minutes > 120:
            print(f"\n  Last run was {age_minutes:.0f} min ago (>120), doing FULL refresh")
            mode = "FULL"
        else:
            mode = "DELTA"
    else:
        mode = "FULL"

    print(f"\n  Mode: {mode}")

    if mode == "FULL":
        # Full 36-hour pull
        hotspots = collect_from_bigquery(
            since_hours=CONFIG["full_window_hours"]
        )
        if not hotspots:
            print("\n✗ No hotspots collected.")
            write_output([], start_time)
            save_state(start_time)
            return
    else:
        # Delta: fetch only the last 30 minutes
        since_hours = CONFIG["delta_window_minutes"] / 60.0
        new_hotspots = collect_from_bigquery(since_hours=since_hours)

        # Load existing data
        existing = load_existing_hotspots(output_path)
        print(f"\n  Existing hotspots: {len(existing):,}")
        print(f"  New hotspots from delta: {len(new_hotspots):,}")

        # Merge
        hotspots = merge_hotspots(
            existing,
            new_hotspots,
            max_age_hours=CONFIG["max_age_hours"],
        )
        print(f"  Merged total: {len(hotspots):,}")

    # Write output
    print()
    write_output(hotspots, start_time)
    save_state(start_time)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"\n{'=' * 60}")
    print(f"Pipeline complete in {elapsed:.1f}s — {len(hotspots):,} hotspots ({mode})")
    print(f"{'=' * 60}")


def load_existing_hotspots(path):
    """Load existing hotspots from the output JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("hotspots", [])
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def write_output(hotspots, timestamp):
    output_path = CONFIG["output_path"]
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    envelope = {
        "generated_at": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hotspot_count": len(hotspots),
        "hotspots": hotspots,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
    print(f"[Output Writer]")
    print(f"  ✓ Wrote {output_path} ({size_str}, {len(hotspots):,} hotspots)")


if __name__ == "__main__":
    run_pipeline()
