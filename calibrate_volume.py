"""
Calibrate Google Trends indices to estimated monthly search volumes.

Uses pytrends comparison queries to get cross-keyword ratios against a
known anchor term, then converts to estimated volumes.

Anchor: "nursing degree" (CIP 51.3801) = 146,000 monthly searches in March 2025.

Creates/populates table `search_volume_calibration` in ipeds.db.
"""

import argparse
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from pytrends.request import TrendReq

DB_PATH = Path(__file__).parent / "ipeds.db"

ANCHOR_TERM = "nursing degree"
ANCHOR_CIP = "51.3801"
ANCHOR_VOLUME = 146_000          # monthly searches, March 2025
ANCHOR_TIMEFRAME = "2025-03-01 2025-03-31"

# Compare window — one month to get a clean ratio
COMPARE_TIMEFRAME = ANCHOR_TIMEFRAME

BATCH_SIZE = 4  # keywords per batch (+ anchor = 5 total, pytrends max)

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS search_volume_calibration (
    cipcode          TEXT    PRIMARY KEY,
    search_term      TEXT    NOT NULL,
    anchor_ratio     REAL    NOT NULL,
    est_monthly_vol  INTEGER,
    calibrated_at    TEXT    NOT NULL
);
"""


def get_pending_terms(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Return (cipcode, search_term) pairs not yet calibrated."""
    rows = conn.execute("""
        SELECT DISTINCT gt.cipcode, gt.search_term
        FROM google_trends_time gt
        LEFT JOIN search_volume_calibration sv ON gt.cipcode = sv.cipcode
        WHERE sv.cipcode IS NULL
        ORDER BY gt.cipcode
    """).fetchall()
    return [(r[0], r[1]) for r in rows]


def compare_batch(
    pt: TrendReq,
    anchor: str,
    keywords: list[str],
    timeframe: str,
) -> dict[str, float]:
    """Query pytrends with anchor + keywords and return {keyword: ratio}.

    Ratio = keyword_avg_interest / anchor_avg_interest.
    Returns 0.0 for keywords with no data.
    """
    terms = [anchor] + keywords
    pt.build_payload(terms, timeframe=timeframe, geo="US")
    df = pt.interest_over_time()

    if df.empty:
        return {kw: 0.0 for kw in keywords}

    # Drop the isPartial column if present
    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    anchor_avg = df[anchor].mean()
    if anchor_avg == 0:
        return {kw: 0.0 for kw in keywords}

    ratios = {}
    for kw in keywords:
        if kw in df.columns:
            ratios[kw] = df[kw].mean() / anchor_avg
        else:
            ratios[kw] = 0.0
    return ratios


def main():
    parser = argparse.ArgumentParser(
        description="Calibrate Google Trends keywords to estimated search volumes."
    )
    parser.add_argument(
        "--delay", type=int, default=15,
        help="Seconds to wait between API calls (default: 15).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be done without calling the API.",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Drop and recreate the calibration table before running.",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    if args.reset:
        conn.execute("DROP TABLE IF EXISTS search_volume_calibration")
        print("Dropped existing calibration table.")

    conn.executescript(TABLE_DDL)

    pending = get_pending_terms(conn)

    # Separate out the anchor — it gets ratio=1.0 automatically
    anchor_pending = [p for p in pending if p[0] == ANCHOR_CIP]
    others = [p for p in pending if p[0] != ANCHOR_CIP]

    if not others and not anchor_pending:
        print("All keywords already calibrated. Use --reset to recalibrate.")
        conn.close()
        return

    print(f"Anchor: '{ANCHOR_TERM}' = {ANCHOR_VOLUME:,} searches/month (March 2025)")
    print(f"Keywords to calibrate: {len(others)} (+1 anchor)")
    print(f"Batches needed: {(len(others) + BATCH_SIZE - 1) // BATCH_SIZE}")
    print(f"Delay between calls: {args.delay}s")
    print()

    if args.dry_run:
        for i in range(0, len(others), BATCH_SIZE):
            batch = others[i:i + BATCH_SIZE]
            terms = [t[1] for t in batch]
            print(f"  Batch {i // BATCH_SIZE + 1}: {terms}")
        print("\nDry run — no API calls made.")
        conn.close()
        return

    # Insert anchor first
    if anchor_pending:
        now = datetime.now().isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO search_volume_calibration VALUES (?, ?, ?, ?, ?)",
            (ANCHOR_CIP, ANCHOR_TERM, 1.0, ANCHOR_VOLUME, now),
        )
        conn.commit()
        print(f"  [OK] Anchor '{ANCHOR_TERM}' -> ratio=1.000, vol={ANCHOR_VOLUME:,}")

    pt = TrendReq(hl="en-US", tz=360)
    total_calls = 0

    for i in range(0, len(others), BATCH_SIZE):
        batch = others[i:i + BATCH_SIZE]
        batch_terms = [t[1] for t in batch]
        batch_num = i // BATCH_SIZE + 1

        print(f"  Batch {batch_num}: {batch_terms} ...", end=" ", flush=True)

        try:
            ratios = compare_batch(pt, ANCHOR_TERM, batch_terms, COMPARE_TIMEFRAME)
            total_calls += 1
        except Exception as e:
            err = str(e).lower()
            if "rate" in err or "429" in err:
                print(f"\n[!] Rate limited after {total_calls} calls. Re-run to resume.")
                break
            print(f"ERROR: {e}")
            # Skip this batch, can retry later
            continue

        now = datetime.now().isoformat()
        for cip, term in batch:
            ratio = ratios.get(term, 0.0)
            vol = round(ratio * ANCHOR_VOLUME)
            conn.execute(
                "INSERT OR REPLACE INTO search_volume_calibration VALUES (?, ?, ?, ?, ?)",
                (cip, term, round(ratio, 6), vol, now),
            )
            print(f"\n    {cip} '{term}' -> ratio={ratio:.4f}, vol={vol:,}", end="")

        conn.commit()
        print()

        # Rate-limit delay (skip after last batch)
        if i + BATCH_SIZE < len(others):
            time.sleep(args.delay)

    # Summary
    total = conn.execute("SELECT COUNT(*) FROM search_volume_calibration").fetchone()[0]
    top5 = conn.execute(
        "SELECT cipcode, search_term, est_monthly_vol "
        "FROM search_volume_calibration ORDER BY est_monthly_vol DESC LIMIT 5"
    ).fetchall()

    print(f"\nDone. {total} keywords calibrated.")
    print("\nTop 5 by estimated monthly volume:")
    for cip, term, vol in top5:
        print(f"  {cip} '{term}': {vol:,}")

    conn.close()


if __name__ == "__main__":
    main()
