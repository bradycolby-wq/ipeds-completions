"""
Batch-fetch Google Trends data for top CIP codes into SQLite.

Creates/updates tables:
  - google_trends_time    (monthly national interest over time)
  - google_trends_state   (state-level aggregate interest)
  - google_trends_dma     (DMA-level aggregate interest)
  - google_trends_progress (checkpoint tracking)

Rate limit: ~200 queries/day. Script checkpoints progress and can be
re-run to continue where it left off.

Usage:
    python load_google_trends.py [--limit N] [--delay SECONDS] [--retry-errors] [--dry-run]
"""

import argparse
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent / "ipeds.db"

# ── Configurable defaults ─────────────────────────────────────────────────────
DEFAULT_LIMIT = 500
DEFAULT_DELAY = 15        # seconds between API calls
MAX_CALLS_PER_SESSION = 180  # safety cap per run
TIMEFRAME = "2021-01-01 2026-03-01"  # ~5 years, monthly resolution

# ── Curated search term overrides for CIP codes with awkward titles ──────────
CIP_SEARCH_OVERRIDES = {
    "99":       None,   # skip aggregate "all programs"
    "24.0101":  "liberal arts degree",
    "52.0201":  "business administration degree",
    "51.3801":  "nursing degree",
    "42.0101":  "psychology degree",
    "24.0102":  "general studies degree",
    "26.0101":  "biology degree",
    "52.0301":  "accounting degree",
    "51.0801":  "medical assistant certificate",
    "12.0401":  "cosmetology license",
    "52.0101":  "business degree",
    "44.0701":  "social work degree",
    "43.0104":  "criminal justice degree",
    "51.3901":  "LPN program",
    "11.0701":  "computer science degree",
    "52.0801":  "finance degree",
    "11.0101":  "computer information systems degree",
    "52.1401":  "marketing degree",
    "14.1901":  "mechanical engineering degree",
    "48.0508":  "welding certification",
    "51.0602":  "dental hygiene degree",
    "13.1202":  "elementary education degree",
    "51.0000":  "health sciences degree",
    "27.0101":  "mathematics degree",
    "51.0912":  "physician assistant program",
    "23.0101":  "english degree",
    "40.0501":  "chemistry degree",
    "45.1001":  "political science degree",
    "11.0401":  "information technology degree",
    "09.0100":  "communications degree",
    "22.0302":  "paralegal certificate",
    "31.0505":  "kinesiology degree",
    "13.0101":  "education degree",
    "51.2001":  "pharmacy degree",
    "51.0001":  "health professions degree",
    "51.1201":  "medicine degree",
    "04.0201":  "architecture degree",
    "11.0102":  "artificial intelligence degree",
    "11.0103":  "data science degree",
    "30.7001":  "data analytics degree",
    "52.1301":  "management science degree",
    "52.1101":  "international business degree",
    "14.0901":  "computer engineering degree",
    "14.1001":  "electrical engineering degree",
    "14.0801":  "civil engineering degree",
    "30.0801":  "math and computer science",
    "51.2201":  "public health degree",
    "54.0101":  "history degree",
    "45.1101":  "sociology degree",
    "40.0801":  "physics degree",
    "50.0409":  "graphic design degree",
}


# ── CIP-to-search-term mapping ───────────────────────────────────────────────

def cip_to_search_term(cipcode: str, ciptitle: str) -> str | None:
    """Map a CIP code to a Google Trends search term.

    Returns None for CIP codes that should be skipped.
    """
    if cipcode in CIP_SEARCH_OVERRIDES:
        return CIP_SEARCH_OVERRIDES[cipcode]
    if not ciptitle or ciptitle == cipcode:
        return None
    # Take first phrase before comma, append "degree"
    clean = ciptitle.split(",")[0].strip()
    clean = re.sub(r'\s*\([^)]*\)', '', clean).strip()
    if len(clean) < 3:
        return None
    return f"{clean} degree".lower()


# ── DB helpers ────────────────────────────────────────────────────────────────

def create_tables(conn: sqlite3.Connection):
    """Create Google Trends tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS google_trends_time (
            cipcode     TEXT    NOT NULL,
            search_term TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            interest    INTEGER,
            is_partial  INTEGER DEFAULT 0,
            PRIMARY KEY (cipcode, date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS google_trends_state (
            cipcode     TEXT    NOT NULL,
            search_term TEXT    NOT NULL,
            state_abbr  TEXT    NOT NULL,
            interest    INTEGER,
            PRIMARY KEY (cipcode, state_abbr)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS google_trends_dma (
            cipcode     TEXT    NOT NULL,
            search_term TEXT    NOT NULL,
            dma_code    TEXT    NOT NULL,
            dma_name    TEXT,
            interest    INTEGER,
            PRIMARY KEY (cipcode, dma_code)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS google_trends_progress (
            cipcode     TEXT    NOT NULL,
            call_type   TEXT    NOT NULL,
            status      TEXT    NOT NULL DEFAULT 'pending',
            error_msg   TEXT,
            updated_at  TEXT,
            PRIMARY KEY (cipcode, call_type)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gtt_cip ON google_trends_time(cipcode)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gts_cip ON google_trends_state(cipcode)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gts_st ON google_trends_state(state_abbr)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gtd_cip ON google_trends_dma(cipcode)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gtd_dma ON google_trends_dma(dma_code)")
    conn.commit()


def get_top_cip_codes(conn: sqlite3.Connection, limit: int) -> list[tuple[str, str]]:
    """Return top CIP codes by total completions: [(cipcode, ciptitle), ...]"""
    rows = conn.execute(f"""
        SELECT c.cipcode, COALESCE(t.ciptitle, c.cipcode) as title
        FROM (
            SELECT cipcode, SUM(ctotalt) as total
            FROM completions
            WHERE majornum = 1 AND ctotalt > 0
            GROUP BY cipcode
            ORDER BY total DESC
            LIMIT ?
        ) c
        LEFT JOIN cip_taxonomy t ON c.cipcode = t.cipcode
    """, (limit,)).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_progress(conn: sqlite3.Connection, cipcode: str, call_type: str) -> str:
    """Return status for a (cipcode, call_type) pair, or 'pending'."""
    row = conn.execute(
        "SELECT status FROM google_trends_progress WHERE cipcode=? AND call_type=?",
        (cipcode, call_type),
    ).fetchone()
    return row[0] if row else "pending"


def update_progress(
    conn: sqlite3.Connection,
    cipcode: str,
    call_type: str,
    status: str,
    error_msg: str | None = None,
):
    """Insert or update progress for a (cipcode, call_type)."""
    conn.execute(
        "INSERT OR REPLACE INTO google_trends_progress "
        "(cipcode, call_type, status, error_msg, updated_at) VALUES (?,?,?,?,?)",
        (cipcode, call_type, status, error_msg, datetime.now().isoformat()),
    )
    conn.commit()


# ── Google Trends fetch functions ─────────────────────────────────────────────

def fetch_time_series(pt, search_term: str) -> pd.DataFrame:
    """Fetch national monthly interest over time."""
    pt.build_payload([search_term], timeframe=TIMEFRAME, geo="US")
    df = pt.interest_over_time()
    if df.empty:
        return pd.DataFrame(columns=["date", "interest", "is_partial"])
    df = df.reset_index()
    df = df.rename(columns={"date": "date", search_term: "interest", "isPartial": "is_partial"})
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df["is_partial"] = df["is_partial"].astype(int)
    return df[["date", "interest", "is_partial"]]


def fetch_state_data(pt, search_term: str) -> pd.DataFrame:
    """Fetch state-level aggregate interest."""
    pt.build_payload([search_term], timeframe=TIMEFRAME, geo="US")
    df = pt.interest_by_region(resolution="REGION", inc_geo_code=True)
    if df.empty:
        return pd.DataFrame(columns=["state_abbr", "interest"])
    df = df.reset_index()
    df = df.rename(columns={search_term: "interest", "geoCode": "geo_code"})
    # geo_code is "US-CA" format; extract state abbreviation
    df["state_abbr"] = df["geo_code"].str.replace("US-", "", regex=False)
    return df[["state_abbr", "interest"]]


def fetch_dma_data(pt, search_term: str) -> pd.DataFrame:
    """Fetch DMA-level aggregate interest."""
    pt.build_payload([search_term], timeframe=TIMEFRAME, geo="US")
    df = pt.interest_by_region(resolution="DMA", inc_geo_code=True)
    if df.empty:
        return pd.DataFrame(columns=["dma_code", "dma_name", "interest"])
    df = df.reset_index()
    df = df.rename(columns={search_term: "interest", "geoCode": "dma_code", "geoName": "dma_name"})
    df["dma_code"] = df["dma_code"].astype(str)
    return df[["dma_code", "dma_name", "interest"]]


# ── Store functions ───────────────────────────────────────────────────────────

def store_time_data(conn: sqlite3.Connection, cipcode: str, search_term: str, df: pd.DataFrame):
    """Store national time series data."""
    conn.execute("DELETE FROM google_trends_time WHERE cipcode=?", (cipcode,))
    for _, row in df.iterrows():
        conn.execute(
            "INSERT OR REPLACE INTO google_trends_time "
            "(cipcode, search_term, date, interest, is_partial) VALUES (?,?,?,?,?)",
            (cipcode, search_term, row["date"], int(row["interest"]), int(row["is_partial"])),
        )
    conn.commit()


def store_state_data(conn: sqlite3.Connection, cipcode: str, search_term: str, df: pd.DataFrame):
    """Store state-level interest data."""
    conn.execute("DELETE FROM google_trends_state WHERE cipcode=?", (cipcode,))
    for _, row in df.iterrows():
        conn.execute(
            "INSERT OR REPLACE INTO google_trends_state "
            "(cipcode, search_term, state_abbr, interest) VALUES (?,?,?,?)",
            (cipcode, search_term, row["state_abbr"], int(row["interest"])),
        )
    conn.commit()


def store_dma_data(conn: sqlite3.Connection, cipcode: str, search_term: str, df: pd.DataFrame):
    """Store DMA-level interest data."""
    conn.execute("DELETE FROM google_trends_dma WHERE cipcode=?", (cipcode,))
    for _, row in df.iterrows():
        conn.execute(
            "INSERT OR REPLACE INTO google_trends_dma "
            "(cipcode, search_term, dma_code, dma_name, interest) VALUES (?,?,?,?,?)",
            (cipcode, search_term, row["dma_code"], row["dma_name"], int(row["interest"])),
        )
    conn.commit()


# ── Main batch orchestration ─────────────────────────────────────────────────

def process_one_cip(pt, conn, cipcode, search_term, delay):
    """Fetch all three data types for one CIP code.

    Checks progress and skips completed calls. Returns number of API calls made.
    """
    calls = 0
    tasks = [
        ("time", fetch_time_series, store_time_data),
        ("state", fetch_state_data, store_state_data),
        ("dma", fetch_dma_data, store_dma_data),
    ]
    for call_type, fetch_fn, store_fn in tasks:
        status = get_progress(conn, cipcode, call_type)
        if status == "done":
            continue
        try:
            time.sleep(delay)
            df = fetch_fn(pt, search_term)
            store_fn(conn, cipcode, search_term, df)
            update_progress(conn, cipcode, call_type, "done")
            calls += 1
        except Exception as e:
            err = str(e)
            update_progress(conn, cipcode, call_type, "error", err)
            if "429" in err or "rate" in err.lower() or "too many" in err.lower():
                print(f"    RATE LIMITED: {err[:80]}")
                raise  # stop the session
            print(f"    Error ({call_type}): {err[:80]}")
    return calls


def main():
    parser = argparse.ArgumentParser(description="Batch-fetch Google Trends data")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Number of top CIP codes to fetch (default: {DEFAULT_LIMIT})")
    parser.add_argument("--delay", type=int, default=DEFAULT_DELAY,
                        help=f"Seconds between API calls (default: {DEFAULT_DELAY})")
    parser.add_argument("--retry-errors", action="store_true",
                        help="Reset error statuses to pending and retry")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be fetched without calling the API")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    # Get top CIP codes and map to search terms
    print(f"Loading top {args.limit} CIP codes by completions volume...")
    cip_list = get_top_cip_codes(conn, args.limit)
    work_items = []
    for cipcode, ciptitle in cip_list:
        term = cip_to_search_term(cipcode, ciptitle)
        if term:
            work_items.append((cipcode, ciptitle, term))

    print(f"  {len(work_items)} CIP codes mapped to search terms")

    # Initialize progress rows
    for cipcode, _, _ in work_items:
        for call_type in ("time", "state", "dma"):
            status = get_progress(conn, cipcode, call_type)
            if status == "":
                update_progress(conn, cipcode, call_type, "pending")

    # Reset errors if requested
    if args.retry_errors:
        n_reset = conn.execute(
            "UPDATE google_trends_progress SET status='pending', error_msg=NULL "
            "WHERE status='error'"
        ).rowcount
        conn.commit()
        print(f"  Reset {n_reset} error statuses to pending")

    # Count pending work
    n_done = 0
    n_pending = 0
    for cipcode, _, _ in work_items:
        for ct in ("time", "state", "dma"):
            s = get_progress(conn, cipcode, ct)
            if s == "done":
                n_done += 1
            else:
                n_pending += 1
    total = n_done + n_pending
    print(f"  Progress: {n_done}/{total} calls done, {n_pending} pending")

    if args.dry_run:
        print("\n[DRY RUN] Would fetch the following:")
        shown = 0
        for cipcode, title, term in work_items:
            pending = [
                ct for ct in ("time", "state", "dma")
                if get_progress(conn, cipcode, ct) != "done"
            ]
            if pending:
                print(f"  {cipcode} ({term}): {', '.join(pending)}")
                shown += 1
                if shown >= 20:
                    remaining = sum(
                        1 for c, _, _ in work_items
                        for ct in ("time", "state", "dma")
                        if get_progress(conn, c, ct) != "done"
                    ) - shown * 3
                    print(f"  ... and {remaining} more calls")
                    break
        conn.close()
        return

    # Initialize Google Trends client
    from pytrends.request import TrendReq
    pt = TrendReq(hl="en-US", tz=360)

    total_calls = 0
    items_done = 0
    print(f"\nStarting batch (max {MAX_CALLS_PER_SESSION} calls, {args.delay}s delay)...\n")

    try:
        for i, (cipcode, title, term) in enumerate(work_items):
            # Check if all calls for this CIP are done
            pending_types = [
                ct for ct in ("time", "state", "dma")
                if get_progress(conn, cipcode, ct) != "done"
            ]
            if not pending_types:
                continue

            print(f"  [{i+1}/{len(work_items)}] {cipcode} -> \"{term}\"")
            calls = process_one_cip(pt, conn, cipcode, term, args.delay)
            total_calls += calls
            if calls > 0:
                items_done += 1

            if total_calls >= MAX_CALLS_PER_SESSION:
                print(f"\n  Rate limit safety cap reached ({MAX_CALLS_PER_SESSION} calls).")
                print("  Re-run to continue where you left off.")
                break
    except KeyboardInterrupt:
        print("\n  Interrupted. Progress saved. Re-run to continue.")
    except Exception as e:
        if "429" in str(e) or "rate" in str(e).lower():
            print(f"\n  Rate limited after {total_calls} calls. Re-run later to continue.")
        else:
            raise

    # Final summary
    n_done_final = conn.execute(
        "SELECT COUNT(*) FROM google_trends_progress WHERE status='done'"
    ).fetchone()[0]
    n_err = conn.execute(
        "SELECT COUNT(*) FROM google_trends_progress WHERE status='error'"
    ).fetchone()[0]
    n_pend = conn.execute(
        "SELECT COUNT(*) FROM google_trends_progress WHERE status='pending'"
    ).fetchone()[0]

    print(f"\n{'='*50}")
    print(f"Session: {total_calls} API calls for {items_done} CIP codes")
    print(f"Overall: {n_done_final} done, {n_pend} pending, {n_err} errors")
    print(f"{'='*50}")

    conn.close()


if __name__ == "__main__":
    main()
