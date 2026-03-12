"""
Build cip_crosswalk table by scraping CIP 2020 detail pages.

For each code that first appears in AY 2019-20 or later (year >= 2020),
fetch its NCES CIP detail page and extract any "Moved from X" action text.

Table: cip_crosswalk(new_cipcode TEXT, old_cipcode TEXT)
  new_cipcode — CIP 2020 code used in completions from year 2020+
  old_cipcode — CIP 2010 code it replaced (may have multiple rows per new_cipcode)
"""

import re
import sqlite3
import time
import urllib.request
from pathlib import Path

DB_PATH = Path(__file__).parent / "ipeds.db"
BASE_URL = "https://nces.ed.gov/ipeds/cipcode/cipdetail.aspx?y=56&cip={}"
HEADERS  = {"User-Agent": "Mozilla/5.0 (research)"}
SLEEP_S  = 0.3   # polite crawl delay


# ── Scraping helpers ───────────────────────────────────────────────────────────

def fetch_action_text(cipcode: str) -> str:
    """Return the 'Action:' paragraph text from a CIP 2020 detail page, or ''."""
    url = BASE_URL.format(cipcode)
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [WARN] {cipcode}: {e}")
        return ""

    m = re.search(r"<strong>Action:</strong>\s*(.*?)</p>", html, re.S)
    if not m:
        return ""
    raw = m.group(1)
    return re.sub(r"<[^>]+>", "", raw).strip()


def parse_old_codes(action_text: str) -> list[str]:
    """
    Extract old CIP code(s) from action text.

    Patterns seen:
      "Moved from 51.2401 to 01.8001"
      "Moved from 43.0106 and 43.0111 to 43.0401"
      "Moved from 30.1701 and 30.1801 to 30.3401"
      "Merged from 30.9999 to 30.0001"
      "Renamed from 01.0309 to 01.0309"  (code unchanged — skip)
    """
    # Find all 6-digit CIP codes in the action text
    all_codes = re.findall(r"\b\d{2}\.\d{4}\b", action_text)
    text_lower = action_text.lower()

    if not all_codes:
        return []

    # "Renamed" typically keeps the same code — skip if all codes are identical
    if re.search(r"\brenam", text_lower):
        unique = set(all_codes)
        if len(unique) == 1:
            return []  # same code, not a real change

    # For "Moved from X to Y" / "Merged from X to Y":
    # The old codes appear before "to" in the sentence.
    m = re.search(r"(?:moved|merged|transferred)\s+from\s+([\d.,\s]+?)\s+to\s+", text_lower)
    if m:
        old_part = m.group(1)
        old_codes = re.findall(r"\d{2}\.\d{4}", old_part)
        return old_codes

    return []


# ── DB setup ──────────────────────────────────────────────────────────────────

def setup_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cip_crosswalk (
            new_cipcode TEXT NOT NULL,
            old_cipcode TEXT NOT NULL,
            action_text TEXT,
            PRIMARY KEY (new_cipcode, old_cipcode)
        )
    """)
    conn.commit()


def get_new_codes(conn) -> list[str]:
    """Return all CIP codes that first appear in year 2020+ (CIP 2020 additions)."""
    rows = conn.execute("""
        SELECT cipcode FROM completions
        GROUP BY cipcode
        HAVING MIN(year) >= 2020
        ORDER BY cipcode
    """).fetchall()
    return [r[0] for r in rows]


def already_done(conn) -> set[str]:
    try:
        rows = conn.execute("SELECT DISTINCT new_cipcode FROM cip_crosswalk").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(DB_PATH)
    setup_table(conn)

    new_codes = get_new_codes(conn)
    done      = already_done(conn)

    print(f"Total new CIP 2020 codes to check: {len(new_codes)}")
    print(f"Already processed: {len(done)}")

    moved_count = 0
    skipped = 0

    for i, cipcode in enumerate(new_codes, 1):
        if cipcode in done:
            skipped += 1
            continue

        action = fetch_action_text(cipcode)
        old_codes = parse_old_codes(action)

        if old_codes:
            for old in old_codes:
                conn.execute(
                    "INSERT OR REPLACE INTO cip_crosswalk VALUES (?, ?, ?)",
                    (cipcode, old, action),
                )
            conn.commit()
            moved_count += 1
            print(f"  [{i}/{len(new_codes)}] {cipcode}: {action[:80]}")
        else:
            # Insert a record with empty old_cipcode so we know it's been checked
            # (use a special sentinel so we don't re-fetch next run)
            conn.execute(
                "INSERT OR REPLACE INTO cip_crosswalk VALUES (?, ?, ?)",
                (cipcode, "__CHECKED__", action or ""),
            )
            conn.commit()

        time.sleep(SLEEP_S)

    conn.close()
    print(f"\nDone. {moved_count} codes had predecessor crosswalks.")
    print(f"Skipped (already done): {skipped}")


if __name__ == "__main__":
    main()
