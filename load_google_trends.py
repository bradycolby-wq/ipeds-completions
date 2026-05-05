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

    # ── Liberal Arts / General ────────────────────────────────────────────────
    "24.0101":  "liberal arts degree",
    "24.0102":  "general studies degree",
    "24.0199":  "liberal arts and sciences degree",
    "30.0000":  "interdisciplinary studies degree",
    "30.9999":  "multidisciplinary studies degree",

    # ── Business ──────────────────────────────────────────────────────────────
    "52.0101":  "business degree",
    "52.0201":  "business administration degree",
    "52.0301":  "accounting degree",
    "52.0801":  "finance degree",
    "52.1401":  "marketing degree",
    "52.0201":  "business administration degree",
    "52.1301":  "management science degree",
    "52.1101":  "international business degree",
    "52.0601":  "business analytics degree",
    "52.0901":  "hospitality management degree",
    "52.1001":  "human resources degree",
    "52.0203":  "logistics and supply chain degree",
    "52.1501":  "real estate degree",
    "52.0701":  "entrepreneurship degree",
    "52.0401":  "management information systems degree",
    "52.0301":  "accounting degree",
    "52.1601":  "taxation degree",
    "52.0801":  "finance degree",
    "52.0299":  "business management degree",
    "52.1001":  "human resources degree",

    # ── Computer / IT ─────────────────────────────────────────────────────────
    "11.0701":  "computer science degree",
    "11.0101":  "computer information systems degree",
    "11.0401":  "information technology degree",
    "11.0102":  "artificial intelligence degree",
    "11.0103":  "data science degree",
    "11.0201":  "computer programming degree",
    "11.1003":  "cybersecurity degree",
    "11.0801":  "web development degree",
    "11.0804":  "software engineering degree",
    "11.1001":  "network administration degree",
    "11.0901":  "computer networking degree",
    "11.0501":  "computer systems analysis degree",
    "30.7001":  "data analytics degree",
    "30.0801":  "math and computer science",

    # ── Engineering ───────────────────────────────────────────────────────────
    "14.1901":  "mechanical engineering degree",
    "14.0901":  "computer engineering degree",
    "14.1001":  "electrical engineering degree",
    "14.0801":  "civil engineering degree",
    "14.0101":  "engineering degree",
    "14.0701":  "chemical engineering degree",
    "14.0201":  "aerospace engineering degree",
    "14.0301":  "agricultural engineering degree",
    "14.0501":  "biomedical engineering degree",
    "14.0801":  "civil engineering degree",
    "14.1801":  "materials engineering degree",
    "14.3501":  "industrial engineering degree",
    "14.2501":  "petroleum engineering degree",
    "14.0903":  "software engineering degree",
    "14.4401":  "environmental engineering degree",

    # ── Engineering Technology ────────────────────────────────────────────────
    "15.0000":  "engineering technology degree",
    "15.0303":  "electronics technician program",
    "15.0613":  "manufacturing technology degree",
    "15.0801":  "aeronautics degree",
    "15.1001":  "construction management degree",
    "15.0612":  "industrial technology degree",

    # ── Health / Nursing ──────────────────────────────────────────────────────
    "51.3801":  "nursing degree",
    "51.3901":  "LPN program",
    "51.3805":  "nurse practitioner program",
    "51.3818":  "nursing administration degree",
    "51.0000":  "health sciences degree",
    "51.0001":  "health professions degree",
    "51.0801":  "medical assistant certificate",
    "51.0602":  "dental hygiene degree",
    "51.0912":  "physician assistant program",
    "51.2001":  "pharmacy degree",
    "51.1201":  "medicine degree",
    "51.2201":  "public health degree",
    "51.0707":  "health information management degree",
    "51.0701":  "health administration degree",
    "51.0803":  "occupational therapy program",
    "51.2308":  "physical therapy program",
    "51.0908":  "respiratory therapy program",
    "51.0911":  "radiologic technology program",
    "51.0909":  "surgical technology program",
    "51.0601":  "dental assistant program",
    "51.0901":  "cardiovascular technology program",
    "51.0713":  "medical coding certificate",
    "51.0716":  "medical billing certificate",
    "51.0706":  "health information technology degree",
    "51.0904":  "emergency medical technician program",
    "51.0907":  "medical lab technician program",
    "51.1501":  "substance abuse counseling degree",
    "51.0204":  "speech pathology degree",
    "51.2306":  "occupational therapy assistant program",
    "51.0806":  "physical therapy assistant program",
    "51.3501":  "massage therapy program",
    "51.1005":  "medical laboratory science degree",
    "51.2399":  "rehabilitation counseling degree",
    "51.0910":  "diagnostic medical sonography program",
    "51.0203":  "audiology degree",
    "51.2310":  "athletic training degree",
    "51.3802":  "nursing RN to BSN program",
    "51.0201":  "pharmacy technician certificate",
    "51.1401":  "medical scientist degree",
    "51.2706":  "health and wellness degree",
    "51.0000":  "health sciences degree",
    "51.0710":  "medical office administration certificate",

    # ── Education ─────────────────────────────────────────────────────────────
    "13.0101":  "education degree",
    "13.1202":  "elementary education degree",
    "13.1205":  "secondary education degree",
    "13.1210":  "early childhood education degree",
    "13.1001":  "special education degree",
    "13.0401":  "educational leadership degree",
    "13.0501":  "educational counseling degree",
    "13.0601":  "educational assessment degree",
    "13.0301":  "curriculum and instruction degree",
    "13.1314":  "physical education degree",
    "13.0603":  "educational technology degree",
    "13.1501":  "teaching English as second language degree",
    "13.1312":  "music education degree",
    "13.1311":  "mathematics education degree",
    "13.1316":  "science education degree",
    "13.1305":  "English education degree",
    "13.0404":  "higher education administration degree",
    "13.1209":  "kindergarten teacher certification",

    # ── Psychology ────────────────────────────────────────────────────────────
    "42.0101":  "psychology degree",
    "42.2801":  "clinical psychology degree",
    "42.2803":  "counseling psychology degree",
    "42.2806":  "forensic psychology degree",
    "42.2807":  "industrial organizational psychology degree",
    "42.2812":  "school psychology degree",

    # ── Social Sciences ───────────────────────────────────────────────────────
    "45.1001":  "political science degree",
    "45.1101":  "sociology degree",
    "45.0601":  "economics degree",
    "45.0201":  "anthropology degree",
    "45.0701":  "geography degree",
    "45.0401":  "criminology degree",

    # ── Social Work / Human Services ──────────────────────────────────────────
    "44.0701":  "social work degree",
    "44.0401":  "public administration degree",
    "44.0501":  "public policy degree",
    "44.0000":  "human services degree",
    "51.1503":  "clinical mental health counseling degree",

    # ── Law / Legal ───────────────────────────────────────────────────────────
    "22.0101":  "law school",
    "22.0302":  "paralegal certificate",
    "22.0206":  "legal studies degree",
    "43.0104":  "criminal justice degree",
    "43.0103":  "law enforcement training",
    "43.0107":  "homeland security degree",
    "43.0106":  "forensic science degree",
    "43.0203":  "fire science degree",

    # ── Arts / Humanities ─────────────────────────────────────────────────────
    "50.0409":  "graphic design degree",
    "50.0101":  "visual arts degree",
    "50.0901":  "music degree",
    "50.0501":  "theater degree",
    "50.0601":  "film degree",
    "50.0401":  "design degree",
    "50.0702":  "fine arts degree",
    "50.0706":  "digital arts degree",
    "50.0411":  "game design degree",
    "50.0102":  "digital media degree",
    "23.0101":  "english degree",
    "38.0101":  "philosophy degree",
    "38.0201":  "religion degree",
    "54.0101":  "history degree",
    "16.0901":  "French degree",
    "16.0905":  "Spanish degree",
    "09.0100":  "communications degree",
    "09.0401":  "journalism degree",
    "09.0702":  "public relations degree",
    "09.0902":  "social media marketing degree",

    # ── Sciences ──────────────────────────────────────────────────────────────
    "26.0101":  "biology degree",
    "27.0101":  "mathematics degree",
    "40.0501":  "chemistry degree",
    "40.0801":  "physics degree",
    "40.0601":  "geology degree",
    "26.0502":  "microbiology degree",
    "26.0202":  "biochemistry degree",
    "26.1301":  "ecology degree",
    "26.0801":  "genetics degree",
    "26.0908":  "neuroscience degree",
    "40.0401":  "atmospheric science degree",
    "03.0103":  "environmental science degree",
    "03.0104":  "environmental studies degree",
    "03.0601":  "wildlife management degree",
    "03.0502":  "forestry degree",
    "03.0201":  "natural resources management degree",
    "30.1801":  "sustainability degree",

    # ── Trades / Vocational ───────────────────────────────────────────────────
    "48.0508":  "welding certification",
    "12.0401":  "cosmetology license",
    "46.0302":  "electrician training",
    "46.0503":  "plumbing training",
    "47.0201":  "HVAC certification",
    "47.0604":  "auto mechanic training",
    "46.0101":  "construction trades program",
    "48.0501":  "machinist training",
    "46.0201":  "carpentry training",
    "49.0205":  "CDL truck driving school",
    "12.0503":  "culinary arts program",
    "12.0504":  "pastry chef program",
    "12.0500":  "cooking and culinary arts school",
    "47.0303":  "industrial maintenance training",
    "47.0110":  "diesel mechanic training",
    "15.0501":  "HVAC technology program",
    "46.0000":  "construction management certificate",
    "48.0510":  "CNC machining program",
    "12.0402":  "barbering license",
    "12.0406":  "esthetician training",
    "12.0410":  "nail technician training",

    # ── Agriculture ───────────────────────────────────────────────────────────
    "01.0000":  "agriculture degree",
    "01.0101":  "agricultural business degree",
    "01.0901":  "animal science degree",
    "01.1101":  "plant science degree",
    "01.0605":  "landscape architecture degree",
    "01.0701":  "agriculture science degree",
    "01.0801":  "agricultural economics degree",
    "01.8101":  "veterinary technician program",

    # ── Architecture / Planning ───────────────────────────────────────────────
    "04.0201":  "architecture degree",
    "04.0301":  "urban planning degree",
    "04.0501":  "interior design degree",

    # ── Parks / Recreation / Fitness ──────────────────────────────────────────
    "31.0505":  "kinesiology degree",
    "31.0504":  "sport management degree",
    "31.0501":  "health and physical education degree",
    "31.0507":  "personal trainer certification",
    "31.0101":  "recreation management degree",

    # ── Theology / Ministry ───────────────────────────────────────────────────
    "39.0601":  "theology degree",
    "39.0701":  "pastoral ministry degree",
    "39.0201":  "divinity degree",

    # ── Military / Homeland Security ──────────────────────────────────────────
    "29.0101":  "military science degree",
    "43.0302":  "intelligence studies degree",

    # ── Library Science ───────────────────────────────────────────────────────
    "25.0101":  "library science degree",

    # ── Family / Consumer Sciences ────────────────────────────────────────────
    "19.0707":  "family counseling degree",
    "19.0101":  "family and consumer sciences degree",
    "19.0706":  "child development degree",
}


# ── Smarter suffix logic for auto-generated search terms ─────────────────────
# CIP 2-digit families that are typically trade/vocational programs
_TRADE_FAMILIES = {"12", "46", "47", "48", "49"}
# CIP 2-digit families where "program" is a better suffix than "degree"
_PROGRAM_FAMILIES = {"51"}  # health — many are certificates/programs
# Specific 4-digit CIP prefixes that are certificate-oriented
_CERT_PREFIXES = {
    "51.08", "51.06", "51.09", "51.07", "51.02",  # health tech / assisting
    "51.39",  # LPN/LVN
    "22.03",  # paralegal
    "15.03", "15.04",  # engineering tech
    "10.02", "10.03",  # communications tech
}
# Keywords in CIP titles that signal non-degree programs
_CERT_TITLE_KEYWORDS = {
    "technician", "technologist", "assistant", "aide", "helper",
    "operator", "mechanic", "repairer", "installer",
}
# Title words to strip because they add noise to search terms
_NOISE_WORDS = {"general", "other", "all other", "not elsewhere classified"}


# ── CIP-to-search-term mapping ───────────────────────────────────────────────

def cip_to_search_term(cipcode: str, ciptitle: str) -> str | None:
    """Map a CIP code to a Google Trends search term.

    Uses context-aware suffix selection:
      - Trades (CIP 12/46/47/48/49): "training" or "certification"
      - Health tech / assisting roles: "program" or "certificate"
      - Title contains technician/assistant/aide: "program"
      - Everything else: "degree"

    Returns None for CIP codes that should be skipped.
    """
    if cipcode in CIP_SEARCH_OVERRIDES:
        return CIP_SEARCH_OVERRIDES[cipcode]
    if not ciptitle or ciptitle == cipcode:
        return None

    # Clean the title: take first phrase before comma, strip parentheticals
    clean = ciptitle.split(",")[0].strip()
    clean = re.sub(r'\s*\([^)]*\)', '', clean).strip()
    # Take first option before slash if the result would still be 3+ chars
    # e.g. "Botany/Plant Biology" -> "Botany", "Pharmacy Technician/Assistant" -> "Pharmacy Technician"
    if "/" in clean:
        first_part = clean.split("/")[0].strip()
        if len(first_part) >= 3:
            clean = first_part
    # Remove noise words like "General" or "Other"
    for noise in _NOISE_WORDS:
        clean = re.sub(rf'\b{noise}\b', '', clean, flags=re.IGNORECASE).strip()
    # Strip trailing "Technologies" / "Technology" — people don't search that way
    clean = re.sub(r'\s+Technolog(y|ies)$', '', clean, flags=re.IGNORECASE).strip()
    # Strip trailing "Services"
    clean = re.sub(r'\s+Services$', '', clean, flags=re.IGNORECASE).strip()
    # Collapse whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()

    if len(clean) < 3:
        return None

    # Cap length — overly long search terms get zero volume on Google Trends
    words = clean.split()
    if len(words) > 5:
        clean = " ".join(words[:5])

    clean_lower = clean.lower()
    family = cipcode[:2]       # e.g. "51"
    prefix = cipcode[:5]       # e.g. "51.08"

    # Pick the right suffix based on program type
    if family in _TRADE_FAMILIES:
        suffix = "training"
    elif prefix in _CERT_PREFIXES:
        suffix = "program"
    elif any(kw in clean_lower for kw in _CERT_TITLE_KEYWORDS):
        suffix = "program"
    elif family in _PROGRAM_FAMILIES:
        # Health family — default to "degree" for broader programs,
        # but many sub-fields already caught by _CERT_PREFIXES above
        suffix = "degree"
    else:
        suffix = "degree"

    # Avoid redundancy: don't append "degree" if title already ends with it
    if clean_lower.endswith((" degree", " program", " training",
                              " certificate", " certification")):
        return clean_lower

    return f"{clean_lower} {suffix}"


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
