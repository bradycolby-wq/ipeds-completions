#!/usr/bin/env python3
"""
IPEDS Data Setup
================
Downloads Completions (C_A) and Institutional Characteristics (HD)
for academic years 2014-2023, then loads everything into a SQLite database
at ipeds.db for easy querying.

Files downloaded from: https://nces.ed.gov/ipeds/datacenter/data/
"""

import csv
import os
import sqlite3
import sys
import urllib.request
import zipfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path(__file__).parent
ZIPS_DIR   = OUTPUT_DIR / "zips"
RAW_DIR    = OUTPUT_DIR / "raw"
DB_PATH    = OUTPUT_DIR / "ipeds.db"
BASE_URL   = "https://nces.ed.gov/ipeds/datacenter/data/"
YEARS      = list(range(2014, 2024))   # 2014-15 through 2023-24

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------
AWARD_LEVELS = {
    1:  "Less than 1-year certificate",
    2:  "1-2 year certificate",
    3:  "Associate's degree",
    4:  "2-4 year certificate",
    5:  "Bachelor's degree",
    6:  "Post-baccalaureate certificate",
    7:  "Master's degree",
    8:  "Post-master's certificate",
    17: "Doctor's degree - Research/Scholarship",
    18: "Doctor's degree - Professional Practice",
    19: "Doctor's degree - Other",
}

SECTORS = {
    0:  "Administrative Unit",
    1:  "Public, 4-year or above",
    2:  "Private nonprofit, 4-year or above",
    3:  "Private for-profit, 4-year or above",
    4:  "Public, 2-year",
    5:  "Private nonprofit, 2-year",
    6:  "Private for-profit, 2-year",
    7:  "Public, less than 2-year",
    8:  "Private nonprofit, less than 2-year",
    9:  "Private for-profit, less than 2-year",
    99: "Sector unknown",
}

CONTROL = {
    1: "Public",
    2: "Private nonprofit",
    3: "Private for-profit",
}

ICLEVEL = {
    1: "Four or more years",
    2: "At least 2 but less than 4 years",
    3: "Less than 2 years",
}

LOCALE = {
    11: "City: Large",
    12: "City: Midsize",
    13: "City: Small",
    21: "Suburb: Large",
    22: "Suburb: Midsize",
    23: "Suburb: Small",
    31: "Town: Fringe",
    32: "Town: Distant",
    33: "Town: Remote",
    41: "Rural: Fringe",
    42: "Rural: Distant",
    43: "Rural: Remote",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log(msg):
    print(msg, flush=True)


def safe_int(val, default=None):
    try:
        return int(float(val)) if val and str(val).strip() not in ("", ".", "NA") else default
    except (ValueError, TypeError):
        return default


def safe_float(val, default=None):
    try:
        return float(val) if val and str(val).strip() not in ("", ".", "NA") else default
    except (ValueError, TypeError):
        return default


def download_file(url, dest: Path, label=""):
    if dest.exists():
        log(f"  [skip] {dest.name} already downloaded")
        return True
    log(f"  [get]  {label or dest.name}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (IPEDS-downloader)"})
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read()
        dest.write_bytes(data)
        log(f"  [ok]   {dest.name}  ({len(data)/1048576:.1f} MB)")
        return True
    except Exception as e:
        log(f"  [err]  {label}: {e}")
        return False


def extract_csv(zip_path: Path, prefix: str) -> Path | None:
    """Extract the main data CSV from a zip (skip _RV revised and _Dict files)."""
    with zipfile.ZipFile(zip_path) as z:
        # Prefer non-revised data file
        candidates = [n for n in z.namelist()
                      if n.lower().endswith(".csv")
                      and "_rv" not in n.lower()
                      and "dict" not in n.lower()]
        if not candidates:
            candidates = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not candidates:
            log(f"  [warn] no CSV found in {zip_path.name}")
            return None
        name = candidates[0]
        dest = RAW_DIR / name
        if not dest.exists():
            z.extract(name, RAW_DIR)
        return RAW_DIR / name


def read_csv(path: Path):
    """Return (fieldnames_upper, list-of-dicts) with uppercase keys.
    Handles UTF-8 BOM present in some IPEDS years (e.g. 2023+).
    """
    for enc in ("utf-8-sig", "latin-1", "utf-8", "cp1252"):
        try:
            with open(path, "r", encoding=enc, errors="replace") as f:
                reader = csv.DictReader(f)
                # Strip BOM remnants and whitespace from header names
                fieldnames = [
                    h.encode("ascii", "ignore").decode().upper().strip()
                    for h in (reader.fieldnames or [])
                ]
                rows = []
                for row in reader:
                    rows.append({
                        k.encode("ascii", "ignore").decode().upper().strip(): v.strip()
                        for k, v in row.items()
                    })
            return fieldnames, rows
        except Exception:
            continue
    log(f"  [warn] could not read {path}")
    return [], []


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS completions (
    year      INTEGER NOT NULL,
    unitid    INTEGER NOT NULL,
    cipcode   TEXT    NOT NULL,
    majornum  INTEGER NOT NULL DEFAULT 1,
    awlevel   INTEGER NOT NULL,
    ctotalt   INTEGER,
    PRIMARY KEY (year, unitid, cipcode, majornum, awlevel)
);

CREATE TABLE IF NOT EXISTS institutions (
    year      INTEGER NOT NULL,
    unitid    INTEGER NOT NULL,
    instnm    TEXT,
    city      TEXT,
    stabbr    TEXT,
    fips      INTEGER,
    obereg    INTEGER,
    sector    INTEGER,
    iclevel   INTEGER,
    control   INTEGER,
    hloffer   INTEGER,
    ugoffer   INTEGER,
    groffer   INTEGER,
    hdegofr1  INTEGER,
    deggrant  INTEGER,
    carnegie  INTEGER,
    instsize  INTEGER,
    locale    INTEGER,
    latitude  REAL,
    longitud  REAL,
    countycd  TEXT,
    countynm  TEXT,
    cbsa      TEXT,
    cbsanm    TEXT,
    opeflag   INTEGER,
    closeind  INTEGER,
    PRIMARY KEY (year, unitid)
);

CREATE TABLE IF NOT EXISTS award_levels (
    awlevel     INTEGER PRIMARY KEY,
    description TEXT
);

CREATE TABLE IF NOT EXISTS sectors (
    sector      INTEGER PRIMARY KEY,
    description TEXT
);

CREATE TABLE IF NOT EXISTS controls (
    control     INTEGER PRIMARY KEY,
    description TEXT
);

CREATE TABLE IF NOT EXISTS iclevels (
    iclevel     INTEGER PRIMARY KEY,
    description TEXT
);

CREATE TABLE IF NOT EXISTS locales (
    locale      INTEGER PRIMARY KEY,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_comp_cipcode  ON completions(cipcode);
CREATE INDEX IF NOT EXISTS idx_comp_awlevel  ON completions(awlevel);
CREATE INDEX IF NOT EXISTS idx_comp_year     ON completions(year);
CREATE INDEX IF NOT EXISTS idx_comp_unitid   ON completions(unitid);
CREATE INDEX IF NOT EXISTS idx_inst_stabbr   ON institutions(stabbr);
CREATE INDEX IF NOT EXISTS idx_inst_sector   ON institutions(sector);
CREATE INDEX IF NOT EXISTS idx_inst_control  ON institutions(control);
CREATE INDEX IF NOT EXISTS idx_inst_iclevel  ON institutions(iclevel);
CREATE INDEX IF NOT EXISTS idx_inst_year     ON institutions(year);
CREATE INDEX IF NOT EXISTS idx_inst_locale   ON institutions(locale);
"""

VIEW_DDL = """
DROP VIEW IF EXISTS completions_view;
CREATE VIEW completions_view AS
SELECT
    c.year,
    c.unitid,
    i.instnm,
    i.city,
    i.stabbr,
    i.fips,
    i.obereg,
    i.sector,
    s.description  AS sector_name,
    i.iclevel,
    il.description AS iclevel_name,
    i.control,
    ct.description AS control_name,
    i.carnegie,
    i.instsize,
    i.locale,
    lo.description AS locale_name,
    i.latitude,
    i.longitud,
    i.countycd,
    i.countynm,
    i.cbsa,
    i.cbsanm,
    i.closeind,
    c.cipcode,
    c.majornum,
    c.awlevel,
    al.description AS award_level_name,
    c.ctotalt
FROM completions c
LEFT JOIN institutions i  ON c.unitid  = i.unitid  AND c.year = i.year
LEFT JOIN award_levels al ON c.awlevel = al.awlevel
LEFT JOIN sectors      s  ON i.sector  = s.sector
LEFT JOIN controls     ct ON i.control = ct.control
LEFT JOIN iclevels     il ON i.iclevel = il.iclevel
LEFT JOIN locales      lo ON i.locale  = lo.locale;
"""


def setup_db(conn):
    conn.executescript(DDL)
    for k, v in AWARD_LEVELS.items():
        conn.execute("INSERT OR REPLACE INTO award_levels VALUES (?,?)", (k, v))
    for k, v in SECTORS.items():
        conn.execute("INSERT OR REPLACE INTO sectors VALUES (?,?)", (k, v))
    for k, v in CONTROL.items():
        conn.execute("INSERT OR REPLACE INTO controls VALUES (?,?)", (k, v))
    for k, v in ICLEVEL.items():
        conn.execute("INSERT OR REPLACE INTO iclevels VALUES (?,?)", (k, v))
    for k, v in LOCALE.items():
        conn.execute("INSERT OR REPLACE INTO locales VALUES (?,?)", (k, v))
    conn.commit()
    log("  Schema ready.")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def load_completions(conn, year: int, csv_path: Path):
    # Check if already loaded
    existing = conn.execute("SELECT COUNT(*) FROM completions WHERE year=?", (year,)).fetchone()[0]
    if existing > 0:
        log(f"  [skip] completions {year} already in DB ({existing:,} rows)")
        return existing

    _, rows = read_csv(csv_path)
    if not rows:
        log(f"  [warn] no rows in {csv_path.name}")
        return 0

    batch = []
    for row in rows:
        unitid   = safe_int(row.get("UNITID"))
        cipcode  = row.get("CIPCODE", "").strip()
        majornum = safe_int(row.get("MAJORNUM"), 1)
        awlevel  = safe_int(row.get("AWLEVEL"))
        ctotalt  = safe_int(row.get("CTOTALT"))
        if unitid and cipcode and awlevel is not None:
            batch.append((year, unitid, cipcode, majornum, awlevel, ctotalt))

    conn.executemany(
        "INSERT OR REPLACE INTO completions VALUES (?,?,?,?,?,?)", batch
    )
    conn.commit()
    log(f"  -> {len(batch):,} completions rows loaded for {year}")
    return len(batch)


def load_institutions(conn, year: int, csv_path: Path):
    existing = conn.execute("SELECT COUNT(*) FROM institutions WHERE year=?", (year,)).fetchone()[0]
    if existing > 0:
        log(f"  [skip] institutions {year} already in DB ({existing:,} rows)")
        return existing

    _, rows = read_csv(csv_path)
    if not rows:
        log(f"  [warn] no rows in {csv_path.name}")
        return 0

    batch = []
    for row in rows:
        unitid = safe_int(row.get("UNITID"))
        if not unitid:
            continue
        batch.append((
            year,
            unitid,
            row.get("INSTNM", "").strip(),
            row.get("CITY",   "").strip(),
            row.get("STABBR", "").strip(),
            safe_int(row.get("FIPS")),
            safe_int(row.get("OBEREG")),
            safe_int(row.get("SECTOR")),
            safe_int(row.get("ICLEVEL")),
            safe_int(row.get("CONTROL")),
            safe_int(row.get("HLOFFER")),
            safe_int(row.get("UGOFFER")),
            safe_int(row.get("GROFFER")),
            safe_int(row.get("HDEGOFR1")),
            safe_int(row.get("DEGGRANT")),
            safe_int(row.get("CARNEGIE")),
            safe_int(row.get("INSTSIZE")),
            safe_int(row.get("LOCALE")),
            safe_float(row.get("LATITUDE")),
            safe_float(row.get("LONGITUD")),
            row.get("COUNTYCD", "").strip(),
            row.get("COUNTYNM", "").strip(),
            row.get("CBSA",     "").strip(),
            row.get("CBSANM",   "").strip(),
            safe_int(row.get("OPEFLAG")),
            safe_int(row.get("CLOSE")),
        ))

    conn.executemany(
        """INSERT OR REPLACE INTO institutions VALUES
           (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        batch
    )
    conn.commit()
    log(f"  -> {len(batch):,} institution rows loaded for {year}")
    return len(batch)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log("=" * 60)
    log("IPEDS Data Setup")
    log("=" * 60)

    ZIPS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64 MB cache

    log("\n[1] Setting up database schema...")
    setup_db(conn)

    for year in YEARS:
        log(f"\n[{year}-{year+1}] -----------------------------------------------")

        # --- Completions ---
        comp_zip = ZIPS_DIR / f"C{year}_A.zip"
        download_file(BASE_URL + f"C{year}_A.zip", comp_zip, f"Completions {year}")
        if comp_zip.exists():
            csv_path = extract_csv(comp_zip, f"c{year}_a")
            if csv_path:
                load_completions(conn, year, csv_path)

        # --- Institutional Characteristics (HD = Header/Directory) ---
        hd_zip = ZIPS_DIR / f"HD{year}.zip"
        download_file(BASE_URL + f"HD{year}.zip", hd_zip, f"Inst. Characteristics {year}")
        if hd_zip.exists():
            csv_path = extract_csv(hd_zip, f"hd{year}")
            if csv_path:
                load_institutions(conn, year, csv_path)

    log("\n[final] Creating completions_view...")
    conn.executescript(VIEW_DDL)
    conn.commit()

    # Summary
    n_comp = conn.execute("SELECT COUNT(*) FROM completions").fetchone()[0]
    n_inst = conn.execute("SELECT COUNT(*) FROM institutions").fetchone()[0]
    years_loaded = conn.execute(
        "SELECT MIN(year), MAX(year), COUNT(DISTINCT year) FROM completions"
    ).fetchone()
    conn.close()

    log("\n" + "=" * 60)
    log("DONE")
    log(f"  Database : {DB_PATH}")
    log(f"  Completions rows : {n_comp:,}")
    log(f"  Institution rows : {n_inst:,}")
    log(f"  Years loaded     : {years_loaded[0]}-{years_loaded[1]} ({years_loaded[2]} years)")
    log("=" * 60)
    log("\nRun query_ipeds.py to search the data.")


if __name__ == "__main__":
    main()
