"""
Load IPEDS Completions DEP (Distance Education Programs) data into ipeds.db.

Source: NCES IPEDS C{YEAR}DEP.zip files (2013-2023)
Creates table `completions_dep` with program counts by CIP, award level,
and distance education availability.

Each row = one (year, unitid, cipcode, awlevel) with:
  - programs: total programs offered
  - programs_de: programs completable entirely via distance education
  - programs_de_some: programs with some (but not all) DE component
                      (only available 2020+, NULL for earlier years)
  - programs_de_any: programs with ANY DE (all + some).
        For 2013-2019: same as programs_de (the DE column was yes/no).
        For 2020+: programs_de + programs_de_some.
"""

import sqlite3
import zipfile
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent / "ipeds.db"
DATA_DIR = Path(r"C:\Users\brady\OneDrive\Desktop\ipeds_temp")

# IPEDS CDEP column prefix -> awlevel code (matching completions table)
LEVEL_MAP = {
    "CERT1a": 1,     # Certificate < 1 year
    "CERT1b": 2,     # Certificate 1-2 years
    "ASSOC":  3,     # Associate's
    "CERT2":  4,     # Certificate 2-4 years
    "BACHL":  5,     # Bachelor's
    "PBACC":  6,     # Post-baccalaureate certificate
    "MASTR":  7,     # Master's
    "CERT4":  8,     # Post-master's certificate
    "DOCRS": 17,     # Doctoral - Research/Scholarship
    "DOCPP": 18,     # Doctoral - Professional Practice
    "DOCOT": 19,     # Doctoral - Other
    "PMAST": 20,     # Post-master's (older code)
}

TABLE_DDL = """
DROP TABLE IF EXISTS completions_dep;
CREATE TABLE completions_dep (
    year             INTEGER NOT NULL,
    unitid           INTEGER NOT NULL,
    cipcode          TEXT    NOT NULL,
    awlevel          INTEGER NOT NULL,
    programs         INTEGER,
    programs_de      INTEGER,
    programs_de_some INTEGER,
    programs_de_any  INTEGER,
    PRIMARY KEY (year, unitid, cipcode, awlevel)
);
"""

INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_cdep_cip ON completions_dep(cipcode);
CREATE INDEX IF NOT EXISTS idx_cdep_yr  ON completions_dep(year);
CREATE INDEX IF NOT EXISTS idx_cdep_aw  ON completions_dep(awlevel);
"""


def load_year(file_year: int) -> pd.DataFrame:
    """Load one year of CDEP data using vectorized pandas, return long-format."""
    zip_path = DATA_DIR / f"C{file_year}DEP.zip"
    if not zip_path.exists():
        print(f"  [SKIP] {zip_path.name} not found")
        return pd.DataFrame()

    with zipfile.ZipFile(zip_path) as zf:
        csvs = [f for f in zf.namelist() if f.endswith(".csv")]
        csv_name = next(
            (f for f in csvs if "_RV" not in f.upper()),
            csvs[0] if csvs else None,
        )
        if csv_name is None:
            return pd.DataFrame()
        with zf.open(csv_name) as fh:
            df = pd.read_csv(fh, dtype=str)

    df.columns = [c.strip() for c in df.columns]

    # Melt from wide to long: one row per (unitid, cipcode, awlevel)
    frames = []
    for prefix, awlevel in LEVEL_MAP.items():
        col_total = f"P{prefix}"
        col_de = f"P{prefix}DE"
        col_des = f"P{prefix}DES"

        if col_total not in df.columns:
            continue

        sub = df[["UNITID", "CIPCODE"]].copy()
        sub["awlevel"] = awlevel
        sub["programs"] = pd.to_numeric(df[col_total], errors="coerce")
        sub["programs_de"] = pd.to_numeric(
            df[col_de], errors="coerce"
        ) if col_de in df.columns else float("nan")
        sub["programs_de_some"] = pd.to_numeric(
            df[col_des], errors="coerce"
        ) if col_des in df.columns else float("nan")

        # Drop rows with 0 or NaN total programs
        sub = sub[sub["programs"].notna() & (sub["programs"] > 0)]
        if not sub.empty:
            frames.append(sub)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["year"] = file_year
    result.rename(columns={"UNITID": "unitid", "CIPCODE": "cipcode"}, inplace=True)
    result["unitid"] = result["unitid"].astype(int)

    # Compute programs_de_any:
    # For 2020+ (has DES column): all DE + some DE
    # For 2013-2019 (no DES column): same as programs_de
    #   (the old question was binary yes/no, so DE meant "any DE")
    de_all = result["programs_de"].fillna(0)
    de_some = result["programs_de_some"].fillna(0)
    result["programs_de_any"] = (de_all + de_some).astype(int)

    # Convert to nullable ints for DB
    for col in ["programs", "programs_de", "programs_de_any"]:
        result[col] = result[col].astype("Int64")
    # programs_de_some stays NaN for pre-2020 years
    result["programs_de_some"] = result["programs_de_some"].astype("Int64")

    return result[[
        "year", "unitid", "cipcode", "awlevel",
        "programs", "programs_de", "programs_de_some", "programs_de_any",
    ]]


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(TABLE_DDL)

    total_rows = 0
    for file_year in range(2013, 2024):
        print(f"Loading C{file_year}DEP ...", end=" ", flush=True)
        df = load_year(file_year)
        if df.empty:
            print("empty")
            continue
        df.to_sql("completions_dep", conn, if_exists="append", index=False)
        total_rows += len(df)
        print(f"{len(df):,} rows")

    conn.executescript(INDEX_DDL)
    conn.commit()

    # Summary
    print(f"\nTotal: {total_rows:,} rows loaded into completions_dep")
    cur = conn.execute("SELECT COUNT(DISTINCT year) FROM completions_dep")
    print(f"Years: {cur.fetchone()[0]}")
    cur = conn.execute("SELECT COUNT(DISTINCT unitid) FROM completions_dep")
    print(f"Institutions: {cur.fetchone()[0]:,}")
    cur = conn.execute("SELECT COUNT(DISTINCT cipcode) FROM completions_dep")
    print(f"CIP codes: {cur.fetchone()[0]:,}")

    # DE trend (using programs_de_any for consistent comparison)
    print("\nDE programs offered over time (any DE = all + some):")
    cur = conn.execute("""
        SELECT year,
               SUM(programs) as total_programs,
               SUM(programs_de_any) as de_any_programs,
               ROUND(100.0 * SUM(programs_de_any) / SUM(programs), 1) as pct_de
        FROM completions_dep
        GROUP BY year ORDER BY year
    """)
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,} total, {r[2]:,} DE-any ({r[3]}%)")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
