"""
Load College Scorecard Field-of-Study data into ipeds.db.

Source: U.S. Department of Education College Scorecard
File:   Most-Recent-Cohorts-Field-of-Study.csv

Creates table `college_scorecard` with median earnings (4yr post-grad),
median debt, and pre-computed debt-to-earnings ratio by
institution × CIP (4-digit) × credential level.
"""

import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent / "ipeds.db"

CSV_PATH = Path(
    r"C:\Users\brady\OneDrive\Desktop\scorecard_temp"
    r"\Most-Recent-Cohorts-Field-of-Study.csv"
)

# Only read the columns we need (out of 174)
KEEP_COLS = [
    "UNITID", "INSTNM", "CIPCODE", "CIPDESC",
    "CREDLEV", "CREDDESC", "CONTROL", "DISTANCE",
    "EARN_MDN_4YR", "DEBT_ALL_STGP_EVAL_MDN",
]

# Scorecard CREDLEV → IPEDS awlevel(s)
# One-to-many where scorecard doesn't distinguish sub-types
CREDLEV_TO_AWLEVELS = {
    1: [1, 2],         # Undergraduate Certificate → <1yr + 1-2yr cert
    2: [3],            # Associate's Degree
    3: [5],            # Bachelor's Degree
    4: [6],            # Post-baccalaureate Certificate
    5: [7],            # Master's Degree
    6: [17, 18, 19],   # Doctoral Degree (research / professional / other)
    7: [20],           # First Professional Degree
    8: [8],            # Graduate/Professional Certificate
}

TABLE_DDL = """
DROP TABLE IF EXISTS college_scorecard;
CREATE TABLE college_scorecard (
    unitid              INTEGER NOT NULL,
    instnm              TEXT,
    cipcode             TEXT    NOT NULL,   -- XX.XX format (4-digit)
    cipdesc             TEXT,
    credlev             INTEGER NOT NULL,   -- original scorecard CREDLEV
    creddesc            TEXT,
    awlevel             INTEGER NOT NULL,   -- mapped IPEDS awlevel
    control             TEXT,
    distance            INTEGER,
    earn_mdn_4yr        REAL,
    debt_all_stgp_eval_mdn REAL,
    debt_to_earnings    REAL
);
"""

INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_sc_unitid  ON college_scorecard(unitid);
CREATE INDEX IF NOT EXISTS idx_sc_cipcode ON college_scorecard(cipcode);
CREATE INDEX IF NOT EXISTS idx_sc_awlevel ON college_scorecard(awlevel);
"""


def format_cipcode(val) -> str | None:
    """Convert integer CIP (e.g. 5201) to XX.XX string (e.g. '52.01')."""
    if pd.isna(val):
        return None
    code = int(float(val))
    major = code // 100
    minor = code % 100
    return f"{major:02d}.{minor:02d}"


def parse_numeric(val) -> float | None:
    """Convert scorecard value to float.  'PS' / 'PrivacySuppressed' → None."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s in ("PS", "PrivacySuppressed", "NULL", ""):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def main():
    print(f"Reading {CSV_PATH.name} ...")
    df = pd.read_csv(CSV_PATH, usecols=KEEP_COLS, dtype=str, low_memory=False)
    print(f"  {len(df):,} rows, {len(df.columns)} columns")

    # ── Convert fields ────────────────────────────────────────────────────
    df["cipcode"] = df["CIPCODE"].apply(format_cipcode)
    df = df.dropna(subset=["cipcode", "UNITID"])

    df["earn_mdn_4yr"] = df["EARN_MDN_4YR"].apply(parse_numeric)
    df["debt_all_stgp_eval_mdn"] = df["DEBT_ALL_STGP_EVAL_MDN"].apply(parse_numeric)

    # Debt-to-earnings ratio
    def _dte(row):
        e = row["earn_mdn_4yr"]
        d = row["debt_all_stgp_eval_mdn"]
        if e and e > 0 and d is not None:
            return round(d / e, 3)
        return None

    df["debt_to_earnings"] = df.apply(_dte, axis=1)

    df["CREDLEV"] = pd.to_numeric(df["CREDLEV"], errors="coerce")
    df["DISTANCE"] = pd.to_numeric(df["DISTANCE"], errors="coerce")

    # ── Expand CREDLEV → awlevel (one-to-many) ───────────────────────────
    print("Expanding CREDLEV -> awlevel ...")
    rows = []
    for _, r in df.iterrows():
        cl = r["CREDLEV"]
        if pd.isna(cl) or int(cl) not in CREDLEV_TO_AWLEVELS:
            continue
        for aw in CREDLEV_TO_AWLEVELS[int(cl)]:
            rows.append({
                "unitid": int(r["UNITID"]),
                "instnm": r["INSTNM"],
                "cipcode": r["cipcode"],
                "cipdesc": r["CIPDESC"],
                "credlev": int(cl),
                "creddesc": r["CREDDESC"],
                "awlevel": aw,
                "control": r["CONTROL"],
                "distance": int(r["DISTANCE"]) if pd.notna(r["DISTANCE"]) else None,
                "earn_mdn_4yr": r["earn_mdn_4yr"],
                "debt_all_stgp_eval_mdn": r["debt_all_stgp_eval_mdn"],
                "debt_to_earnings": r["debt_to_earnings"],
            })

    result = pd.DataFrame(rows)
    print(f"  {len(result):,} rows after expansion")

    # ── Write to SQLite ───────────────────────────────────────────────────
    print(f"Writing to {DB_PATH} ...")
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(TABLE_DDL)
    result.to_sql("college_scorecard", conn, if_exists="append", index=False)
    conn.executescript(INDEX_DDL)
    conn.commit()

    # ── Summary ───────────────────────────────────────────────────────────
    total = len(result)
    with_earn = result["earn_mdn_4yr"].notna().sum()
    with_debt = result["debt_all_stgp_eval_mdn"].notna().sum()
    with_both = (
        result["earn_mdn_4yr"].notna() & result["debt_all_stgp_eval_mdn"].notna()
    ).sum()
    print(f"\nOK: Loaded {total:,} rows into college_scorecard")
    print(f"  With 4yr earnings : {with_earn:,}")
    print(f"  With median debt  : {with_debt:,}")
    print(f"  With both (D/E)   : {with_both:,}")
    print(f"  Unique institutions: {result['unitid'].nunique():,}")
    print(f"  Unique CIP codes   : {result['cipcode'].nunique():,}")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
