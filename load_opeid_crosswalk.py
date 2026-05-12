"""Build a unitid <-> OPEID crosswalk from IPEDS HD files.

NC-SARA reports by OPEID; the rest of the explorer is keyed by IPEDS UnitID.
This script reads every HD20YY.csv in raw/ and writes a deduped lookup table.

Schema
------
unitid_opeid_crosswalk(
    unitid INTEGER,
    opeid TEXT,            -- 8-character zero-padded
    last_year INTEGER      -- most recent year this pairing was reported
)
PRIMARY KEY (unitid, opeid)

A handful of unitids change OPEID over time (mergers, branch reassignments)
so we store every distinct pairing and record the latest year it was active.
"""
from __future__ import annotations

import glob
import os
import sqlite3
import pandas as pd

DB_PATH = "ipeds.db"


def _hd_files(raw_dir: str = "raw") -> list[str]:
    paths = glob.glob(os.path.join(raw_dir, "HD*.csv")) + \
            glob.glob(os.path.join(raw_dir, "hd*.csv"))
    return sorted({os.path.normcase(p) for p in paths})


def _year_from_path(path: str) -> int | None:
    base = os.path.basename(path).lower()
    digits = "".join(c for c in base if c.isdigit())
    if len(digits) == 4:
        return int(digits)
    return None


def _normalize_opeid(raw) -> str | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s or s == "-2" or s == "-1":
        return None
    return s.zfill(8)


def main(db_path: str = DB_PATH) -> None:
    rows = []
    for path in _hd_files():
        year = _year_from_path(path)
        if year is None:
            continue
        df = pd.read_csv(path, encoding="latin-1", dtype=str,
                         usecols=lambda c: c.upper() in {"UNITID", "OPEID"})
        df.columns = [c.upper() for c in df.columns]
        if "UNITID" not in df.columns or "OPEID" not in df.columns:
            continue
        df["unitid"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
        df["opeid"] = df["OPEID"].map(_normalize_opeid)
        df = df[df["unitid"].notna() & df["opeid"].notna()]
        df["year"] = year
        rows.append(df[["unitid", "opeid", "year"]])

    all_df = pd.concat(rows, ignore_index=True)
    # Reduce to (unitid, opeid) with the latest year they co-occurred.
    crosswalk = (
        all_df.groupby(["unitid", "opeid"], as_index=False)["year"].max()
        .rename(columns={"year": "last_year"})
    )

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS unitid_opeid_crosswalk")
    cur.execute(
        """
        CREATE TABLE unitid_opeid_crosswalk (
            unitid INTEGER NOT NULL,
            opeid TEXT NOT NULL,
            last_year INTEGER NOT NULL,
            PRIMARY KEY (unitid, opeid)
        )
        """
    )
    crosswalk.to_sql(
        "unitid_opeid_crosswalk", conn, if_exists="append", index=False
    )
    cur.execute(
        "CREATE INDEX idx_opeid_xwalk_opeid ON unitid_opeid_crosswalk(opeid)"
    )
    conn.commit()
    n = cur.execute("SELECT COUNT(*) FROM unitid_opeid_crosswalk").fetchone()[0]
    u = cur.execute(
        "SELECT COUNT(DISTINCT unitid) FROM unitid_opeid_crosswalk"
    ).fetchone()[0]
    o = cur.execute(
        "SELECT COUNT(DISTINCT opeid) FROM unitid_opeid_crosswalk"
    ).fetchone()[0]
    conn.close()
    print(f"Wrote {n:,} (unitid, opeid) pairings — "
          f"{u:,} distinct unitids, {o:,} distinct OPEIDs.")


if __name__ == "__main__":
    main()
