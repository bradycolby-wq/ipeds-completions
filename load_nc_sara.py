"""Load NC-SARA fall enrollment data.

NC-SARA publishes annual snapshots of out-of-state distance-ed enrollment at
SARA-member institutions, broken down by receiving state. We load Fall 2019–
2024 from the Excel exports the user dropped on disk into a single
``nc_sara_enrollment`` table keyed by (year, opeid, dest_state).

Schema
------
nc_sara_enrollment(
    year INTEGER,            -- Fall year (2019..2024)
    opeid TEXT,              -- 8-digit zero-padded OPEID from NC-SARA
    inst_state TEXT,         -- 2-letter abbr of institution's home state
    member_type TEXT,        -- Public / Private Non-Profit / Private For-Profit / Tribal
    ipeds_level TEXT,        -- '2 Year' / '4 Year'
    dest_state TEXT,         -- 2-letter abbr, or 'NON_SARA' for the residual bucket
    enrollments REAL         -- Fall headcount from receiving state
)
PRIMARY KEY (year, opeid, dest_state)

NC-SARA also has Puerto Rico / US Virgin Islands columns; we keep them in the
table tagged as 'PR' / 'VI' but the distribution engine will skip them when
allocating to US-state CBSAs (the explorer's rankings cover the 50 states + DC).
"""
from __future__ import annotations

import os
import sqlite3
import pandas as pd

DB_PATH = "ipeds.db"

NC_SARA_FILES = {
    2019: r"C:\Users\brady\OneDrive\Desktop\fAll 2019 ncsara.xlsx",
    2020: r"C:\Users\brady\OneDrive\Desktop\Fall 2020 ncsara.xlsx",
    2021: r"C:\Users\brady\OneDrive\Desktop\fALL 2021 NCSARA.xlsx",
    2022: r"C:\Users\brady\OneDrive\Desktop\Fall 2022 ncsara.xlsx",
    2023: r"C:\Users\brady\OneDrive\Desktop\FALL 2023 NCSARA.xlsx",
    2024: r"C:\Users\brady\OneDrive\Desktop\fALL 2024 NCSARA.xlsx",
}

STATE_NAME_TO_ABBR = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT",
    "Delaware": "DE", "District of Columbia": "DC", "Florida": "FL",
    "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY",
    "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
    "Puerto Rico": "PR", "US Virgin Islands": "VI",
}

NON_SARA_TAG = "NON_SARA"

META_COLS = {
    "Institution", "OPEID__c (Account)1", "Institution State1",
    "Member Type1", "2 vs 4 Year", "Grand Total",
}


def _normalize_opeid(raw) -> str | None:
    """NC-SARA stores OPEID as a string sometimes ('03267300') and sometimes
    Excel coerces it to an int. We standardize to 8-character zero-padded
    string to match IPEDS HD."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s or s.lower() == "total":
        return None
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(8)


def _load_one_file(year: int, path: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    df = df[df["Institution"].astype(str).str.strip().str.lower() != "total"]
    df["opeid"] = df["OPEID__c (Account)1"].map(_normalize_opeid)
    df = df[df["opeid"].notna()].copy()

    inst_state_abbr = (
        df["Institution State1"].map(STATE_NAME_TO_ABBR).fillna("")
    )
    df["inst_state"] = inst_state_abbr

    state_cols = [c for c in df.columns if c not in META_COLS
                  and c not in {"opeid", "inst_state"}]

    long = df.melt(
        id_vars=["opeid", "inst_state", "Member Type1", "2 vs 4 Year"],
        value_vars=state_cols,
        var_name="dest_col",
        value_name="enrollments",
    )
    long["enrollments"] = pd.to_numeric(long["enrollments"], errors="coerce")
    long = long[long["enrollments"].fillna(0) > 0]
    long["dest_state"] = long["dest_col"].map(
        lambda c: NON_SARA_TAG if c == "Non-SARA" else STATE_NAME_TO_ABBR.get(c)
    )
    long = long[long["dest_state"].notna()].copy()
    long["year"] = year
    long = long.rename(
        columns={"Member Type1": "member_type", "2 vs 4 Year": "ipeds_level"}
    )
    return long[
        ["year", "opeid", "inst_state", "member_type", "ipeds_level",
         "dest_state", "enrollments"]
    ]


def main(db_path: str = DB_PATH) -> None:
    frames = []
    for year, path in NC_SARA_FILES.items():
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        print(f"Loading NC-SARA Fall {year}…")
        frames.append(_load_one_file(year, path))

    all_df = pd.concat(frames, ignore_index=True)
    # Same OPEID can appear in multiple branch rows; collapse to one row
    # per (year, opeid, dest_state). Metadata (state/type/level) is the
    # first-reported value — they're effectively identical for a given OPEID
    # within a year, and tiny discrepancies don't affect distribution math.
    enr = (
        all_df.groupby(["year", "opeid", "dest_state"], as_index=False)
        ["enrollments"].sum()
    )
    meta = (
        all_df.drop_duplicates(["year", "opeid"])
        [["year", "opeid", "inst_state", "member_type", "ipeds_level"]]
    )
    all_df = enr.merge(meta, on=["year", "opeid"], how="left")
    all_df = all_df[
        ["year", "opeid", "inst_state", "member_type",
         "ipeds_level", "dest_state", "enrollments"]
    ]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS nc_sara_enrollment")
    cur.execute(
        """
        CREATE TABLE nc_sara_enrollment (
            year INTEGER NOT NULL,
            opeid TEXT NOT NULL,
            inst_state TEXT,
            member_type TEXT,
            ipeds_level TEXT,
            dest_state TEXT NOT NULL,
            enrollments REAL NOT NULL,
            PRIMARY KEY (year, opeid, dest_state)
        )
        """
    )
    all_df.to_sql("nc_sara_enrollment", conn, if_exists="append", index=False)
    cur.execute(
        "CREATE INDEX idx_nc_sara_opeid_year ON nc_sara_enrollment(opeid, year)"
    )
    conn.commit()
    n = cur.execute("SELECT COUNT(*) FROM nc_sara_enrollment").fetchone()[0]
    yrs = cur.execute(
        "SELECT MIN(year), MAX(year), COUNT(DISTINCT opeid) FROM nc_sara_enrollment"
    ).fetchone()
    conn.close()
    print(f"Wrote {n:,} rows; year range {yrs[0]}–{yrs[1]}; "
          f"{yrs[2]:,} distinct OPEIDs.")


if __name__ == "__main__":
    main()
