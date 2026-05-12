# -*- coding: utf-8 -*-
"""Verify ipeds_render.db (the slimmed Render copy) still powers rankings."""
from __future__ import annotations

import shutil
import sqlite3
import sys
import pandas as pd

sys.path.insert(0, "C:/Users/brady/ipeds_completions")
import rankings

RENDER_DB = "C:/Users/brady/ipeds_completions/ipeds_render.db"

stabbr_to_fips = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09",
    "DE":"10","DC":"11","FL":"12","GA":"13","HI":"15","ID":"16","IL":"17",
    "IN":"18","IA":"19","KS":"20","KY":"21","LA":"22","ME":"23","MD":"24",
    "MA":"25","MI":"26","MN":"27","MS":"28","MO":"29","MT":"30","NE":"31",
    "NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36","NC":"37","ND":"38",
    "OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45","SD":"46",
    "TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56",
}
fips_to_stabbr = {v: k for k, v in stabbr_to_fips.items()}

conn = sqlite3.connect(RENDER_DB)

# Row counts in the slimmed tables
for tbl in ("completions_by_state", "completions_by_metro",
            "nc_sara_enrollment", "completions", "institutions"):
    n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"{tbl}: {n:,} rows")

# Confirm only the 3 anchor years remain in the redistributed tables
yrs_state = sorted([r[0] for r in conn.execute(
    "SELECT DISTINCT year FROM completions_by_state").fetchall()])
yrs_metro = sorted([r[0] for r in conn.execute(
    "SELECT DISTINCT year FROM completions_by_metro").fetchall()])
print(f"\nYears in completions_by_state: {yrs_state}")
print(f"Years in completions_by_metro: {yrs_metro}")
assert yrs_state == [2014, 2021, 2024], f"unexpected state years: {yrs_state}"
assert yrs_metro == [2014, 2021, 2024], f"unexpected metro years: {yrs_metro}"

# Run BSN markets ranking — uses all three trend years
markets = rankings.score_markets_for_program(
    conn, cipcode="51.3801", awlevels=(5,), market_grain="state",
    stabbr_to_fips=stabbr_to_fips, fips_to_stabbr=fips_to_stabbr,
    excluded_states=set(),
)
print("\nBSN state ranking (top 5):")
print(markets[["area_label", "completions", "composite", "grade",
               "completions_long_trend", "completions_pc_trend"]]
      .head(5).to_string(index=False))
assert not markets.empty
assert markets["completions_long_trend"].notna().any(), "long trend missing"
assert markets["completions_pc_trend"].notna().any(), "pc trend missing"

# Run programs-for-state ranking — also uses all three trend years
progs = rankings.score_programs_for_geo(
    conn, geo_key="state", geo_values=("CA",),
    awlevels=(5,), stabbr_to_fips=stabbr_to_fips, min_completions=25,
)
print("\nCA bachelor programs ranking (top 5):")
print(progs[["cipcode", "cipdesc", "completions",
             "completions_long_trend", "composite", "grade"]]
      .head(5).to_string(index=False))
assert not progs.empty
assert progs["completions_long_trend"].notna().any()

conn.close()
print("\nRender DB smoke checks passed.")
