# -*- coding: utf-8 -*-
"""Smoke test for the redistributed completions pipeline.

Verifies:
1. The new tables (nc_sara_enrollment, unitid_opeid_crosswalk,
   completions_by_state, completions_by_metro) exist and have data.
2. Yearly totals in completions_by_state reconcile to within a few percent
   of the raw completions totals.
3. rankings.score_markets_for_program runs end-to-end for a representative
   CIP (Registered Nursing) at the state grain.
4. rankings.score_programs_for_geo runs for a state filter (CA).
5. Sanity check: Capella's HQ state (MN) no longer dominates BSN rankings.
"""
from __future__ import annotations

import sqlite3
import sys
import pandas as pd

sys.path.insert(0, "C:/Users/brady/ipeds_completions")
import rankings

DB = "C:/Users/brady/ipeds_completions/ipeds.db"
BSN_CIP = "51.3801"  # Registered Nursing
BACHELOR_AW = (5,)

conn = sqlite3.connect(DB)

# ── 1. Tables exist with rows
need = [
    "nc_sara_enrollment", "unitid_opeid_crosswalk",
    "completions_by_state", "completions_by_metro",
]
for t in need:
    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"{t}: {n:,} rows")
    assert n > 0, f"{t} is empty"

# ── 2. Reconciliation
rec = pd.read_sql_query(
    """
    SELECT raw.year, raw.raw, redist.redistributed,
           1.0 * (redist.redistributed - raw.raw) / raw.raw * 100 AS diff_pct
    FROM (SELECT year, SUM(ctotalt) AS raw FROM completions GROUP BY year) raw
    JOIN (SELECT year, SUM(completions) AS redistributed
            FROM completions_by_state GROUP BY year) redist
      ON raw.year = redist.year
    ORDER BY raw.year
    """,
    conn,
)
print("\nReconciliation (raw vs redistributed):")
print(rec.to_string(index=False))
worst = rec["diff_pct"].abs().max()
assert worst < 5.0, f"Reconciliation drift too large: {worst:.2f}%"

# ── 3. Markets ranking for BSN at the state grain
print("\nrankings.score_markets_for_program(BSN, bachelor, state) — top 10:")
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
markets = rankings.score_markets_for_program(
    conn, cipcode=BSN_CIP, awlevels=BACHELOR_AW,
    market_grain="state",
    stabbr_to_fips=stabbr_to_fips,
    fips_to_stabbr=fips_to_stabbr,
    excluded_states=set(),
)
print(markets[["area_label", "completions", "composite", "grade"]]
      .head(10).to_string(index=False))
assert not markets.empty, "BSN markets ranking is empty"

# ── 4. Capella sanity check — MN should not dominate BSN completions anymore
mn_row = markets.loc[markets["area_label"] == "MN", "completions"]
mn_completions = float(mn_row.iloc[0]) if not mn_row.empty else 0.0
total_completions = float(markets["completions"].sum())
mn_share = mn_completions / total_completions
print(f"\nMN share of BSN completions: {mn_share:.2%}")
# Pre-redistribution Capella alone reported 7k+ BSN grads to MN; with
# redistribution MN's share should sit closer to its population share (~1.7%).
assert mn_share < 0.05, (
    f"MN still dominates BSN at {mn_share:.1%}; "
    "expected redistribution to flatten it."
)

# ── 5. Programs ranking for CA filter
print("\nrankings.score_programs_for_geo(state=CA, bachelor) — top 10:")
progs = rankings.score_programs_for_geo(
    conn, geo_key="state", geo_values=("CA",),
    awlevels=BACHELOR_AW,
    stabbr_to_fips=stabbr_to_fips,
    min_completions=25,
)
if progs.empty:
    print("(empty)")
else:
    print(progs[["cipcode", "cipdesc", "completions", "composite", "grade"]]
          .head(10).to_string(index=False))
assert not progs.empty, "CA programs ranking is empty"

conn.close()
print("\nAll smoke checks passed.")
