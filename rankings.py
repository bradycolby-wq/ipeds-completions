"""
Demand-rankings engine for the VI IPEDS Data Explorer.

Two modes:
  * `score_programs_for_geo`  — Top Programs by Market.
        Given a geography (national / state(s) / metro(s)) and an award
        level, score every 6-digit CIP that has both completions data
        and a SOC mapping, then convert composite scores to letter
        grades within that cohort.
  * `score_markets_for_program` — Top Markets by Program.
        Given a CIP code and award level, score each state or each
        metro on demand for that program, then convert to letter
        grades within the chosen market grain.

Scoring uses z-scores per component within the cohort, weighted to a
0-100 composite, then percentiles within the cohort produce letter
grades from A+ down to F.

The post-2019 BLS OES window (SOC 2018 codes only) is used for
employment metrics. That keeps the joins simple — SOC 2010 bridging is
unnecessary for the 5-year CAGR we compute here — at the cost of a
shorter trend window than the explorer view.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np
import pandas as pd


# ── Letter grade utility ─────────────────────────────────────────────────────

# Percentile cut-points (ascending) and the grade each bucket earns.
# Top of cohort gets A+; bottom gets F. 11 buckets total.
_GRADE_CUTS = [
    (0.95, "A+"),
    (0.85, "A"),
    (0.75, "A-"),
    (0.65, "B+"),
    (0.55, "B"),
    (0.45, "B-"),
    (0.35, "C+"),
    (0.25, "C"),
    (0.15, "C-"),
    (0.05, "D"),
    (0.00, "F"),
]

# Grade -> hex color for table styling. Greens for A's, yellows for B/C,
# reds for D/F — bright but not garish, lifted from the existing VI palette.
GRADE_COLORS = {
    "A+": "#0F766E",
    "A":  "#15803D",
    "A-": "#22C55E",
    "B+": "#84CC16",
    "B":  "#EAB308",
    "B-": "#F59E0B",
    "C+": "#F97316",
    "C":  "#FB923C",
    "C-": "#EF4444",
    "D":  "#DC2626",
    "F":  "#991B1B",
}


def letter_grades(scores: pd.Series) -> pd.Series:
    """Map a series of composite scores to A+/A/.../F by percentile rank.

    Rows with NaN scores stay NaN. Ties share the same percentile (and
    therefore the same grade).
    """
    pct = scores.rank(pct=True, method="average")
    out = pd.Series(index=scores.index, dtype="object")
    for idx, p in pct.items():
        if pd.isna(p):
            out.loc[idx] = None
            continue
        for cut, grade in _GRADE_CUTS:
            if p >= cut:
                out.loc[idx] = grade
                break
    return out


def _zscore(values: pd.Series) -> pd.Series:
    """Z-score, robust to all-NaN or zero-variance inputs."""
    v = pd.to_numeric(values, errors="coerce")
    mean = v.mean()
    std = v.std(ddof=0)
    if not std or math.isnan(std):
        return pd.Series(0.0, index=v.index)
    return (v - mean) / std


def _composite(df: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    """Weighted sum of z-scored components, scaled to 0-100.

    Missing components contribute 0 (neutral z) rather than NaN, so a
    CIP without earnings data doesn't get dropped from the ranking.
    Weights are normalized to sum to 1 across the components that have
    at least one non-null value in the cohort.
    """
    present = {}
    for col, w in weights.items():
        if col not in df.columns:
            continue
        if df[col].notna().sum() == 0:
            continue
        present[col] = w
    if not present:
        return pd.Series(np.nan, index=df.index)

    total_w = sum(present.values())
    norm_w = {k: v / total_w for k, v in present.items()}

    z = pd.DataFrame(index=df.index)
    for col in present:
        z[col] = _zscore(df[col]).fillna(0)

    raw = sum(z[col] * w for col, w in norm_w.items())
    # Map z-score sum to roughly 0-100. Sum-of-weighted-z is mean 0 with
    # spread roughly ±2 in the tails; squash to a sigmoid then scale.
    return 50 + 25 * raw.clip(-2, 2)


# ── Component weights ────────────────────────────────────────────────────────

PROGRAM_WEIGHTS = {
    "emp_volume":           0.25,   # absolute size of opportunity
    "emp_growth":           0.15,   # recent momentum (5y CAGR)
    "emp_projection":       0.15,   # BLS projected CAGR (next 10y)
    "wage":                 0.15,   # employment-weighted median wage
    "earnings":             0.10,   # Scorecard median earnings 4yr post-grad
    "search_interest":      0.10,   # Google Trends interest in geography
    "automation_resilience":0.10,   # 11 - LMII risk
}

MARKET_WEIGHTS = {
    "emp_volume":         0.25,
    "location_quotient":  0.15,
    "emp_growth":         0.15,
    "emp_projection":     0.15,
    "wage":               0.15,
    "search_interest":    0.10,
    "competition_inv":    0.05,
}


# ── Shared helpers ───────────────────────────────────────────────────────────


def _awlevel_filter_sql(awlevels: tuple[int, ...]) -> str:
    """Translate award levels → cip_soc_crosswalk.awlevel_group filter.

    Mirrors the logic in app.run_employment_query so the SOC universe
    matches what the explorer view uses.
    """
    UNDERGRAD = {1, 2, 3, 4, 5, 20, 21}
    GRADUATE = {6, 7, 8, 17, 18, 19}
    has_u = bool(set(awlevels) & UNDERGRAD)
    has_g = bool(set(awlevels) & GRADUATE)
    if has_u and has_g:
        return ""  # any mapping
    if has_g:
        return " AND awlevel_group IN ('all', 'graduate')"
    return " AND awlevel_group = 'all'"


def _geo_to_oes_filter(geo_key: str, geo_values: tuple, stabbr_to_fips: dict):
    """Return (sql_fragment, params) to filter oes_employment by geo."""
    if geo_key == "national":
        return "AND oes.area_type = 1", []
    if geo_key == "state":
        fips = [stabbr_to_fips.get(s) for s in geo_values]
        fips = [f for f in fips if f]
        if not fips:
            return None, None
        ph = ",".join("?" * len(fips))
        return f"AND oes.area_type = 2 AND oes.area_code IN ({ph})", fips
    if geo_key == "metro":
        codes = ["00" + str(c).zfill(5) for c in geo_values]
        ph = ",".join("?" * len(codes))
        return f"AND oes.area_type = 4 AND oes.area_code IN ({ph})", codes
    return None, None


# ── Top Programs by Market ───────────────────────────────────────────────────


def score_programs_for_geo(
    conn,
    geo_key: str,
    geo_values: tuple,
    awlevels: tuple[int, ...],
    stabbr_to_fips: dict,
    cip_family: Optional[str] = None,
    min_completions: int = 25,
) -> pd.DataFrame:
    """Score every eligible CIP for demand in the chosen geography.

    Returns a DataFrame with columns:
        cipcode, cipdesc, n_socs, completions_latest,
        emp_volume, emp_growth, emp_projection, wage, earnings,
        search_interest, automation_risk, automation_resilience,
        composite, grade, rank
    """
    awlevel_filter = _awlevel_filter_sql(awlevels)
    area_sql, area_params = _geo_to_oes_filter(geo_key, geo_values, stabbr_to_fips)
    if area_sql is None:
        return pd.DataFrame()

    # ── 1. Eligible CIPs: those with completions ≥ threshold at this level + geo
    latest_yr = conn.execute("SELECT MAX(year) FROM completions").fetchone()[0]

    # Geo filter for completions: states map via institutions.stabbr;
    # metros via institutions.cbsa. National = all.
    comp_geo_join = ""
    comp_geo_where = ""
    comp_geo_params: list = []
    if geo_key == "state" and geo_values:
        comp_geo_join = (
            " JOIN institutions i "
            " ON i.unitid = c.unitid AND i.year = c.year"
        )
        ph = ",".join("?" * len(geo_values))
        comp_geo_where = f" AND i.stabbr IN ({ph})"
        comp_geo_params = list(geo_values)
    elif geo_key == "metro" and geo_values:
        comp_geo_join = (
            " JOIN institutions i "
            " ON i.unitid = c.unitid AND i.year = c.year"
        )
        ph = ",".join("?" * len(geo_values))
        comp_geo_where = f" AND i.cbsa IN ({ph})"
        comp_geo_params = list(str(v) for v in geo_values)

    awlevel_ph = ",".join("?" * len(awlevels))
    cip_family_clause = ""
    cip_family_params: list = []
    if cip_family:
        cip_family_clause = " AND substr(c.cipcode, 1, 2) = ?"
        cip_family_params = [cip_family]

    sql_eligible = f"""
        SELECT c.cipcode, SUM(c.ctotalt) AS completions
        FROM completions c
        {comp_geo_join}
        WHERE c.year = ?
          AND c.awlevel IN ({awlevel_ph})
          {comp_geo_where}
          {cip_family_clause}
        GROUP BY c.cipcode
        HAVING SUM(c.ctotalt) >= ?
        ORDER BY completions DESC
    """
    params_elig = (
        [latest_yr] + list(awlevels) + comp_geo_params
        + cip_family_params + [min_completions]
    )
    elig = pd.read_sql_query(sql_eligible, conn, params=params_elig)
    if elig.empty:
        return pd.DataFrame()
    cipcodes = tuple(elig["cipcode"].tolist())
    cip_ph = ",".join("?" * len(cipcodes))

    # ── 2. CIP → SOC mapping for these CIPs
    mapping = pd.read_sql_query(
        f"""SELECT cipcode, soc_code, soc_title
            FROM cip_soc_crosswalk
            WHERE cipcode IN ({cip_ph}) {awlevel_filter}""",
        conn,
        params=list(cipcodes),
    )

    # ── 3. Latest-year OES per (CIP via SOC): volume + wage
    oes_latest_yr = conn.execute(
        "SELECT MAX(year) FROM oes_employment WHERE year >= 2019"
    ).fetchone()[0]
    sql_oes_latest = f"""
        SELECT oes.occ_code,
               SUM(oes.tot_emp) AS tot_emp,
               CASE WHEN SUM(CASE WHEN oes.a_median IS NOT NULL THEN oes.tot_emp ELSE 0 END) > 0
                    THEN 1.0 * SUM(CASE WHEN oes.a_median IS NOT NULL THEN oes.tot_emp * oes.a_median ELSE 0 END)
                         / SUM(CASE WHEN oes.a_median IS NOT NULL THEN oes.tot_emp ELSE 0 END)
                    ELSE NULL END AS wage
        FROM oes_employment oes
        WHERE oes.year = ?
          {area_sql}
          AND oes.tot_emp IS NOT NULL
          AND oes.soc_version = 2018
        GROUP BY oes.occ_code
    """
    oes_latest = pd.read_sql_query(
        sql_oes_latest, conn, params=[oes_latest_yr] + (area_params or [])
    )

    # ── 4. Five-year-ago OES per SOC (for CAGR)
    oes_base_yr = max(2019, oes_latest_yr - 5)
    if oes_base_yr < oes_latest_yr:
        sql_oes_base = f"""
            SELECT oes.occ_code, SUM(oes.tot_emp) AS tot_emp_base
            FROM oes_employment oes
            WHERE oes.year = ?
              {area_sql}
              AND oes.tot_emp IS NOT NULL
              AND oes.soc_version = 2018
            GROUP BY oes.occ_code
        """
        oes_base = pd.read_sql_query(
            sql_oes_base, conn, params=[oes_base_yr] + (area_params or [])
        )
    else:
        oes_base = pd.DataFrame(columns=["occ_code", "tot_emp_base"])

    # ── 5. Projections per SOC for the chosen geo (national-level fallback)
    proj_geo_clause = ""
    proj_params: list = []
    if geo_key == "national":
        proj_geo_clause = " AND geo_level = 'national'"
    elif geo_key == "state" and geo_values:
        proj_ph = ",".join("?" * len(geo_values))
        proj_geo_clause = f" AND geo_level = 'state' AND geo_code IN ({proj_ph})"
        proj_params = list(geo_values)
    else:
        # No metro-level projections available — fall back to national
        proj_geo_clause = " AND geo_level = 'national'"

    sql_proj = f"""
        SELECT occ_code,
               1.0 * SUM(cagr * base_emp) / NULLIF(SUM(base_emp), 0) AS cagr
        FROM employment_projections
        WHERE 1=1 {proj_geo_clause}
          AND cagr IS NOT NULL AND base_emp IS NOT NULL
        GROUP BY occ_code
    """
    proj = pd.read_sql_query(sql_proj, conn, params=proj_params)

    # ── 6. Roll up SOC-level metrics to CIP via the crosswalk.
    # Each side join keys directly off mapping.soc_code, so SOCs that exist
    # in one OES year but not the other (BLS confidentiality combined codes
    # that get renumbered, e.g. 11-9198 → 11-9199) still contribute their
    # half of the CAGR rather than silently dropping out.
    socs_per_cip = mapping.merge(
        oes_latest.rename(columns={"occ_code": "soc_code"}),
        on="soc_code", how="left",
    )
    socs_per_cip = socs_per_cip.merge(
        oes_base.rename(columns={"occ_code": "soc_code"}),
        on="soc_code", how="left",
    )
    socs_per_cip = socs_per_cip.merge(
        proj.rename(columns={"occ_code": "soc_code"}),
        on="soc_code", how="left",
    )

    cip_agg = socs_per_cip.groupby("cipcode").agg(
        n_socs=("soc_code", "nunique"),
        emp_volume=("tot_emp", "sum"),
        emp_base=("tot_emp_base", "sum"),
        wage_num=("wage", lambda s: (s * socs_per_cip.loc[s.index, "tot_emp"]).sum(skipna=True)),
        wage_den=("tot_emp", lambda s: socs_per_cip.loc[s.index].dropna(subset=["wage"])["tot_emp"].sum()),
        proj_num=("cagr", lambda s: (s * socs_per_cip.loc[s.index, "tot_emp"]).sum(skipna=True)),
        proj_den=("tot_emp", lambda s: socs_per_cip.loc[s.index].dropna(subset=["cagr"])["tot_emp"].sum()),
    ).reset_index()
    cip_agg["wage"] = cip_agg["wage_num"] / cip_agg["wage_den"].replace(0, np.nan)
    cip_agg["emp_projection"] = cip_agg["proj_num"] / cip_agg["proj_den"].replace(0, np.nan)
    # CAGR from emp_base → emp_volume over (oes_latest_yr - oes_base_yr) years
    span = max(1, oes_latest_yr - oes_base_yr)
    with np.errstate(invalid="ignore"):
        cip_agg["emp_growth"] = np.where(
            (cip_agg["emp_base"] > 0) & (cip_agg["emp_volume"] > 0),
            (cip_agg["emp_volume"] / cip_agg["emp_base"]) ** (1 / span) - 1,
            np.nan,
        )

    # ── 7. Earnings (Scorecard) per CIP at chosen level
    awlevel_ph_sc = ",".join("?" * len(awlevels))
    cip4_set = sorted({c[:5] for c in cipcodes})
    cip4_ph = ",".join("?" * len(cip4_set))
    sc_geo_join = ""
    sc_geo_where = ""
    sc_geo_params: list = []
    if geo_key == "state" and geo_values:
        sc_geo_join = (
            " JOIN ( SELECT unitid, MAX(year) AS y FROM institutions GROUP BY unitid) imax "
            " ON sc.unitid = imax.unitid "
            " JOIN institutions i ON i.unitid = imax.unitid AND i.year = imax.y"
        )
        ph = ",".join("?" * len(geo_values))
        sc_geo_where = f" AND i.stabbr IN ({ph})"
        sc_geo_params = list(geo_values)
    elif geo_key == "metro" and geo_values:
        sc_geo_join = (
            " JOIN ( SELECT unitid, MAX(year) AS y FROM institutions GROUP BY unitid) imax "
            " ON sc.unitid = imax.unitid "
            " JOIN institutions i ON i.unitid = imax.unitid AND i.year = imax.y"
        )
        ph = ",".join("?" * len(geo_values))
        sc_geo_where = f" AND i.cbsa IN ({ph})"
        sc_geo_params = list(str(v) for v in geo_values)

    sql_sc = f"""
        SELECT sc.cipcode AS cip4,
               1.0 * SUM(sc.earn_mdn_4yr) / COUNT(*) AS earnings
        FROM college_scorecard sc
        {sc_geo_join}
        WHERE sc.earn_mdn_4yr IS NOT NULL
          AND sc.awlevel IN ({awlevel_ph_sc})
          AND sc.cipcode IN ({cip4_ph})
          {sc_geo_where}
        GROUP BY sc.cipcode
    """
    earnings_4d = pd.read_sql_query(
        sql_sc,
        conn,
        params=list(awlevels) + cip4_set + sc_geo_params,
    )
    earnings_map = dict(zip(earnings_4d["cip4"], earnings_4d["earnings"]))
    cip_agg["earnings"] = cip_agg["cipcode"].apply(lambda c: earnings_map.get(c[:5]))

    # ── 8. Search interest per CIP (Google Trends state-level)
    if geo_key == "national":
        sql_search = (
            f"SELECT cipcode, 1.0 * AVG(interest) AS search_interest "
            f"FROM google_trends_state WHERE cipcode IN ({cip_ph}) "
            f"GROUP BY cipcode"
        )
        search_df = pd.read_sql_query(sql_search, conn, params=list(cipcodes))
    elif geo_key == "state" and geo_values:
        ph = ",".join("?" * len(geo_values))
        sql_search = (
            f"SELECT cipcode, 1.0 * AVG(interest) AS search_interest "
            f"FROM google_trends_state "
            f"WHERE cipcode IN ({cip_ph}) AND state_abbr IN ({ph}) "
            f"GROUP BY cipcode"
        )
        search_df = pd.read_sql_query(
            sql_search, conn, params=list(cipcodes) + list(geo_values)
        )
    else:
        # Metro mode: fall back to national-level search volume
        sql_search = (
            f"SELECT cipcode, 1.0 * AVG(interest) AS search_interest "
            f"FROM google_trends_state WHERE cipcode IN ({cip_ph}) "
            f"GROUP BY cipcode"
        )
        search_df = pd.read_sql_query(sql_search, conn, params=list(cipcodes))
    cip_agg = cip_agg.merge(search_df, on="cipcode", how="left")

    # ── 9. Automation risk (employment-weighted across SOCs)
    risk = pd.read_sql_query(
        "SELECT occ_code, risk_score FROM occ_automation_risk",
        conn,
    )
    socs_risk = mapping.merge(
        oes_latest[["occ_code", "tot_emp"]].rename(columns={"occ_code": "soc_code"}),
        on="soc_code", how="left",
    )
    socs_risk = socs_risk.merge(
        risk.rename(columns={"occ_code": "soc_code"}),
        on="soc_code", how="left",
    )
    socs_risk["w"] = socs_risk["tot_emp"].fillna(0)
    risk_agg = (
        socs_risk.dropna(subset=["risk_score"])
                 .assign(num=lambda d: d["risk_score"] * d["w"])
                 .groupby("cipcode")
                 .agg(rnum=("num", "sum"), rden=("w", "sum"))
                 .reset_index()
    )
    risk_agg["automation_risk"] = risk_agg["rnum"] / risk_agg["rden"].replace(0, np.nan)
    cip_agg = cip_agg.merge(risk_agg[["cipcode", "automation_risk"]],
                            on="cipcode", how="left")
    cip_agg["automation_resilience"] = 11 - cip_agg["automation_risk"]

    # ── 10. Completions volume (already computed in step 1)
    cip_agg = cip_agg.merge(elig, on="cipcode", how="left")

    # ── 11. Friendly CIP title
    cip_titles = pd.read_sql_query(
        f"SELECT cipcode, ciptitle FROM cip_taxonomy WHERE cipcode IN ({cip_ph})",
        conn,
        params=list(cipcodes),
    )
    title_map = dict(zip(cip_titles["cipcode"], cip_titles["ciptitle"]))
    cip_agg["cipdesc"] = cip_agg["cipcode"].apply(lambda c: title_map.get(c, c))

    # ── 12. Score, grade, rank
    # Log-transform raw emp_volume before z-scoring (counts span orders of
    # magnitude). Other components are kept on their natural scale.
    cip_scoring = cip_agg.copy()
    cip_scoring["emp_volume"] = np.log1p(cip_scoring["emp_volume"].fillna(0))
    cip_agg["composite"] = _composite(cip_scoring, PROGRAM_WEIGHTS)
    cip_agg["grade"] = letter_grades(cip_agg["composite"])
    cip_agg["rank"] = cip_agg["composite"].rank(ascending=False, method="min").astype("Int64")
    cip_agg = cip_agg.sort_values("composite", ascending=False, na_position="last").reset_index(drop=True)

    keep = [
        "rank", "grade", "composite",
        "cipcode", "cipdesc", "completions", "n_socs",
        "emp_volume", "emp_growth", "emp_projection",
        "wage", "earnings", "search_interest",
        "automation_risk", "automation_resilience",
    ]
    return cip_agg[[c for c in keep if c in cip_agg.columns]]


# ── Top Markets by Program ───────────────────────────────────────────────────


def score_markets_for_program(
    conn,
    cipcode: str,
    awlevels: tuple[int, ...],
    market_grain: str,                # "state" or "metro"
    stabbr_to_fips: dict,
    fips_to_stabbr: dict,
    excluded_states: set,
    top_n: Optional[int] = None,
    min_emp: int = 100,
) -> pd.DataFrame:
    """Score each state (or metro) on demand for a given CIP.

    Returns a DataFrame with columns:
        rank, grade, composite, area_code, area_label,
        emp_volume, location_quotient, emp_growth, emp_projection,
        wage, search_interest, completions, competition_inv
    """
    awlevel_filter = _awlevel_filter_sql(awlevels)

    # ── SOC mapping for this CIP
    mapping = pd.read_sql_query(
        f"""SELECT soc_code FROM cip_soc_crosswalk
            WHERE cipcode = ?{awlevel_filter}""",
        conn,
        params=[cipcode],
    )
    socs = tuple(mapping["soc_code"].unique())
    if not socs:
        return pd.DataFrame()
    soc_ph = ",".join("?" * len(socs))

    # Latest OES year (SOC 2018 era)
    oes_latest_yr = conn.execute(
        "SELECT MAX(year) FROM oes_employment WHERE year >= 2019"
    ).fetchone()[0]
    oes_base_yr = max(2019, oes_latest_yr - 5)

    area_type = 2 if market_grain == "state" else 4

    # ── Volume + wage per market (latest year)
    sql_latest = f"""
        SELECT oes.area_code, oes.area_title,
               SUM(oes.tot_emp) AS emp_volume,
               CASE WHEN SUM(CASE WHEN oes.a_median IS NOT NULL THEN oes.tot_emp ELSE 0 END) > 0
                    THEN 1.0 * SUM(CASE WHEN oes.a_median IS NOT NULL THEN oes.tot_emp * oes.a_median ELSE 0 END)
                         / SUM(CASE WHEN oes.a_median IS NOT NULL THEN oes.tot_emp ELSE 0 END)
                    ELSE NULL END AS wage
        FROM oes_employment oes
        WHERE oes.year = ?
          AND oes.area_type = ?
          AND oes.occ_code IN ({soc_ph})
          AND oes.tot_emp IS NOT NULL
          AND oes.soc_version = 2018
        GROUP BY oes.area_code, oes.area_title
        HAVING SUM(oes.tot_emp) >= ?
    """
    df = pd.read_sql_query(
        sql_latest,
        conn,
        params=[oes_latest_yr, area_type] + list(socs) + [min_emp],
    )
    if df.empty:
        return pd.DataFrame()

    # ── Five-year-ago employment per market
    sql_base = f"""
        SELECT area_code, SUM(tot_emp) AS emp_base
        FROM oes_employment
        WHERE year = ? AND area_type = ?
          AND occ_code IN ({soc_ph})
          AND tot_emp IS NOT NULL
          AND soc_version = 2018
        GROUP BY area_code
    """
    base = pd.read_sql_query(
        sql_base, conn,
        params=[oes_base_yr, area_type] + list(socs),
    )
    df = df.merge(base, on="area_code", how="left")
    span = max(1, oes_latest_yr - oes_base_yr)
    with np.errstate(invalid="ignore"):
        df["emp_growth"] = np.where(
            (df["emp_base"] > 0) & (df["emp_volume"] > 0),
            (df["emp_volume"] / df["emp_base"]) ** (1 / span) - 1,
            np.nan,
        )

    # ── Total employment per market (all occupations) for LQ denominator
    sql_total = f"""
        SELECT area_code, SUM(tot_emp) AS total_emp
        FROM oes_employment
        WHERE year = ? AND area_type = ?
          AND tot_emp IS NOT NULL
          AND soc_version = 2018
        GROUP BY area_code
    """
    total = pd.read_sql_query(
        sql_total, conn, params=[oes_latest_yr, area_type],
    )
    df = df.merge(total, on="area_code", how="left")

    # National-level program + total employment for LQ
    nat_prog = conn.execute(
        f"""SELECT SUM(tot_emp) FROM oes_employment
            WHERE year = ? AND area_type = 1
              AND occ_code IN ({soc_ph})
              AND tot_emp IS NOT NULL AND soc_version = 2018""",
        [oes_latest_yr] + list(socs),
    ).fetchone()[0] or 0
    nat_total = conn.execute(
        """SELECT SUM(tot_emp) FROM oes_employment
           WHERE year = ? AND area_type = 1
             AND tot_emp IS NOT NULL AND soc_version = 2018""",
        [oes_latest_yr],
    ).fetchone()[0] or 1
    nat_share = nat_prog / nat_total if nat_total else None

    if nat_share and nat_share > 0:
        df["location_quotient"] = (
            (df["emp_volume"] / df["total_emp"].replace(0, np.nan))
            / nat_share
        )
    else:
        df["location_quotient"] = np.nan

    # ── Projections per market.
    # employment_projections.geo_code uses FIPS for states (e.g. "06" =
    # California) and 5-digit CBSA for metros (e.g. "35620" = NYC).
    proj_geo_clause = (
        "geo_level = 'state'" if market_grain == "state" else "geo_level = 'metro'"
    )
    sql_proj = f"""
        SELECT geo_code,
               1.0 * SUM(cagr * base_emp) / NULLIF(SUM(base_emp), 0) AS emp_projection
        FROM employment_projections
        WHERE {proj_geo_clause}
          AND occ_code IN ({soc_ph})
          AND cagr IS NOT NULL AND base_emp IS NOT NULL
        GROUP BY geo_code
    """
    proj = pd.read_sql_query(sql_proj, conn, params=list(socs))

    # OES area_code: states = 2-digit FIPS (matches as-is); metros = "00"
    # + 5-digit CBSA (strip the "00" prefix to match geo_code).
    if market_grain == "state":
        df["state_abbr"] = df["area_code"].apply(
            lambda c: fips_to_stabbr.get(str(c).zfill(2))
        )
        df["_proj_key"] = df["area_code"].astype(str).str.zfill(2)
    else:
        df["_proj_key"] = df["area_code"].astype(str).str[2:]
    df = df.merge(
        proj.rename(columns={"geo_code": "_proj_key"}),
        on="_proj_key", how="left",
    )
    df = df.drop(columns=["_proj_key"])

    # ── Search interest per market (state-level Google Trends)
    if market_grain == "state":
        sql_search = (
            "SELECT state_abbr, 1.0 * AVG(interest) AS search_interest "
            "FROM google_trends_state WHERE cipcode = ? GROUP BY state_abbr"
        )
        search = pd.read_sql_query(sql_search, conn, params=[cipcode])
        df = df.merge(search, on="state_abbr", how="left")
    else:
        df["search_interest"] = np.nan

    # ── Completions per market (for competition-inverse metric)
    latest_comp_yr = conn.execute("SELECT MAX(year) FROM completions").fetchone()[0]
    awlevel_ph = ",".join("?" * len(awlevels))
    if market_grain == "state":
        sql_comp = f"""
            SELECT i.stabbr AS state_abbr, SUM(c.ctotalt) AS completions
            FROM completions c
            JOIN institutions i ON i.unitid = c.unitid AND i.year = c.year
            WHERE c.year = ?
              AND c.cipcode = ?
              AND c.awlevel IN ({awlevel_ph})
            GROUP BY i.stabbr
        """
        comp_df = pd.read_sql_query(
            sql_comp, conn,
            params=[latest_comp_yr, cipcode] + list(awlevels),
        )
        df = df.merge(comp_df, on="state_abbr", how="left")
    else:
        sql_comp = f"""
            SELECT i.cbsa AS cbsa, SUM(c.ctotalt) AS completions
            FROM completions c
            JOIN institutions i ON i.unitid = c.unitid AND i.year = c.year
            WHERE c.year = ?
              AND c.cipcode = ?
              AND c.awlevel IN ({awlevel_ph})
              AND i.cbsa IS NOT NULL
            GROUP BY i.cbsa
        """
        comp_df = pd.read_sql_query(
            sql_comp, conn,
            params=[latest_comp_yr, cipcode] + list(awlevels),
        )
        # BLS metro area_code is "00" + 5-digit CBSA
        df["cbsa"] = df["area_code"].astype(str).str[2:]
        df = df.merge(comp_df, on="cbsa", how="left")

    df["completions"] = df["completions"].fillna(0)
    # Opportunity = emp_volume / (completions + 1) — fewer grads per job = better
    df["competition_inv"] = df["emp_volume"] / (df["completions"] + 1)

    # ── Exclude territories
    if market_grain == "state":
        df = df[~df["state_abbr"].isin(excluded_states)].copy()
        df["area_label"] = df["state_abbr"]
    else:
        df["area_label"] = df["area_title"]

    # ── Score, grade, rank
    df_scoring = df.copy()
    df_scoring["emp_volume"] = np.log1p(df_scoring["emp_volume"].fillna(0))
    df["composite"] = _composite(df_scoring, MARKET_WEIGHTS)
    df["grade"] = letter_grades(df["composite"])
    df["rank"] = df["composite"].rank(ascending=False, method="min").astype("Int64")
    df = df.sort_values("composite", ascending=False, na_position="last").reset_index(drop=True)

    keep = [
        "rank", "grade", "composite", "area_label", "area_code",
        "emp_volume", "location_quotient", "emp_growth", "emp_projection",
        "wage", "search_interest", "completions", "competition_inv",
    ]
    out = df[[c for c in keep if c in df.columns]]
    if top_n:
        out = out.head(top_n)
    return out.reset_index(drop=True)
