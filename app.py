"""
IPEDS Completions Explorer
Streamlit app — academic years 2014-15 through 2023-24
"""

import sqlite3
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing

    _HAS_STATSMODELS = True
except ImportError:
    _HAS_STATSMODELS = False

# ── Config ────────────────────────────────────────────────────────────────────
# GitHub Release URL for the database (used on Streamlit Community Cloud)
_GITHUB_DB_URL = (
    "https://github.com/bradycolby-wq/ipeds-completions/releases/"
    "download/v1.0/ipeds.db"
)


def _get_db_path() -> Path:
    """Return path to ipeds.db, downloading from GitHub Release if needed."""
    local = Path(__file__).parent / "ipeds.db"
    if local.exists():
        return local  # local development

    # Cloud deployment: download to a writable cache location
    cache_dir = Path.home() / ".cache" / "ipeds"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached = cache_dir / "ipeds.db"

    if not cached.exists():
        with st.spinner("Downloading database (~600 MB) — this takes ~60 seconds on first launch..."):
            urllib.request.urlretrieve(_GITHUB_DB_URL, cached)

    return cached


DB_PATH = _get_db_path()

st.set_page_config(
    page_title="IPEDS Completions Explorer",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Reference data ────────────────────────────────────────────────────────────

AWARD_LEVELS = {
    1:  "Less than 1-year certificate",
    2:  "1–2 year certificate",
    3:  "Associate's degree",
    4:  "2–4 year certificate",
    5:  "Bachelor's degree",
    6:  "Post-baccalaureate certificate",
    7:  "Master's degree",
    8:  "Post-master's certificate",
    17: "Doctorate – Research/Scholarship",
    18: "Doctorate – Professional Practice",
    19: "Doctorate – Other",
}

CHART_COLORS = [
    "#f26822", "#0f86c1", "#e87537", "#6fb6da", "#faa94d",
    "#333333", "#666666", "#999999",
]

# State abbreviation -> FIPS code (for BLS OES state area queries)
STABBR_TO_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}

# Territories excluded from the platform
EXCLUDED_TERRITORIES = {"PR", "VI", "GU", "AS", "MP", "MH", "FM", "PW"}

EMPLOYMENT_COLORS = [
    "#0f86c1", "#e87537", "#6fb6da", "#f26822", "#faa94d",
    "#333333", "#8B5CF6", "#10B981", "#EF4444", "#F59E0B",
    "#6366F1", "#EC4899", "#14B8A6", "#F97316", "#8B5CF6",
]

# ── NCES projection constants ────────────────────────────────────────────────
# Maps IPEDS award level codes to NCES projection categories
NCES_CATEGORY_MAP = {
    3: "associates",
    5: "bachelors",
    7: "masters",
    17: "doctors",
    18: "doctors",
    19: "doctors",
}

# NCES Projections of Education Statistics to 2032, Table 318.10
# Projected total degrees conferred nationally, by category and academic year
# year key = start of academic year (e.g. 2024 = 2024-25)
NCES_PROJECTIONS = {
    "associates": {2024: 1029185, 2025: 1047212, 2026: 1067132, 2027: 1085468, 2028: 1100217},
    "bachelors":  {2024: 2167569, 2025: 2217039, 2026: 2270050, 2027: 2319984, 2028: 2363718},
    "masters":    {2024: 864457,  2025: 886365,  2026: 907435,  2027: 925313,  2028: 943396},
    "doctors":    {2024: 203053,  2025: 205173,  2026: 207292,  2027: 210434,  2028: 215090},
}


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA query_only = ON")
    return conn


def ensure_cbsa_index():
    """Add CBSA index if missing. Silently skip if DB is read-only."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_inst_cbsa ON institutions(cbsa)")
        conn.commit()
        conn.close()
    except sqlite3.OperationalError:
        pass  # read-only filesystem; index should already exist


@st.cache_data(show_spinner=False)
def load_states():
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT stabbr FROM institutions "
        "WHERE stabbr IS NOT NULL AND stabbr != '' ORDER BY stabbr"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows if r[0] not in EXCLUDED_TERRITORIES]


@st.cache_data(show_spinner=False)
def load_cbsas():
    """Return BLS OES metro areas that have IPEDS institutions, sorted by name.
    Excludes metros that are solely in excluded territories (PR, VI, etc.)."""
    territory_placeholders = ",".join(f"'{t}'" for t in EXCLUDED_TERRITORIES)
    conn = get_conn()
    rows = conn.execute(f"""
        SELECT b.cbsa, b.area_name
        FROM bls_oes_areas b
        INNER JOIN (
            SELECT DISTINCT cbsa FROM institutions
            WHERE cbsa IS NOT NULL AND CAST(cbsa AS INTEGER) > 0
              AND stabbr NOT IN ({territory_placeholders})
        ) i ON i.cbsa = b.cbsa
        WHERE b.area_type = 'metro'
        ORDER BY b.area_name
    """).fetchall()
    conn.close()
    return [(r[0], r[1]) for r in rows]


@st.cache_data(show_spinner=False)
def load_cip_options():
    """Return sorted list of (cipcode, display_label) for all codes with data."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT c.cipcode, COALESCE(t.ciptitle, c.cipcode) AS title
        FROM (SELECT DISTINCT cipcode FROM completions) c
        LEFT JOIN cip_taxonomy t ON c.cipcode = t.cipcode
        ORDER BY c.cipcode
    """).fetchall()
    conn.close()
    return [(r[0], f"{r[0]} \u2013 {r[1]}") for r in rows]


@st.cache_data(show_spinner=False)
def load_cip_crosswalk() -> dict[str, list[str]]:
    """Return mapping: new_cipcode -> [old_cipcode, ...] from the crosswalk table."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT new_cipcode, old_cipcode FROM cip_crosswalk "
            "WHERE old_cipcode != '__CHECKED__'"
        ).fetchall()
    except Exception:
        rows = []
    conn.close()
    result: dict[str, list[str]] = {}
    for new, old in rows:
        result.setdefault(new, []).append(old)
    return result


def expand_cip_patterns(cip_patterns: tuple) -> tuple:
    """Add predecessor CIP 2010 codes for any selected CIP 2020 codes."""
    if not cip_patterns:
        return cip_patterns
    crosswalk = load_cip_crosswalk()
    expanded = list(cip_patterns)
    for code in cip_patterns:
        if "%" not in code:  # only exact codes have crosswalk entries
            for old in crosswalk.get(code, []):
                if old not in expanded:
                    expanded.append(old)
    return tuple(expanded)


@st.cache_data(show_spinner=False, ttl=600)
def run_national_totals(awlevels: tuple):
    """Return {year: total_completions} nationally for the given award levels."""
    conn = get_conn()
    ph = ",".join("?" * len(awlevels))
    df = pd.read_sql_query(
        f"SELECT year, SUM(ctotalt) AS completions "
        f"FROM completions "
        f"WHERE majornum = 1 AND ctotalt > 0 AND awlevel IN ({ph}) "
        f"GROUP BY year ORDER BY year",
        conn,
        params=list(awlevels),
    )
    conn.close()
    return dict(zip(df["year"], df["completions"]))


def _nces_growth_index(selected_awlevels, proj_years):
    """Return {year: growth_index} based on NCES projections.

    The growth index is relative to an estimated base year (the year before the
    first projection year), computed by back-extrapolating from NCES using the
    average annual growth rate over the projection window.
    """
    cats = {NCES_CATEGORY_MAP[al] for al in selected_awlevels if al in NCES_CATEGORY_MAP}
    if not cats:
        return None

    # Combined NCES totals by year
    nces = {}
    for y in proj_years:
        nces[y] = sum(NCES_PROJECTIONS.get(c, {}).get(y, 0) for c in cats)

    first_y, last_y = proj_years[0], proj_years[-1]
    if nces.get(first_y, 0) <= 0:
        return None

    # Average annual growth over the NCES projection period
    n = last_y - first_y
    if n > 0 and nces[last_y] > 0:
        cagr = (nces[last_y] / nces[first_y]) ** (1 / n) - 1
    else:
        cagr = 0

    # Back-extrapolate one year to estimate NCES equivalent for our base year
    nces_base = nces[first_y] / (1 + cagr) if cagr else nces[first_y]

    return {y: nces.get(y, nces[last_y]) / nces_base for y in proj_years}


def compute_projection(sel_dict, national_dict, selected_awlevels, n_forward=5):
    """NCES-constrained top-down projection.

    1.  Compute selection's historical *share* of the national total for the
        chosen award levels.
    2.  Project shares forward with Holt exponential smoothing.
    3.  Project national totals forward using NCES growth indices (or Holt
        fallback for levels without NCES coverage).
    4.  Result = projected_share × projected_national.

    Returns list[(year, projected_completions)] or None on failure.
    """
    if not _HAS_STATSMODELS:
        return None

    years = sorted(set(sel_dict) & set(national_dict))
    if len(years) < 3:
        return None

    last_year = years[-1]
    proj_years = list(range(last_year + 1, last_year + n_forward + 1))

    # ── Historical shares ────────────────────────────────────────────────────
    shares = np.array([
        sel_dict[y] / national_dict[y] if national_dict[y] > 0 else 0
        for y in years
    ])

    # ── Project shares ───────────────────────────────────────────────────────
    try:
        if shares.std() < 1e-10:
            proj_shares = np.full(n_forward, shares[-1])
        else:
            fit = ExponentialSmoothing(
                shares, trend="add", initialization_method="estimated",
            ).fit(optimized=True, use_brute=True)
            proj_shares = fit.forecast(n_forward)
    except Exception:
        # Linear fallback
        slope = np.polyfit(np.arange(len(shares)), shares, 1)[0]
        proj_shares = shares[-1] + slope * np.arange(1, n_forward + 1)
    proj_shares = np.clip(proj_shares, 0, 1)

    # ── Project national totals ──────────────────────────────────────────────
    last_national = national_dict[last_year]
    growth = _nces_growth_index(selected_awlevels, proj_years)

    if growth:
        proj_nationals = np.array([last_national * growth[y] for y in proj_years])
    else:
        # No NCES coverage → Holt on national totals
        nat_vals = np.array([national_dict[y] for y in years])
        try:
            fit = ExponentialSmoothing(
                nat_vals, trend="add", initialization_method="estimated",
            ).fit(optimized=True, use_brute=True)
            proj_nationals = fit.forecast(n_forward)
        except Exception:
            slope = np.polyfit(np.arange(len(nat_vals)), nat_vals, 1)[0]
            proj_nationals = nat_vals[-1] + slope * np.arange(1, n_forward + 1)
        proj_nationals = np.maximum(proj_nationals, 0)

    # ── Final constrained projection ─────────────────────────────────────────
    result = proj_shares * proj_nationals
    return list(zip(proj_years, np.maximum(result, 0).astype(int)))


@st.cache_data(show_spinner=False, ttl=600)
def run_institution_query(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Return year × institution completions using the same filters as run_query."""
    cip_patterns = expand_cip_patterns(cip_patterns)

    conn = get_conn()
    params = []
    where = [
        "majornum = 1",
        "ctotalt IS NOT NULL",
        "ctotalt > 0",
    ]

    if cip_patterns:
        cip_clauses = []
        for p in cip_patterns:
            cip_clauses.append("cipcode LIKE ?" if "%" in p else "cipcode = ?")
            params.append(p)
        where.append(f"({' OR '.join(cip_clauses)})")

    if awlevels:
        placeholders = ",".join("?" * len(awlevels))
        where.append(f"awlevel IN ({placeholders})")
        params.extend(awlevels)

    if geo_key == "state" and geo_values:
        placeholders = ",".join("?" * len(geo_values))
        where.append(f"stabbr IN ({placeholders})")
        params.extend(geo_values)
    elif geo_key == "metro" and geo_values:
        placeholders = ",".join("?" * len(geo_values))
        where.append(f"cbsa IN ({placeholders})")
        params.extend(geo_values)

    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT
            year,
            unitid,
            MAX(instnm)       AS instnm,
            MAX(city)         AS city,
            MAX(stabbr)       AS stabbr,
            MAX(control_name) AS control_name,
            SUM(ctotalt)      AS completions
        FROM completions_view
        {where_sql}
        GROUP BY unitid, year
        ORDER BY unitid, year
    """

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


@st.cache_data(show_spinner=False, ttl=600)
def run_query(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
    split_by_level: bool,
):
    # Transparently include predecessor CIP 2010 codes for historical continuity
    cip_patterns = expand_cip_patterns(cip_patterns)

    conn = get_conn()
    params = []
    where = [
        "majornum = 1",
        "ctotalt IS NOT NULL",
        "ctotalt > 0",
    ]

    # CIP filter
    if cip_patterns:
        cip_clauses = []
        for p in cip_patterns:
            cip_clauses.append("cipcode LIKE ?" if "%" in p else "cipcode = ?")
            params.append(p)
        where.append(f"({' OR '.join(cip_clauses)})")

    # Award level filter
    if awlevels:
        placeholders = ",".join("?" * len(awlevels))
        where.append(f"awlevel IN ({placeholders})")
        params.extend(awlevels)

    # Geography filter
    if geo_key == "state" and geo_values:
        placeholders = ",".join("?" * len(geo_values))
        where.append(f"stabbr IN ({placeholders})")
        params.extend(geo_values)
    elif geo_key == "metro" and geo_values:
        placeholders = ",".join("?" * len(geo_values))
        where.append(f"cbsa IN ({placeholders})")
        params.extend(geo_values)

    where_sql = "WHERE " + " AND ".join(where)

    if split_by_level:
        select   = "year, awlevel, award_level_name, SUM(ctotalt) AS completions"
        group_by = "year, awlevel, award_level_name"
        order_by = "year, awlevel"
    else:
        select   = "year, SUM(ctotalt) AS completions"
        group_by = "year"
        order_by = "year"

    sql = f"""
        SELECT {select}
        FROM completions_view
        {where_sql}
        GROUP BY {group_by}
        ORDER BY {order_by}
    """

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


@st.cache_data(show_spinner=False, ttl=600)
def run_employment_query(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Query OES employment data for occupations related to selected CIP codes.

    Handles SOC version differences:
      - 2015-2017 OES data uses SOC 2010 codes
      - 2018-2024 OES data uses SOC 2018 codes
      - CIP-SOC crosswalk maps CIP -> SOC 2018
      - soc_2010_to_2018 table bridges the gap for older years

    Uses awlevel_group filtering:
      - undergrad levels (1-5): include "all" + exclude "graduate"-only
      - graduate levels (6-19): include "all" + "graduate"
      - mixed: include all mappings
    """
    conn = get_conn()

    # Determine award-level group filter
    # undergrad: awlevel 1-5; graduate: awlevel 6+
    UNDERGRAD_LEVELS = {1, 2, 3, 4, 5}
    GRADUATE_LEVELS = {6, 7, 8, 17, 18, 19}
    has_undergrad = bool(set(awlevels) & UNDERGRAD_LEVELS)
    has_graduate = bool(set(awlevels) & GRADUATE_LEVELS)

    if has_undergrad and has_graduate:
        awlevel_filter = ""  # include all mappings
    elif has_graduate:
        awlevel_filter = " AND awlevel_group IN ('all', 'graduate')"
    else:
        # Undergrad only: exclude graduate-only mappings
        awlevel_filter = " AND awlevel_group = 'all'"

    # 1. Find SOC 2018 codes mapped to the selected CIP codes
    if cip_patterns:
        cip_clauses = []
        cip_params = []
        for p in cip_patterns:
            cip_clauses.append("cipcode LIKE ?" if "%" in p else "cipcode = ?")
            cip_params.append(p)
        cip_where = " OR ".join(cip_clauses)
        soc_rows = conn.execute(
            f"SELECT DISTINCT soc_code FROM cip_soc_crosswalk WHERE ({cip_where}){awlevel_filter}",
            cip_params,
        ).fetchall()
    else:
        soc_rows = conn.execute(
            f"SELECT DISTINCT soc_code FROM cip_soc_crosswalk WHERE 1=1{awlevel_filter}"
        ).fetchall()

    soc_2018_codes = [r[0] for r in soc_rows]
    if not soc_2018_codes:
        conn.close()
        return pd.DataFrame()

    # 2. Also get SOC 2010 equivalents for querying older data
    soc_ph = ",".join("?" * len(soc_2018_codes))
    soc_2010_rows = conn.execute(
        f"SELECT DISTINCT soc_2010, soc_2018 FROM soc_2010_to_2018 "
        f"WHERE soc_2018 IN ({soc_ph})",
        soc_2018_codes,
    ).fetchall()
    soc_2010_codes = [r[0] for r in soc_2010_rows]
    soc_2010_to_2018_map = {r[0]: r[1] for r in soc_2010_rows}

    # 3. Build area filter
    area_where = ""
    area_params = []
    if geo_key == "national":
        area_where = "AND area_type = 1"
    elif geo_key == "state" and geo_values:
        fips_codes = [STABBR_TO_FIPS.get(s, "") for s in geo_values]
        fips_codes = [f for f in fips_codes if f]
        if fips_codes:
            ph = ",".join("?" * len(fips_codes))
            area_where = f"AND area_type = 2 AND area_code IN ({ph})"
            area_params = fips_codes
        else:
            conn.close()
            return pd.DataFrame()
    elif geo_key == "metro" and geo_values:
        # Our geo_values are 5-digit CBSAs; BLS uses 00+CBSA (7-digit)
        bls_codes = ["00" + str(c).zfill(5) for c in geo_values]
        ph = ",".join("?" * len(bls_codes))
        area_where = f"AND area_type = 4 AND area_code IN ({ph})"
        area_params = bls_codes

    # 4. Query: UNION of SOC 2018 data (2018+) and SOC 2010 data (2015-2017)
    # For 2018+ data, use SOC 2018 codes directly
    soc_ph_2018 = ",".join("?" * len(soc_2018_codes))
    params_2018 = soc_2018_codes + area_params

    sql_2018 = f"""
        SELECT year, occ_code, occ_title,
               SUM(tot_emp) AS tot_emp,
               CASE WHEN COUNT(CASE WHEN a_mean IS NOT NULL THEN 1 END) > 0
                    THEN CAST(SUM(CASE WHEN a_mean IS NOT NULL THEN tot_emp * a_mean ELSE 0 END)
                         / NULLIF(SUM(CASE WHEN a_mean IS NOT NULL THEN tot_emp ELSE 0 END), 0) AS INTEGER)
                    ELSE NULL END AS a_mean,
               CASE WHEN COUNT(CASE WHEN a_median IS NOT NULL THEN 1 END) > 0
                    THEN CAST(SUM(CASE WHEN a_median IS NOT NULL THEN tot_emp * a_median ELSE 0 END)
                         / NULLIF(SUM(CASE WHEN a_median IS NOT NULL THEN tot_emp ELSE 0 END), 0) AS INTEGER)
                    ELSE NULL END AS a_median
        FROM oes_employment
        WHERE year >= 2018
          AND occ_code IN ({soc_ph_2018})
          {area_where}
        GROUP BY year, occ_code, occ_title
    """

    # For pre-2018 data, use SOC 2010 codes and map to 2018
    dfs = [pd.read_sql_query(sql_2018, conn, params=params_2018)]

    if soc_2010_codes:
        soc_ph_2010 = ",".join("?" * len(soc_2010_codes))
        params_2010 = soc_2010_codes + area_params

        sql_2010 = f"""
            SELECT year, occ_code, occ_title,
                   SUM(tot_emp) AS tot_emp,
                   CASE WHEN COUNT(CASE WHEN a_mean IS NOT NULL THEN 1 END) > 0
                        THEN CAST(SUM(CASE WHEN a_mean IS NOT NULL THEN tot_emp * a_mean ELSE 0 END)
                             / NULLIF(SUM(CASE WHEN a_mean IS NOT NULL THEN tot_emp ELSE 0 END), 0) AS INTEGER)
                        ELSE NULL END AS a_mean,
                   CASE WHEN COUNT(CASE WHEN a_median IS NOT NULL THEN 1 END) > 0
                        THEN CAST(SUM(CASE WHEN a_median IS NOT NULL THEN tot_emp * a_median ELSE 0 END)
                             / NULLIF(SUM(CASE WHEN a_median IS NOT NULL THEN tot_emp ELSE 0 END), 0) AS INTEGER)
                        ELSE NULL END AS a_median
            FROM oes_employment
            WHERE year < 2018
              AND occ_code IN ({soc_ph_2010})
              {area_where}
            GROUP BY year, occ_code, occ_title
        """

        df_2010 = pd.read_sql_query(sql_2010, conn, params=params_2010)
        # Map SOC 2010 codes to SOC 2018 for consistent time series
        if not df_2010.empty:
            df_2010["occ_code"] = df_2010["occ_code"].map(
                lambda x: soc_2010_to_2018_map.get(x, x)
            )
            # Re-aggregate after remapping (multiple 2010 codes may map to one 2018 code)
            df_2010 = df_2010.groupby(["year", "occ_code"]).agg({
                "occ_title": "first",
                "tot_emp": "sum",
                "a_mean": "first",
                "a_median": "first",
            }).reset_index()
            dfs.append(df_2010)

    conn.close()

    if not dfs or all(d.empty for d in dfs):
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)
    # Update occ_title for remapped codes (use 2018+ titles)
    title_map = result[result["year"] >= 2018].drop_duplicates("occ_code").set_index("occ_code")["occ_title"].to_dict()
    result["occ_title"] = result["occ_code"].map(lambda x: title_map.get(x, x))

    return result.sort_values(["occ_code", "year"]).reset_index(drop=True)


@st.cache_data(show_spinner=False, ttl=3600)
def get_projection_coverage():
    """Load metro projection coverage tracking data."""
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT * FROM metro_projection_coverage ORDER BY state_abbr, cbsa_name", conn)
        return df
    except Exception:
        return None
    finally:
        conn.close()


@st.cache_data(show_spinner=False, ttl=600)
def get_employment_projections(
    soc_codes: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Fetch projected growth (CAGR) for given SOC codes and geography.

    Returns DataFrame with columns: occ_code, cagr, base_year, proj_year, source.
    Uses best available geo match: metro > state > national.
    """
    conn = get_conn()

    # Check if projections table exists
    try:
        conn.execute("SELECT 1 FROM employment_projections LIMIT 1")
    except Exception:
        conn.close()
        return pd.DataFrame()

    if not soc_codes:
        conn.close()
        return pd.DataFrame()

    soc_ph = ",".join("?" * len(soc_codes))

    # Build geo filter based on geo_key
    if geo_key == "metro" and geo_values:
        # Try metro first, then fall back to state/national
        cbsa_ph = ",".join("?" * len(geo_values))
        # For metro, we might have multiple CBSAs — average across them
        sql = f"""
            SELECT occ_code, AVG(cagr) as cagr,
                   MIN(base_year) as base_year, MAX(proj_year) as proj_year,
                   'metro' as geo_level
            FROM employment_projections
            WHERE occ_code IN ({soc_ph})
              AND geo_level = 'metro'
              AND geo_code IN ({cbsa_ph})
            GROUP BY occ_code
        """
        params = list(soc_codes) + list(geo_values)
        df = pd.read_sql_query(sql, conn, params=params)

    elif geo_key == "state" and geo_values:
        fips_codes = [STABBR_TO_FIPS.get(s, "") for s in geo_values]
        fips_codes = [f for f in fips_codes if f]
        if fips_codes:
            fips_ph = ",".join("?" * len(fips_codes))
            sql = f"""
                SELECT occ_code, AVG(cagr) as cagr,
                       MIN(base_year) as base_year, MAX(proj_year) as proj_year,
                       'state' as geo_level
                FROM employment_projections
                WHERE occ_code IN ({soc_ph})
                  AND geo_level = 'state'
                  AND geo_code IN ({fips_ph})
                GROUP BY occ_code
            """
            params = list(soc_codes) + fips_codes
            df = pd.read_sql_query(sql, conn, params=params)
        else:
            df = pd.DataFrame()
    else:
        # National
        sql = f"""
            SELECT occ_code, cagr, base_year, proj_year, 'national' as geo_level
            FROM employment_projections
            WHERE occ_code IN ({soc_ph})
              AND geo_level = 'national'
        """
        df = pd.read_sql_query(sql, conn, params=list(soc_codes))

    # If metro/state returned nothing, fall back to national
    if df.empty and geo_key != "national":
        sql = f"""
            SELECT occ_code, cagr, base_year, proj_year, 'national' as geo_level
            FROM employment_projections
            WHERE occ_code IN ({soc_ph})
              AND geo_level = 'national'
        """
        df = pd.read_sql_query(sql, conn, params=list(soc_codes))

    conn.close()
    return df


# ── App ───────────────────────────────────────────────────────────────────────
def main():
    # One-time DB prep
    ensure_cbsa_index()

    # ── Global styles ─────────────────────────────────────────────────────────
    st.markdown(
        """
        <link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap" rel="stylesheet">
        <style>
        html, body, [class*="css"], .stApp, .stMarkdown, .stTextInput,
        .stSelectbox, .stMultiSelect, .stRadio, .stCheckbox, .stMetric,
        .stSidebar, .stButton, .stCaption, .stExpander, .stDataFrame,
        button, input, select, textarea {
            font-family: 'Montserrat', Arial, sans-serif !important;
        }
        h1, h2, h3, h4, h5, h6,
        .stTitle, [data-testid="stMetricValue"],
        .stSidebar h1, .stSidebar h2, .stSidebar h3 {
            font-family: 'Montserrat', Arial, sans-serif !important;
            color: #f26822 !important;
        }
        [data-testid="stMetricLabel"] {
            font-family: 'Montserrat', Arial, sans-serif !important;
            color: #666666 !important;
        }
        [data-testid="stMetricDelta"] {
            font-family: 'Montserrat', Arial, sans-serif !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## 🎓 IPEDS Explorer")
        st.caption("Completions 2014–15 → 2023–24")
        st.divider()

        # 1. Geography
        st.markdown("### 1 · Geography")
        geo_type = st.radio(
            "scope",
            ["National", "By State", "By Metro Area"],
            label_visibility="collapsed",
        )

        geo_values = []
        selected_metro_labels = []
        all_states = False
        all_metros = False

        if geo_type == "By State":
            states = load_states()
            all_states = st.checkbox("All states", value=False, key="all_states")
            if all_states:
                geo_values = list(states)
            else:
                selected_states = st.multiselect(
                    "State(s):",
                    options=states,
                    placeholder="Select states…",
                )
                geo_values = selected_states

        elif geo_type == "By Metro Area":
            cbsa_list = load_cbsas()
            cbsa_display_to_code = {label: code for code, label in cbsa_list}
            all_metros = st.checkbox("All metro areas", value=False, key="all_metros")
            if all_metros:
                selected_metro_labels = [label for _, label in cbsa_list]
                geo_values = [code for code, _ in cbsa_list]
            else:
                selected_metro_labels = st.multiselect(
                    "Metro area(s):",
                    options=[label for _, label in cbsa_list],
                    placeholder="Search metro areas…",
                )
                geo_values = [cbsa_display_to_code[l] for l in selected_metro_labels]

        st.divider()

        # 2. Subject (6-digit CIP)
        st.markdown("### 2 · Subject")

        all_cips = st.checkbox("All CIP codes", value=False, key="all_cips")

        cip_options = load_cip_options()
        cip_label_to_code = {label: code for code, label in cip_options}

        if all_cips:
            selected_cip_labels = []
            cip_patterns = ()  # empty = no filter = all
        else:
            default_cip_labels = [l for _, l in cip_options if l.startswith("51.3801")]
            selected_cip_labels = st.multiselect(
                "CIP code(s):",
                options=[label for _, label in cip_options],
                default=default_cip_labels,
                placeholder="Search by code or name…",
                label_visibility="collapsed",
            )
            cip_patterns = tuple(cip_label_to_code[l] for l in selected_cip_labels)

        st.caption(
            "🔍 [Look up CIP codes](https://nces.ed.gov/ipeds/cipcode/default.aspx?y=56)"
        )

        st.divider()

        # 3. Award Level
        st.markdown("### 3 · Program Level")

        all_levels = st.checkbox("All award levels", value=False, key="all_levels")

        # Build option list: individual levels + aggregate groups
        AGGREGATE_LEVELS = {
            "Undergraduate Certificate": (1, 2, 4),
            "Graduate Certificate": (6, 8),
            "Doctoral Degree": (17, 18, 19),
        }
        level_option_labels = list(AGGREGATE_LEVELS.keys()) + [v for v in AWARD_LEVELS.values()]
        level_label_to_code = {v: k for k, v in AWARD_LEVELS.items()}

        if all_levels:
            selected_level_labels = list(AWARD_LEVELS.values())
            selected_awlevels = tuple(AWARD_LEVELS.keys())
        else:
            default_levels = ["Bachelor's degree"]
            selected_level_labels = st.multiselect(
                "Award level(s):",
                options=level_option_labels,
                default=default_levels,
                placeholder="Choose levels…",
                label_visibility="collapsed",
            )
            # Expand aggregate groups into individual awlevel codes
            awlevel_set = set()
            for lbl in selected_level_labels:
                if lbl in AGGREGATE_LEVELS:
                    awlevel_set.update(AGGREGATE_LEVELS[lbl])
                else:
                    awlevel_set.add(level_label_to_code[lbl])
            selected_awlevels = tuple(sorted(awlevel_set))

    # ── Main area ─────────────────────────────────────────────────────────────
    st.title("IPEDS Completions Trend Explorer")
    st.caption(
        "Total degrees and certificates awarded by IPEDS-reporting institutions "
        "| Source: NCES IPEDS Completions Survey"
    )

    # Determine geo_key for query — "All states" is functionally national
    if geo_type == "By State" and all_states:
        geo_key = "national"
    else:
        geo_key = {"National": "national", "By State": "state", "By Metro Area": "metro"}[geo_type]

    # Validate — show landing if incomplete
    geo_ready = (geo_type == "National") or bool(geo_values)
    cip_ready = all_cips or bool(cip_patterns)
    level_ready = all_levels or bool(selected_awlevels)

    if not (geo_ready and cip_ready and level_ready):
        c1, c2, c3 = st.columns(3)
        status = lambda ok: "✅" if ok else "⬜"
        c1.info(f"{status(geo_ready)} **Step 1:** Select a geography")
        c2.info(f"{status(cip_ready)} **Step 2:** Select CIP code(s)")
        c3.info(f"{status(level_ready)} **Step 3:** Select program level(s)")
        st.divider()
        st.markdown(
            "Use the sidebar to build your query. This tool searches "
            f"**{3_000_000:,}+** completions records across 10 academic years "
            "from ~7,000 U.S. postsecondary institutions."
        )
        return

    # ── Query ─────────────────────────────────────────────────────────────────
    with st.spinner("Querying…"):
        df = run_query(
            cip_patterns=cip_patterns,
            awlevels=selected_awlevels,
            geo_key=geo_key,
            geo_values=tuple(geo_values),
            split_by_level=True,
        )
        df_inst = run_institution_query(
            cip_patterns=cip_patterns,
            awlevels=selected_awlevels,
            geo_key=geo_key,
            geo_values=tuple(geo_values),
        )

    if df.empty:
        st.warning(
            "No completions found for these filters. "
            "Try selecting a broader CIP series, more award levels, or a larger geography."
        )
        return

    # ── Build labels ──────────────────────────────────────────────────────────
    if geo_type == "National":
        geo_label = "United States"
    elif geo_type == "By State":
        geo_label = "All States" if all_states else ", ".join(geo_values)
    else:
        if all_metros:
            geo_label = "All BLS Metro Areas"
        elif selected_metro_labels:
            geo_label = ", ".join(selected_metro_labels)
        else:
            geo_label = "Selected Metro Areas"

    if all_cips:
        cip_display = "All Programs"
    elif len(selected_cip_labels) == 1:
        cip_display = selected_cip_labels[0].split(" \u2013 ", 1)[-1]
    elif len(selected_cip_labels) <= 3:
        cip_display = " / ".join(l.split(" \u2013 ", 1)[-1] for l in selected_cip_labels)
    else:
        cip_display = f"{len(selected_cip_labels)} CIP codes"

    if all_levels:
        level_str = "All Award Levels"
    elif len(selected_level_labels) <= 2:
        level_str = " & ".join(selected_level_labels)
    else:
        level_str = f"{len(selected_level_labels)} award levels"

    def yr_label(y):
        return f"{y}–{str(y + 1)[-2:]}"

    def yr_label_short(y):
        return f"'{str(y)[-2:]}–'{str(y + 1)[-2:]}"

    all_years = sorted(df["year"].unique())
    year_tick_labels = [yr_label(y) for y in all_years]

    # ── Compute projection (needed by metrics + chart) ──────────────────────
    df_totals = df.groupby("year")["completions"].sum()
    sel_dict = df_totals.to_dict()
    national_dict = run_national_totals(selected_awlevels)
    projection = compute_projection(sel_dict, national_dict, selected_awlevels)

    # ── Summary metrics ───────────────────────────────────────────────────────
    agg = df_totals
    first_yr, last_yr = agg.index.min(), agg.index.max()
    last_val = int(agg[last_yr])

    # 10-year CAGR
    n10 = last_yr - first_yr
    first_val = int(agg[first_yr])
    cagr_10 = (last_val / first_val) ** (1 / n10) - 1 if first_val and n10 > 0 else None

    # 3-year CAGR
    yr_3ago = last_yr - 3
    val_3ago = int(agg[yr_3ago]) if yr_3ago in agg.index else None
    cagr_3 = (last_val / val_3ago) ** (1 / 3) - 1 if val_3ago else None

    # Projected CAGR
    if projection and last_val > 0:
        proj_last_yr, proj_last_val = projection[-1]
        n_proj = proj_last_yr - last_yr
        cagr_proj = (proj_last_val / last_val) ** (1 / n_proj) - 1 if n_proj > 0 else None
    else:
        proj_last_yr, cagr_proj = last_yr + 5, None

    # Institution count
    n_inst = df_inst["unitid"].nunique()

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric(f"{yr_label(last_yr)} Completions", f"{last_val:,}")
    m2.metric(
        f"10-yr CAGR ({yr_label(first_yr)} → {yr_label(last_yr)})",
        f"{cagr_10:+.1%}" if cagr_10 is not None else "N/A",
    )
    m3.metric(
        f"3-yr CAGR ({yr_label(yr_3ago)} → {yr_label(last_yr)})",
        f"{cagr_3:+.1%}" if cagr_3 is not None else "N/A",
    )
    m4.metric(
        f"Projected CAGR ({yr_label(last_yr)} → {yr_label(proj_last_yr)})",
        f"{cagr_proj:+.1%}" if cagr_proj is not None else "N/A",
    )
    m5.metric("Reporting Institutions", f"{n_inst:,}")

    st.divider()

    # ── Chart ─────────────────────────────────────────────────────────────────
    chart_title = (
        f"<b>{cip_display}</b>"
        f"<br><sup style='color:#999999'>{level_str} · {geo_label}</sup>"
    )

    if "award_level_name" in df.columns:
        fig = px.line(
            df,
            x="year",
            y="completions",
            color="award_level_name",
            text="completions",
            markers=True,
            title=chart_title,
            labels={
                "year": "",
                "completions": "Total Completions",
                "award_level_name": "Award Level",
            },
            color_discrete_sequence=CHART_COLORS,
        )
        fig.update_traces(
            mode="lines+markers+text",
            textposition="top center",
            texttemplate="%{y:,.0f}",
            textfont=dict(size=11),
            hovertemplate="<b>%{fullData.name}</b><br>%{y:,.0f} completions<extra></extra>",
            line=dict(width=2.5),
            marker=dict(size=8),
        )
    else:
        df_agg = df.groupby("year")["completions"].sum().reset_index()
        fig = px.line(
            df_agg,
            x="year",
            y="completions",
            text="completions",
            markers=True,
            title=chart_title,
            labels={"year": "", "completions": "Total Completions"},
            color_discrete_sequence=["#f26822"],
        )
        fig.update_traces(
            mode="lines+markers+text",
            textposition="top center",
            texttemplate="%{y:,.0f}",
            textfont=dict(size=11),
            hovertemplate="%{y:,.0f} completions<extra></extra>",
            line=dict(width=2.5),
            marker=dict(size=9),
        )

    # ── NCES-constrained projection (5 years forward) ───────────────────────
    chart_years = list(all_years)

    if projection:
        proj_years_list = [p[0] for p in projection]
        proj_vals_list = [p[1] for p in projection]
        chart_years = list(all_years) + proj_years_list

        # Faint gray shaded region over the projection area
        fig.add_vrect(
            x0=all_years[-1] + 0.5,
            x1=proj_years_list[-1] + 0.5,
            fillcolor="#E5E7EB",
            opacity=0.3,
            layer="below",
            line_width=0,
        )

        # Projection line — semi-transparent orange dashes matching the actuals color
        fig.add_trace(go.Scatter(
            x=[all_years[-1]] + proj_years_list,
            y=[int(df_totals[all_years[-1]])] + proj_vals_list,
            mode="lines+markers+text",
            name="Projected (NCES-constrained)",
            line=dict(color="rgba(242, 104, 34, 0.45)", width=2.5, dash="dash"),
            marker=dict(size=7, symbol="diamond", color="rgba(242, 104, 34, 0.45)"),
            text=[""] + [f"{v:,}" for v in proj_vals_list],
            textposition="top center",
            textfont=dict(size=10, color="rgba(242, 104, 34, 0.6)"),
            hovertemplate="%{y:,.0f} (projected)<extra></extra>",
        ))

    chart_tick_labels = [yr_label(y) for y in chart_years]

    fig.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=chart_years,
            ticktext=chart_tick_labels,
            tickangle=-30,
            showgrid=True,
            gridcolor="#F3F4F6",
            gridwidth=1,
        ),
        yaxis=dict(
            tickformat=",",
            showgrid=True,
            gridcolor="#F3F4F6",
            gridwidth=1,
            zeroline=False,
            rangemode="tozero",
        ),
        hovermode="x unified",
        showlegend=False,
        height=520,
        margin=dict(t=90, b=60, l=70, r=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Montserrat, Arial, sans-serif", size=13, color="#333333"),
    )

    st.plotly_chart(fig, use_container_width=True)

    # ── YoY change bar chart ───────────────────────────────────────────────────
    df_yoy = df.groupby("year")["completions"].sum().reset_index().sort_values("year")
    df_yoy["yoy"] = df_yoy["completions"].pct_change() * 100
    df_yoy = df_yoy.dropna(subset=["yoy"])
    df_yoy["color"] = df_yoy["yoy"].apply(lambda v: "#16a34a" if v >= 0 else "#dc2626")
    df_yoy["text"] = df_yoy["yoy"].apply(lambda v: f"+{v:.1f}%" if v >= 0 else f"{v:.1f}%")

    # Actuals bars
    fig_yoy = go.Figure(go.Bar(
        x=df_yoy["year"],
        y=df_yoy["yoy"],
        marker_color=df_yoy["color"],
        text=df_yoy["text"],
        textposition="outside",
        textfont=dict(size=10, family="Montserrat, Arial, sans-serif", color="#333333"),
        hovertemplate="%{text}<extra></extra>",
        name="Actual",
        showlegend=False,
    ))

    # Projected YoY bars (semi-transparent)
    if projection:
        last_actual = int(df_totals[all_years[-1]])
        proj_chain = [last_actual] + [p[1] for p in projection]
        proj_yoy_years = [p[0] for p in projection]
        proj_yoy_vals = [
            ((proj_chain[i + 1] - proj_chain[i]) / proj_chain[i] * 100)
            if proj_chain[i] > 0 else 0
            for i in range(len(projection))
        ]
        proj_yoy_colors = [
            "rgba(22, 163, 74, 0.35)" if v >= 0 else "rgba(220, 38, 38, 0.35)"
            for v in proj_yoy_vals
        ]
        proj_yoy_text = [
            f"+{v:.1f}%" if v >= 0 else f"{v:.1f}%" for v in proj_yoy_vals
        ]
        proj_yoy_textcolors = [
            "rgba(22, 163, 74, 0.6)" if v >= 0 else "rgba(220, 38, 38, 0.6)"
            for v in proj_yoy_vals
        ]
        fig_yoy.add_trace(go.Bar(
            x=proj_yoy_years,
            y=proj_yoy_vals,
            marker_color=proj_yoy_colors,
            text=proj_yoy_text,
            textposition="outside",
            textfont=dict(
                size=10,
                family="Montserrat, Arial, sans-serif",
                color=proj_yoy_textcolors,
            ),
            hovertemplate="%{text} (projected)<extra></extra>",
            name="Projected",
            showlegend=False,
        ))

        # Faint gray shaded region matching the main chart
        fig_yoy.add_vrect(
            x0=all_years[-1] + 0.5,
            x1=proj_yoy_years[-1] + 0.5,
            fillcolor="#E5E7EB",
            opacity=0.3,
            layer="below",
            line_width=0,
        )

    fig_yoy.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=chart_years,
            ticktext=chart_tick_labels,
            tickangle=-30,
            showgrid=True,
            gridcolor="#F3F4F6",
            gridwidth=1,
            range=[chart_years[0] - 0.5, chart_years[-1] + 0.5],
        ),
        yaxis=dict(
            ticksuffix="%",
            tickformat=".1f",
            showgrid=True,
            gridcolor="#F3F4F6",
            gridwidth=1,
            zeroline=True,
            zerolinecolor="#999999",
            zerolinewidth=1,
        ),
        title=dict(text="Year-over-Year % Change", font=dict(size=13), x=0, xanchor="left"),
        height=220,
        margin=dict(t=40, b=60, l=70, r=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Montserrat, Arial, sans-serif", size=12, color="#333333"),
        showlegend=False,
    )
    st.plotly_chart(fig_yoy, use_container_width=True)

    # ── Download CSV (completions + YoY) ──────────────────────────────────────
    if "award_level_name" in df.columns:
        download_df = df[["year", "award_level_name", "completions"]].copy()
        download_df["year"] = download_df["year"].apply(yr_label)
        download_df.columns = ["Year", "Award Level", "Completions"]
    else:
        download_df = df.groupby("year")["completions"].sum().reset_index()
        download_df["year"] = download_df["year"].apply(yr_label)
        download_df.columns = ["Year", "Completions"]

    # Merge in YoY % change
    yoy_export = df_yoy[["year", "yoy"]].copy()
    yoy_export["year"] = yoy_export["year"].apply(yr_label)
    yoy_export.columns = ["Year", "YoY % Change"]
    if "Award Level" in download_df.columns:
        # aggregate to total per year first, then merge
        totals = download_df.groupby("Year")["Completions"].sum().reset_index()
        totals = totals.merge(yoy_export, on="Year", how="left")
        download_df = download_df.merge(totals[["Year", "YoY % Change"]], on="Year", how="left")
    else:
        download_df = download_df.merge(yoy_export, on="Year", how="left")

    cip_slug = "all_programs" if all_cips else ("_".join(cip_label_to_code[l] for l in selected_cip_labels) or "completions")
    fname_safe = (
        f"ipeds_{cip_slug}"
        f"_{geo_label.replace(', ', '_').replace(' ', '_')}.csv"
    )
    st.download_button(
        "⬇️ Download CSV",
        data=download_df.to_csv(index=False),
        file_name=fname_safe,
        mime="text/csv",
    )

    # ── By Institution ────────────────────────────────────────────────────────
    st.divider()
    st.subheader("Completions by Institution")

    if df_inst.empty:
        st.info("No institution-level data for these filters.")
    else:
        # Get latest metadata per unitid (name may change across years)
        meta = (
            df_inst.sort_values("year")
            .groupby("unitid")[["instnm", "city", "stabbr", "control_name"]]
            .last()
            .reset_index()
        )

        # Pivot on unitid only so name changes don't split rows
        pivot = df_inst.pivot_table(
            index="unitid",
            columns="year",
            values="completions",
            aggfunc="sum",
            fill_value=0,
        ).reset_index()
        pivot = pivot.merge(meta, on="unitid", how="left")
        pivot.columns.name = None
        yr_cols = sorted([c for c in pivot.columns if isinstance(c, int)])

        # CAGR per institution (stored as %, e.g. 2.3 means 2.3%)
        first_col, last_col = yr_cols[0], yr_cols[-1]
        n_years = last_col - first_col
        col_3ago = last_col - 3

        def inst_cagr(row, start_col, n):
            fv, lv = row[start_col], row[last_col]
            if fv > 0 and lv > 0 and n > 0:
                return ((lv / fv) ** (1 / n) - 1) * 100
            return None

        if col_3ago in yr_cols:
            pivot["3-yr CAGR"] = pivot.apply(lambda r: inst_cagr(r, col_3ago, 3), axis=1)
        pivot["10-yr CAGR"] = pivot.apply(lambda r: inst_cagr(r, first_col, n_years), axis=1)
        pivot = pivot.rename(columns={y: yr_label_short(y) for y in yr_cols})
        last_yr_short = yr_label_short(last_col)
        pivot = pivot.sort_values(last_yr_short, ascending=False, na_position="last").reset_index(drop=True)
        control_map = {"Public": "Public", "Private nonprofit": "Private", "Private for-profit": "For-Profit"}
        pivot["control_name"] = pivot["control_name"].map(control_map).fillna(pivot["control_name"])
        pivot["city"] = pivot["city"] + ", " + pivot["stabbr"]
        pivot = pivot.drop(columns=["unitid", "stabbr"])
        pivot = pivot.rename(columns={"instnm": "Institution", "city": "City", "control_name": "Control"})
        cagr_cols = [c for c in ["3-yr CAGR", "10-yr CAGR"] if c in pivot.columns]
        yr_short_labels = [yr_label_short(y) for y in yr_cols]
        pivot = pivot[["Institution", "City", "Control"] + yr_short_labels + cagr_cols]

        n_institutions = len(pivot)
        st.caption(f"{n_institutions:,} institutions reported completions for these filters")

        # Smaller font for the institution table
        st.markdown(
            "<style>div[data-testid='stDataFrame'] table {font-size: 0.78rem;}</style>",
            unsafe_allow_html=True,
        )

        # Compute column widths so the table fits without horizontal scroll.
        n_yr = len(yr_short_labels)
        n_cagr = len(cagr_cols)
        yr_col_w = 62
        cagr_col_w = 72
        control_col_w = 68
        fixed_w = n_yr * yr_col_w + n_cagr * cagr_col_w + control_col_w
        remaining = max(400, 1100 - fixed_w)
        inst_w = int(remaining * 0.6)
        city_w = remaining - inst_w

        col_cfg = {
            "Institution": st.column_config.TextColumn("Institution", width=inst_w),
            "City": st.column_config.TextColumn("City", width=city_w),
            "Control": st.column_config.TextColumn("Control", width=control_col_w),
            **{
                yr_label_short(y): st.column_config.NumberColumn(
                    yr_label_short(y), format="%,d", width=yr_col_w,
                )
                for y in yr_cols
            },
        }
        if "3-yr CAGR" in cagr_cols:
            col_cfg["3-yr CAGR"] = st.column_config.NumberColumn(
                f"3-yr CAGR ({yr_label_short(col_3ago)} → {yr_label_short(last_col)})",
                format="%.1f%%",
                width=cagr_col_w,
            )
        if "10-yr CAGR" in cagr_cols:
            col_cfg["10-yr CAGR"] = st.column_config.NumberColumn(
                f"10-yr CAGR ({yr_label_short(first_col)} → {yr_label_short(last_col)})",
                format="%.1f%%",
                width=cagr_col_w,
            )

        st.dataframe(
            pivot,
            use_container_width=True,
            hide_index=True,
            column_config=col_cfg,
        )

        cip_slug = "all_programs" if all_cips else ("_".join(cip_label_to_code[l] for l in selected_cip_labels) or "completions")
        fname_inst = (
            f"ipeds_{cip_slug}"
            f"_{geo_label.replace(', ', '_').replace(' ', '_')}_by_institution.csv"
        )
        st.download_button(
            "⬇️ Download CSV",
            data=pivot.to_csv(index=False),
            file_name=fname_inst,
            mime="text/csv",
            key="dl_inst",
        )

    # ── Related Employment by Occupation ─────────────────────────────────────
    st.divider()
    st.subheader("Related Employment by Occupation")

    if all_cips:
        st.info(
            "Employment data is shown when specific CIP code(s) are selected. "
            "Deselect 'All CIP codes' and choose program(s) to see related occupations."
        )
    else:
        # Check if OES tables exist
        _oes_ok = False
        try:
            _conn = get_conn()
            _conn.execute("SELECT 1 FROM oes_employment LIMIT 1")
            _conn.execute("SELECT 1 FROM cip_soc_crosswalk LIMIT 1")
            _conn.close()
            _oes_ok = True
        except Exception:
            pass

        if not _oes_ok:
            st.warning(
                "Employment data not loaded. Run `python load_oes_data.py` to download "
                "BLS OES data and the CIP-SOC crosswalk."
            )
        else:
            with st.spinner("Querying employment data..."):
                df_emp = run_employment_query(
                    cip_patterns=cip_patterns,
                    awlevels=selected_awlevels,
                    geo_key=geo_key,
                    geo_values=tuple(geo_values),
                )

            if df_emp.empty:
                st.info("No employment data found for the selected program(s) and geography.")
            else:
                # Fetch projected growth for the same occupations
                soc_codes_for_proj = tuple(df_emp["occ_code"].unique())
                df_proj = get_employment_projections(
                    soc_codes=soc_codes_for_proj,
                    geo_key=geo_key,
                    geo_values=tuple(geo_values),
                )

                # Aggregate total employment across all occupations by year
                emp_by_year = df_emp.groupby("year")["tot_emp"].sum().reset_index()
                emp_by_year = emp_by_year.sort_values("year")

                latest_emp_year = df_emp["year"].max()

                # Compute weighted average projected CAGR across all related occupations
                proj_avg_cagr = None
                if not df_proj.empty and "cagr" in df_proj.columns:
                    # Weight by latest-year employment
                    latest_emp = df_emp[df_emp["year"] == latest_emp_year][["occ_code", "tot_emp"]]
                    proj_merged = df_proj.merge(latest_emp, on="occ_code", how="inner")
                    proj_merged = proj_merged.dropna(subset=["cagr", "tot_emp"])
                    if not proj_merged.empty and proj_merged["tot_emp"].sum() > 0:
                        proj_avg_cagr = (
                            (proj_merged["cagr"] * proj_merged["tot_emp"]).sum()
                            / proj_merged["tot_emp"].sum()
                        )

                # Employment metrics
                emp_years = sorted(emp_by_year["year"].unique())
                if len(emp_years) >= 2:
                    emp_latest = int(emp_by_year[emp_by_year["year"] == emp_years[-1]]["tot_emp"].iloc[0])
                    emp_first = int(emp_by_year[emp_by_year["year"] == emp_years[0]]["tot_emp"].iloc[0])
                    emp_n = emp_years[-1] - emp_years[0]
                    emp_cagr = ((emp_latest / emp_first) ** (1 / emp_n) - 1) if emp_first > 0 and emp_n > 0 else None

                    # Median wage
                    latest_wages = df_emp[df_emp["year"] == latest_emp_year]
                    wage_weighted = latest_wages.dropna(subset=["a_median", "tot_emp"])
                    if not wage_weighted.empty:
                        avg_median_wage = int(
                            (wage_weighted["a_median"] * wage_weighted["tot_emp"]).sum()
                            / wage_weighted["tot_emp"].sum()
                        )
                    else:
                        avg_median_wage = None

                    n_occs = df_emp["occ_code"].nunique()

                    # 3-year CAGR (mirrors completions section)
                    emp_3yr_cagr = None
                    emp_yr_3ago = latest_emp_year - 3
                    if emp_yr_3ago in emp_by_year["year"].values:
                        emp_3ago_val = int(emp_by_year[emp_by_year["year"] == emp_yr_3ago]["tot_emp"].iloc[0])
                        if emp_3ago_val > 0:
                            emp_3yr_cagr = (emp_latest / emp_3ago_val) ** (1 / 3) - 1

                    em1, em2, em3, em4, em5 = st.columns(5)
                    em1.metric(
                        f"{latest_emp_year} Related Employment",
                        f"{emp_latest:,}",
                    )
                    em2.metric(
                        f"10-yr CAGR ({emp_years[0]} → {emp_years[-1]})",
                        f"{emp_cagr:+.1%}" if emp_cagr is not None else "N/A",
                    )
                    em3.metric(
                        f"3-yr CAGR ({emp_yr_3ago} → {latest_emp_year})",
                        f"{emp_3yr_cagr:+.1%}" if emp_3yr_cagr is not None else "N/A",
                    )
                    em4.metric(
                        f"Projected CAGR ({latest_emp_year} → 2029)",
                        f"{proj_avg_cagr:+.1%}" if proj_avg_cagr is not None else "N/A",
                    )
                    em5.metric(
                        f"Wtd. Median Wage ({latest_emp_year})",
                        f"${avg_median_wage:,}" if avg_median_wage else "N/A",
                    )

                # Line chart: aggregated employment across all related occupations
                if not emp_by_year.empty:
                    fig_emp = go.Figure()
                    # Historical line (solid) with data labels
                    fig_emp.add_trace(go.Scatter(
                        x=emp_by_year["year"],
                        y=emp_by_year["tot_emp"],
                        mode="lines+markers+text",
                        name="Total Related Employment",
                        line=dict(width=2.5, color=EMPLOYMENT_COLORS[0]),
                        marker=dict(size=7),
                        textposition="top center",
                        texttemplate="%{y:,.0f}",
                        textfont=dict(size=10),
                        hovertemplate="<b>%{x}</b><br>%{y:,.0f} employed<extra></extra>",
                    ))

                    # Add dotted projection line using weighted avg CAGR
                    emp_tick_years = sorted(emp_by_year["year"].unique())
                    if proj_avg_cagr is not None:
                        proj_target_year = 2029
                        base_val = emp_by_year[
                            emp_by_year["year"] == latest_emp_year
                        ]["tot_emp"].iloc[0]
                        proj_years = list(range(latest_emp_year, proj_target_year + 1))
                        proj_vals = [
                            base_val * (1 + proj_avg_cagr) ** (y - latest_emp_year)
                            for y in proj_years
                        ]

                        # Faint gray shaded region over the projection area
                        fig_emp.add_vrect(
                            x0=latest_emp_year + 0.5,
                            x1=proj_target_year + 0.5,
                            fillcolor="#E5E7EB",
                            opacity=0.3,
                            layer="below",
                            line_width=0,
                        )

                        # Projection line with diamond markers and data labels
                        fig_emp.add_trace(go.Scatter(
                            x=[latest_emp_year] + proj_years[1:],
                            y=[base_val] + proj_vals[1:],
                            mode="lines+markers+text",
                            name="Projected",
                            line=dict(dash="dash", width=2.5, color="rgba(15, 134, 193, 0.45)"),
                            marker=dict(size=7, symbol="diamond", color="rgba(15, 134, 193, 0.45)"),
                            text=[""] + [f"{int(v):,}" for v in proj_vals[1:]],
                            textposition="top center",
                            textfont=dict(size=10, color="rgba(15, 134, 193, 0.6)"),
                            hovertemplate="<b>%{x} (projected)</b><br>%{y:,.0f} employed<extra></extra>",
                        ))
                        emp_tick_years = sorted(set(emp_tick_years) | set(proj_years[1:]))

                    fig_emp.update_layout(
                        title="<b>Employment Trend: All Related Occupations</b>",
                        xaxis=dict(
                            title="",
                            tickmode="array",
                            tickvals=emp_tick_years,
                            ticktext=[str(y) for y in emp_tick_years],
                            tickangle=-30,
                            showgrid=True,
                            gridcolor="#F3F4F6",
                        ),
                        yaxis=dict(
                            title="Total Employment",
                            tickformat=",",
                            showgrid=True,
                            gridcolor="#F3F4F6",
                            rangemode="tozero",
                        ),
                        showlegend=False,
                        height=480,
                        margin=dict(t=80, b=60, l=70, r=20),
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Montserrat, Arial, sans-serif", size=12, color="#333333"),
                    )
                    st.plotly_chart(fig_emp, use_container_width=True)

                    # ── Employment YoY change bar chart ───────────────────────
                    emp_yoy = emp_by_year.copy().sort_values("year")
                    emp_yoy["yoy"] = emp_yoy["tot_emp"].pct_change() * 100
                    emp_yoy = emp_yoy.dropna(subset=["yoy"])
                    emp_yoy["color"] = emp_yoy["yoy"].apply(
                        lambda v: "#16a34a" if v >= 0 else "#dc2626"
                    )
                    emp_yoy["text"] = emp_yoy["yoy"].apply(
                        lambda v: f"+{v:.1f}%" if v >= 0 else f"{v:.1f}%"
                    )

                    fig_emp_yoy = go.Figure(go.Bar(
                        x=emp_yoy["year"],
                        y=emp_yoy["yoy"],
                        marker_color=emp_yoy["color"],
                        text=emp_yoy["text"],
                        textposition="outside",
                        textfont=dict(size=10, family="Montserrat, Arial, sans-serif", color="#333333"),
                        hovertemplate="%{text}<extra></extra>",
                        name="Actual",
                        showlegend=False,
                    ))

                    # Projected YoY bars
                    if proj_avg_cagr is not None:
                        proj_chain = [base_val] + proj_vals[1:]
                        proj_yoy_years = proj_years[1:]
                        proj_yoy_vals = [
                            ((proj_chain[i + 1] - proj_chain[i]) / proj_chain[i] * 100)
                            if proj_chain[i] > 0 else 0
                            for i in range(len(proj_chain) - 1)
                        ]
                        proj_yoy_colors = [
                            "rgba(22, 163, 74, 0.35)" if v >= 0 else "rgba(220, 38, 38, 0.35)"
                            for v in proj_yoy_vals
                        ]
                        proj_yoy_text = [
                            f"+{v:.1f}%" if v >= 0 else f"{v:.1f}%" for v in proj_yoy_vals
                        ]
                        proj_yoy_textcolors = [
                            "rgba(22, 163, 74, 0.6)" if v >= 0 else "rgba(220, 38, 38, 0.6)"
                            for v in proj_yoy_vals
                        ]
                        fig_emp_yoy.add_trace(go.Bar(
                            x=proj_yoy_years,
                            y=proj_yoy_vals,
                            marker_color=proj_yoy_colors,
                            text=proj_yoy_text,
                            textposition="outside",
                            textfont=dict(
                                size=10,
                                family="Montserrat, Arial, sans-serif",
                                color=proj_yoy_textcolors,
                            ),
                            hovertemplate="%{text} (projected)<extra></extra>",
                            name="Projected",
                            showlegend=False,
                        ))

                        fig_emp_yoy.add_vrect(
                            x0=latest_emp_year + 0.5,
                            x1=proj_yoy_years[-1] + 0.5,
                            fillcolor="#E5E7EB",
                            opacity=0.3,
                            layer="below",
                            line_width=0,
                        )

                    emp_yoy_tick_years = sorted(set(emp_tick_years) | set(emp_yoy["year"].unique()))
                    if proj_avg_cagr is not None:
                        emp_yoy_tick_years = sorted(set(emp_yoy_tick_years) | set(proj_yoy_years))

                    fig_emp_yoy.update_layout(
                        xaxis=dict(
                            tickmode="array",
                            tickvals=emp_yoy_tick_years,
                            ticktext=[str(y) for y in emp_yoy_tick_years],
                            tickangle=-30,
                            showgrid=True,
                            gridcolor="#F3F4F6",
                            gridwidth=1,
                            range=[emp_yoy_tick_years[0] - 0.5, emp_yoy_tick_years[-1] + 0.5],
                        ),
                        yaxis=dict(
                            ticksuffix="%",
                            tickformat=".1f",
                            showgrid=True,
                            gridcolor="#F3F4F6",
                            gridwidth=1,
                            zeroline=True,
                            zerolinecolor="#999999",
                            zerolinewidth=1,
                        ),
                        title=dict(text="Year-over-Year % Change", font=dict(size=13), x=0, xanchor="left"),
                        height=220,
                        margin=dict(t=40, b=60, l=70, r=20),
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Montserrat, Arial, sans-serif", size=12, color="#333333"),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_emp_yoy, use_container_width=True)

                # Occupation detail table
                occ_pivot = df_emp.pivot_table(
                    index=["occ_code", "occ_title"],
                    columns="year",
                    values="tot_emp",
                    aggfunc="sum",
                ).reset_index()
                occ_pivot.columns.name = None

                # Add latest median wage
                wage_df = df_emp[df_emp["year"] == latest_emp_year][["occ_code", "a_median"]].drop_duplicates("occ_code")
                occ_pivot = occ_pivot.merge(wage_df, on="occ_code", how="left")

                emp_yr_cols = sorted([c for c in occ_pivot.columns if isinstance(c, (int, np.integer))])

                # Historical CAGRs for each occupation (3-yr and 10-yr)
                ey_first = emp_yr_cols[0] if emp_yr_cols else None
                ey_last = emp_yr_cols[-1] if emp_yr_cols else None
                ey_3ago = ey_last - 3 if ey_last else None
                if len(emp_yr_cols) >= 2:
                    en = ey_last - ey_first

                    def _occ_cagr(row, start_col, n):
                        fv, lv = row.get(start_col), row.get(ey_last)
                        if fv and lv and fv > 0 and lv > 0 and n > 0:
                            return ((lv / fv) ** (1 / n) - 1) * 100
                        return None

                    if ey_3ago in emp_yr_cols:
                        occ_pivot["3-yr CAGR"] = occ_pivot.apply(
                            lambda r: _occ_cagr(r, ey_3ago, 3), axis=1)
                    occ_pivot["10-yr CAGR"] = occ_pivot.apply(
                        lambda r: _occ_cagr(r, ey_first, en), axis=1)

                # Projected CAGR from employment_projections table
                if not df_proj.empty:
                    proj_cagr_df = df_proj[["occ_code", "cagr"]].copy()
                    proj_cagr_df = proj_cagr_df.rename(columns={"cagr": "Proj. CAGR"})
                    proj_cagr_df["Proj. CAGR"] = proj_cagr_df["Proj. CAGR"] * 100  # to percent
                    occ_pivot = occ_pivot.merge(proj_cagr_df, on="occ_code", how="left")

                # Sort by latest year employment
                if emp_yr_cols:
                    occ_pivot = occ_pivot.sort_values(emp_yr_cols[-1], ascending=False, na_position="last")

                # Format columns
                occ_pivot = occ_pivot.rename(columns={
                    "occ_title": "Occupation",
                    "a_median": f"Median Wage ({latest_emp_year})",
                })
                occ_pivot = occ_pivot.drop(columns=["occ_code"])

                wage_label = f"Median Wage ({latest_emp_year})"
                display_cols = ["Occupation"]
                if wage_label in occ_pivot.columns:
                    display_cols.append(wage_label)
                display_cols += emp_yr_cols
                for _cagr_col in ["3-yr CAGR", "10-yr CAGR", "Proj. CAGR"]:
                    if _cagr_col in occ_pivot.columns:
                        display_cols.append(_cagr_col)

                occ_pivot = occ_pivot[[c for c in display_cols if c in occ_pivot.columns]]

                # Format year columns as comma-separated strings for display
                for _yc in emp_yr_cols:
                    occ_pivot[_yc] = occ_pivot[_yc].apply(
                        lambda v: f"{int(v):,}" if pd.notna(v) else ""
                    )

                # Format wage column
                wage_col = f"Median Wage ({latest_emp_year})"
                if wage_col in occ_pivot.columns:
                    occ_pivot[wage_col] = occ_pivot[wage_col].apply(
                        lambda v: f"${int(v):,}" if pd.notna(v) else ""
                    )

                st.caption(f"{len(occ_pivot):,} related occupations (SOC codes mapped via CIP-SOC crosswalk)")

                emp_col_cfg = {
                    "Occupation": st.column_config.TextColumn("Occupation", width=280),
                }
                for y in emp_yr_cols:
                    emp_col_cfg[y] = st.column_config.TextColumn(str(y), width=82)
                if "3-yr CAGR" in occ_pivot.columns:
                    emp_col_cfg["3-yr CAGR"] = st.column_config.NumberColumn(
                        f"3-yr CAGR ({ey_3ago} → {ey_last})",
                        format="%.1f%%", width=90,
                    )
                if "10-yr CAGR" in occ_pivot.columns:
                    emp_col_cfg["10-yr CAGR"] = st.column_config.NumberColumn(
                        f"10-yr CAGR ({ey_first} → {ey_last})",
                        format="%.1f%%", width=90,
                    )
                if "Proj. CAGR" in occ_pivot.columns:
                    emp_col_cfg["Proj. CAGR"] = st.column_config.NumberColumn(
                        "Proj. CAGR",
                        format="%.1f%%", width=90,
                    )

                st.dataframe(
                    occ_pivot,
                    use_container_width=True,
                    hide_index=True,
                    column_config=emp_col_cfg,
                )

                # Download CSV
                emp_fname = f"employment_{cip_slug}_{geo_label.replace(', ', '_').replace(' ', '_')}.csv"
                st.download_button(
                    "⬇️ Download CSV",
                    data=occ_pivot.to_csv(index=False),
                    file_name=emp_fname,
                    mime="text/csv",
                    key="dl_emp",
                )


if __name__ == "__main__":
    main()
