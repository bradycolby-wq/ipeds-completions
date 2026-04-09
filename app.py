"""
IPEDS Completions Explorer
Streamlit app — academic years 2014-15 through 2023-24
"""

import sqlite3
import urllib.request
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests as _requests
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from plotly.subplots import make_subplots
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
    "download/v1.6/ipeds.db"
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
    version_file = cache_dir / "db_version.txt"

    # Re-download when the release URL changes (new version uploaded)
    current_version = _GITHUB_DB_URL
    cached_version = version_file.read_text().strip() if version_file.exists() else ""

    if not cached.exists() or cached_version != current_version:
        with st.spinner("Downloading database (~600 MB) — this takes ~60 seconds on first launch..."):
            urllib.request.urlretrieve(_GITHUB_DB_URL, cached)
            version_file.write_text(current_version)

    return cached


DB_PATH = _get_db_path()

st.set_page_config(
    page_title="IPEDS Completions Explorer",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Authentication gate ──────────────────────────────────────────────────────
ALLOWED_DOMAIN = "validatedinsights.com"

if not st.user.is_logged_in:
    st.header("IPEDS Completions Explorer")
    st.write("Please sign in with your @validatedinsights.com Google account.")
    if st.button("🔐 Sign in with Google"):
        st.login()
    st.stop()

if not st.user.email or not st.user.email.lower().endswith(f"@{ALLOWED_DOMAIN}"):
    st.error(
        f"Access restricted to @{ALLOWED_DOMAIN} accounts. "
        f"You are signed in as {st.user.email}."
    )
    if st.button("Sign out"):
        st.logout()
    st.stop()

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
    2.  Project shares forward using recent-weighted linear trend (last 5 years,
        recency-weighted) to preserve current momentum.
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

    # ── Project shares (recent-weighted linear trend) ──────────────────────
    # Use the most recent 5 years of share data with recency weighting
    # so the projection preserves current momentum (both growth and decline)
    # rather than dampening it like Holt exponential smoothing does.
    recent_n = min(5, len(shares))
    recent_shares = shares[-recent_n:]
    if recent_shares.std() < 1e-10:
        proj_shares = np.full(n_forward, shares[-1])
    else:
        x = np.arange(recent_n)
        weights = np.linspace(0.5, 1.0, recent_n)  # 2× weight on most recent
        slope = np.polyfit(x, recent_shares, 1, w=weights)[0]
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


def compute_emp_cagr_projection(sel_dict: dict, emp_cagr: float | None, n_forward: int = 5):
    """Project completions using employment CAGR blended with recent momentum.

    Blends the BLS employment-projection CAGR with the selection's own recent
    3-year completions CAGR so the projection incorporates actual field-level
    momentum.  Near-term years lean toward completions momentum (60 %);
    later years decay toward the employment rate (70 %).
    Returns list[(year, projected_completions)] or None.
    """
    if emp_cagr is None:
        return None
    years = sorted(sel_dict.keys())
    if not years:
        return None
    last_year = years[-1]
    last_val = sel_dict[last_year]
    if last_val <= 0:
        return None

    # Recent 3-year completions CAGR
    idx_3 = max(0, len(years) - 4)
    val_3ago = sel_dict[years[idx_3]]
    n_yrs = last_year - years[idx_3]
    if val_3ago > 0 and n_yrs > 0:
        recent_cagr = (last_val / val_3ago) ** (1 / n_yrs) - 1
    else:
        recent_cagr = emp_cagr

    proj_years = list(range(last_year + 1, last_year + n_forward + 1))
    result = []
    for i, y in enumerate(proj_years):
        # Decay from 60 % recent / 40 % emp → 30 % recent / 70 % emp
        w = max(0.30, 0.60 - 0.075 * i)
        rate = w * recent_cagr + (1 - w) * emp_cagr
        projected = last_val * (1 + rate) ** (i + 1)
        result.append((y, max(int(round(projected)), 0)))
    return result


def compute_blended_projection(nces_proj, emp_proj, weight_nces: float = 0.5):
    """Blend NCES-constrained and employment-CAGR projections (50/50 average).

    Both inputs are list[(year, value)]. Returns list[(year, blended_value)] or None.
    """
    if not nces_proj or not emp_proj:
        return None
    nces_d = dict(nces_proj)
    emp_d = dict(emp_proj)
    common = sorted(set(nces_d) & set(emp_d))
    if not common:
        return None
    w = weight_nces
    return [
        (y, max(int(round(w * nces_d[y] + (1 - w) * emp_d[y])), 0))
        for y in common
    ]


def apply_capacity_adjustment(projection, program_counts: dict):
    """Adjust projection values by the trend in number of programs offered.

    Computes the 3-year CAGR of program counts and applies it as a
    multiplicative capacity factor: if programs are growing, projections
    are nudged up; if programs are shrinking, projections are nudged down.

    Returns (adjusted_projection, capacity_cagr) or (projection, None) if
    insufficient data.
    """
    if not projection or not program_counts:
        return projection, None

    years = sorted(program_counts.keys())
    if len(years) < 4:
        return projection, None

    last_year = years[-1]
    last_count = program_counts[last_year]
    yr_3ago = last_year - 3
    count_3ago = program_counts.get(yr_3ago)

    if not count_3ago or count_3ago <= 0 or last_count <= 0:
        return projection, None

    cap_cagr = (last_count / count_3ago) ** (1 / 3) - 1

    # Only adjust if the change is meaningful (>0.5%/yr abs)
    if abs(cap_cagr) < 0.005:
        return projection, cap_cagr

    adjusted = []
    for y, val in projection:
        n = y - last_year
        factor = (1 + cap_cagr) ** n
        adjusted.append((y, max(int(round(val * factor)), 0)))
    return adjusted, cap_cagr


def compute_unified_projection(
    sel_dict: dict,
    emp_cagr: float | None,
    program_counts: dict | None,
    n_forward: int = 5,
):
    """Single unified projection blending trend regression, employment growth,
    and program capacity.

    Components (weighted):
      1. Recent-weighted linear regression of completions (base trend)
      2. Employment CAGR from BLS projections (demand signal)
      3. Program count CAGR (capacity signal)

    Returns (list[(year, value)], dict with component info) or (None, {}).
    """
    years = sorted(sel_dict.keys())
    if len(years) < 3:
        return None, {}

    last_year = years[-1]
    last_val = sel_dict[last_year]
    if last_val <= 0:
        return None, {}

    proj_years = list(range(last_year + 1, last_year + n_forward + 1))

    # ── Component 1: Recent-weighted linear regression CAGR ───────────────
    recent_n = min(5, len(years))
    recent_years = years[-recent_n:]
    recent_vals = np.array([sel_dict[y] for y in recent_years], dtype=float)
    x = np.arange(recent_n)
    weights = np.linspace(0.5, 1.0, recent_n)
    slope = np.polyfit(x, recent_vals, 1, w=weights)[0]
    # Convert slope to an approximate CAGR
    mid_val = recent_vals.mean()
    trend_cagr = slope / mid_val if mid_val > 0 else 0.0

    # ── Component 2: Employment CAGR ──────────────────────────────────────
    has_emp = emp_cagr is not None

    # ── Component 3: Program capacity CAGR ────────────────────────────────
    cap_cagr = None
    if program_counts and len(program_counts) >= 4:
        pc_years = sorted(program_counts.keys())
        pc_last = program_counts[pc_years[-1]]
        yr3 = pc_years[-1] - 3
        pc_3ago = program_counts.get(yr3)
        if pc_3ago and pc_3ago > 0 and pc_last > 0:
            cap_cagr = (pc_last / pc_3ago) ** (1 / 3) - 1

    # ── Blend into a single growth rate ───────────────────────────────────
    # Assign weights based on what's available:
    #   - trend always gets weight
    #   - employment gets weight if available
    #   - capacity gets weight if available and meaningful
    components = {"trend": trend_cagr}
    w_trend = 0.50
    w_emp = 0.0
    w_cap = 0.0

    if has_emp and cap_cagr is not None and abs(cap_cagr) >= 0.005:
        # All three available
        w_trend = 0.40
        w_emp = 0.35
        w_cap = 0.25
        components["employment"] = emp_cagr
        components["capacity"] = cap_cagr
    elif has_emp:
        # Trend + employment
        w_trend = 0.55
        w_emp = 0.45
        components["employment"] = emp_cagr
    elif cap_cagr is not None and abs(cap_cagr) >= 0.005:
        # Trend + capacity
        w_trend = 0.65
        w_cap = 0.35
        components["capacity"] = cap_cagr

    blended_rate = w_trend * trend_cagr + w_emp * (emp_cagr or 0) + w_cap * (cap_cagr or 0)
    components["blended_rate"] = blended_rate
    components["weights"] = {
        k: v for k, v in [("trend", w_trend), ("employment", w_emp), ("capacity", w_cap)] if v > 0
    }

    # ── Project forward ───────────────────────────────────────────────────
    result = []
    for i, y in enumerate(proj_years):
        projected = last_val * (1 + blended_rate) ** (i + 1)
        result.append((y, max(int(round(projected)), 0)))

    return result, components


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
def run_program_count_query(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Count distinct programs (institutions) reporting ≥1 completion per year.

    A 'program' = unique unitid offering the selected CIP/award-level with
    at least one completion. Returns dict {year: program_count}.
    """
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
        SELECT year, COUNT(DISTINCT unitid) AS program_count
        FROM completions_view
        {where_sql}
        GROUP BY year
        ORDER BY year
    """

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return dict(zip(df["year"], df["program_count"]))


@st.cache_data(show_spinner=False, ttl=600)
def run_distance_ed_query(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """DE program counts and DE completions per year.

    Joins completions with distance_ed_status (from IPEDS IC survey) on
    (year, unitid) to identify institutions that offer distance education
    PROGRAMS (distpgs = 1).

    Using distnced = 1 was too restrictive (only exclusively-online
    institutions, ~2% of completions).  Using distnced IN (1, 2) captured
    nearly every institution (~100%).  The distpgs flag is the middle
    ground: institutions that offer at least some fully-online programs
    (~57% of completions).

    Only includes programs with at least 1 completion (ctotalt > 0).

    Returns dict with 'de_program_counts' and 'de_completions' keyed by year,
    or None if distance_ed_status table doesn't exist / no data.
    """
    cip_patterns = expand_cip_patterns(cip_patterns)

    conn = get_conn()

    # Gracefully handle missing table
    try:
        conn.execute("SELECT 1 FROM distance_ed_status LIMIT 1")
    except Exception:
        conn.close()
        return None

    params = []
    where = [
        "c.majornum = 1",
        "c.ctotalt IS NOT NULL",
        "c.ctotalt > 0",
        "d.distpgs = 1",
    ]

    if cip_patterns:
        cip_clauses = []
        for p in cip_patterns:
            cip_clauses.append("c.cipcode LIKE ?" if "%" in p else "c.cipcode = ?")
            params.append(p)
        where.append(f"({' OR '.join(cip_clauses)})")

    if awlevels:
        placeholders = ",".join("?" * len(awlevels))
        where.append(f"c.awlevel IN ({placeholders})")
        params.extend(awlevels)

    # Geography requires joining institutions
    geo_join = ""
    if geo_key == "state" and geo_values:
        geo_join = "INNER JOIN institutions i ON c.unitid = i.unitid AND c.year = i.year"
        placeholders = ",".join("?" * len(geo_values))
        where.append(f"i.stabbr IN ({placeholders})")
        params.extend(geo_values)
    elif geo_key == "metro" and geo_values:
        geo_join = "INNER JOIN institutions i ON c.unitid = i.unitid AND c.year = i.year"
        placeholders = ",".join("?" * len(geo_values))
        where.append(f"i.cbsa IN ({placeholders})")
        params.extend(geo_values)

    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT
            c.year,
            COUNT(DISTINCT c.unitid) AS de_program_count,
            SUM(c.ctotalt)           AS de_completions
        FROM completions c
        INNER JOIN distance_ed_status d
            ON c.year = d.year
            AND c.unitid = d.unitid
        {geo_join}
        {where_sql}
        GROUP BY c.year
        ORDER BY c.year
    """

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()

    if df.empty:
        return None

    return {
        "de_program_counts": dict(zip(df["year"], df["de_program_count"])),
        "de_completions": dict(zip(df["year"], df["de_completions"])),
    }


@st.cache_data(show_spinner=False, ttl=600)
def run_dep_query(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Distance education program counts from completions_dep table.

    Returns DataFrame with columns:
      year, programs, programs_de, programs_de_some, programs_de_any
    aggregated across matching CIPs, award levels, and geography.
    Or None if completions_dep table doesn't exist / no data.

    Uses 6-digit CIP codes from completions_dep (filters out 2-digit
    summary rows to avoid double-counting). Joins institutions table
    for geographic filtering.
    """
    cip_patterns = expand_cip_patterns(cip_patterns)

    conn = get_conn()
    try:
        conn.execute("SELECT 1 FROM completions_dep LIMIT 1")
    except Exception:
        conn.close()
        return None

    params = []
    where = [
        "d.programs > 0",
        "LENGTH(d.cipcode) >= 5",  # exclude 2-digit summary rows
    ]

    if cip_patterns:
        cip_clauses = []
        for p in cip_patterns:
            cip_clauses.append(
                "d.cipcode LIKE ?" if "%" in p else "d.cipcode = ?"
            )
            params.append(p)
        where.append(f"({' OR '.join(cip_clauses)})")

    if awlevels:
        placeholders = ",".join("?" * len(awlevels))
        where.append(f"d.awlevel IN ({placeholders})")
        params.extend(awlevels)

    geo_join = ""
    if geo_key == "state" and geo_values:
        geo_join = (
            "INNER JOIN institutions i "
            "ON d.unitid = i.unitid AND d.year = i.year"
        )
        placeholders = ",".join("?" * len(geo_values))
        where.append(f"i.stabbr IN ({placeholders})")
        params.extend(geo_values)
    elif geo_key == "metro" and geo_values:
        geo_join = (
            "INNER JOIN institutions i "
            "ON d.unitid = i.unitid AND d.year = i.year"
        )
        placeholders = ",".join("?" * len(geo_values))
        where.append(f"i.cbsa IN ({placeholders})")
        params.extend(geo_values)

    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT
            d.year,
            SUM(d.programs)         AS programs,
            SUM(d.programs_de)      AS programs_de,
            SUM(d.programs_de_some) AS programs_de_some,
            SUM(d.programs_de_any)  AS programs_de_any
        FROM completions_dep d
        {geo_join}
        {where_sql}
        GROUP BY d.year
        ORDER BY d.year
    """

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()

    if df.empty:
        return None

    # Compute percentage columns
    df["pct_de_any"] = (
        100.0 * df["programs_de_any"] / df["programs"]
    ).round(1).where(df["programs"] > 0)

    return df


_CORESIGNAL_BASE = "https://api.coresignal.com/cdapi/v2/job_base"

# Generic SOC titles to exclude from Coresignal searches (too broad / noisy)
_GENERIC_SOC_TITLES = {
    "Managers, All Other",
    "Teachers and Instructors, All Other, Except Substitute Teachers",
    "First-Line Supervisors of Office and Administrative Support Workers",
    "Postsecondary Teachers",
    "Education Administrators, Postsecondary",
    "Education Administrators, All Other",
}


def _resolve_coresignal_titles(cip_patterns: tuple, awlevels: tuple) -> list:
    """Map CIP codes to simplified occupation titles for Coresignal searches."""
    conn = get_conn()
    try:
        conn.execute("SELECT 1 FROM cip_soc_crosswalk LIMIT 1")
    except Exception:
        conn.close()
        return []

    params = []
    cip_clauses = []
    for p in cip_patterns:
        cip_clauses.append("cipcode LIKE ?" if "%" in p else "cipcode = ?")
        params.append(p)

    awlevel_filter = ""
    if awlevels:
        undergrad = any(a in (1, 2, 3, 4, 5) for a in awlevels)
        grad = any(a in (6, 7, 8, 17, 18, 19) for a in awlevels)
        if undergrad and not grad:
            awlevel_filter = "AND awlevel_group = 'undergrad'"
        elif grad and not undergrad:
            awlevel_filter = "AND awlevel_group = 'graduate'"

    sql = f"""
        SELECT DISTINCT soc_title
        FROM cip_soc_crosswalk
        WHERE ({' OR '.join(cip_clauses)}) {awlevel_filter}
        ORDER BY soc_title
    """
    soc_titles = [r[0] for r in conn.execute(sql, params).fetchall()]
    conn.close()

    soc_titles = [t for t in soc_titles if t not in _GENERIC_SOC_TITLES]
    simplified = []
    for t in soc_titles:
        t = t.split(",")[0].strip()
        if t.endswith("s") and not t.endswith("ss"):
            t = t[:-1]
        if t and t not in simplified:
            simplified.append(t)
    return simplified[:3]


@st.cache_data(show_spinner=False, ttl=3600)
def run_coresignal_trend(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Query Coresignal for monthly job posting counts over the last 12 months.

    Uses the x-total-pages response header to get exact totals without
    paginating (1 credit per month per title). Also collects a small sample
    of recent postings for a detail table.

    Returns dict with 'trend_df', 'current_active', and 'search_titles',
    or None if the API key is missing or no results found.
    """
    from datetime import datetime, timedelta
    import calendar

    api_key = st.secrets["coresignal"]["api_key"] if "coresignal" in st.secrets else ""
    if not api_key:
        return None

    cip_patterns = expand_cip_patterns(cip_patterns)
    search_titles = _resolve_coresignal_titles(cip_patterns, awlevels)
    if not search_titles:
        return None

    headers = {"accept": "application/json", "apikey": api_key, "Content-Type": "application/json"}
    session = _requests.Session()
    session.headers.update(headers)

    # Resolve geography to Coresignal-compatible location strings.
    # States: use abbreviations directly (e.g., "TX"). Metro filtering not supported.
    cs_locations = []  # empty = national (no location filter)
    if geo_key == "state" and geo_values:
        cs_locations = list(geo_values)

    def _search_total(body: dict) -> int:
        """Run a search/filter call and return estimated total from headers."""
        try:
            resp = session.post(
                f"{_CORESIGNAL_BASE}/search/filter", json=body, timeout=30
            )
            if resp.status_code == 200:
                total_pages = int(resp.headers.get("x-total-pages", 1))
                items_per_page = int(resp.headers.get("x-items-per-page", 1000))
                return total_pages * items_per_page
        except Exception:
            pass
        return 0

    def _query_postings(extra_filters: dict) -> int:
        """Sum posting counts across all search titles and locations."""
        total = 0
        locs = cs_locations or [None]  # None = no location filter
        for title in search_titles:
            for loc in locs:
                body = {"title": title, "country": "United States", **extra_filters}
                if loc is not None:
                    body["location"] = loc
                total += _search_total(body)
        return total

    # Build list of last 12 months
    today = datetime.now()
    months = []
    for i in range(11, -1, -1):
        dt = today.replace(day=1) - timedelta(days=i * 30)
        dt = dt.replace(day=1)
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        end_day = today.day if (dt.year == today.year and dt.month == today.month) else last_day
        months.append({
            "label": dt.strftime("%Y-%m"),
            "gte": f"{dt.year}-{dt.month:02d}-01 00:00:00",
            "lte": f"{dt.year}-{dt.month:02d}-{end_day:02d} 23:59:59",
        })

    # Query posting counts per month
    trend_rows = []
    for m in months:
        month_total = _query_postings({
            "created_at_gte": m["gte"],
            "created_at_lte": m["lte"],
        })
        trend_rows.append({"month": m["label"], "postings": month_total})

    trend_df = pd.DataFrame(trend_rows)

    if trend_df["postings"].sum() == 0:
        return "empty"

    # Get current active postings count
    current_active = _query_postings({"application_active": True})

    return {
        "trend_df": trend_df,
        "current_active": current_active,
        "search_titles": search_titles,
    }


@st.cache_data(show_spinner=False, ttl=600)
def run_employment_query(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Query OES employment data for occupations related to selected CIP codes.

    Handles SOC version differences:
      - 2015-2018 OES data uses SOC 2010 codes
      - 2019-2024 OES data uses SOC 2018 codes
      - Some 2019-2020 data uses BLS combined codes (e.g. 15-1256)
        that are remapped to SOC 2018 detail codes
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
    # For many-to-many mappings, prefer the lowest SOC 2018 code
    # (BLS assigns lower codes to primary occupations, e.g. 15-1252
    # Software Developers before 15-1253 Software QA)
    soc_2010_to_2018_map: dict[str, str] = {}
    for s2010, s2018 in sorted(soc_2010_rows, key=lambda x: x[1]):
        if s2010 not in soc_2010_to_2018_map:
            soc_2010_to_2018_map[s2010] = s2018
    # Override: "All Other" codes (XX-XX99) should map to their "All Other"
    # successor, not inflate a specific occupation (e.g. 15-1199 -> 15-1299)
    for s2010, s2018 in soc_2010_rows:
        if s2010[-2:] == "99" and s2018[-2:] == "99" and s2010[:3] == s2018[:3]:
            soc_2010_to_2018_map[s2010] = s2018

    # 2a. Handle BLS combined codes (2019-2020)
    # BLS sometimes publishes combined codes when detail codes can't be
    # separately disclosed. Include them and remap to detail codes.
    _COMBINED_SOC = {
        "15-1245": {"15-1242", "15-1243"},  # DB Admins + Architects
        "15-1256": {"15-1252", "15-1253"},  # Software Devs + QA
        "15-1257": {"15-1254", "15-1255"},  # Web Devs + Designers
        "15-2098": {"15-2051", "15-2099"},  # Data Scientists + Math Sci Other
    }
    combined_remap = {}  # combined_code -> detail_code to remap to
    for combined, details in _COMBINED_SOC.items():
        overlap = details & set(soc_2018_codes)
        if overlap:
            combined_remap[combined] = sorted(overlap)[0]
    all_soc_2018 = list(set(soc_2018_codes) | set(combined_remap.keys()))

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

    # 4. Query: UNION of SOC 2018 data (2019+) and SOC 2010 data (2015-2018)
    # For 2019+ data, use SOC 2018 codes (+ any BLS combined codes)
    soc_ph_2018 = ",".join("?" * len(all_soc_2018))
    params_2018 = all_soc_2018 + area_params

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
        WHERE year >= 2019
          AND occ_code IN ({soc_ph_2018})
          {area_where}
        GROUP BY year, occ_code, occ_title
    """

    # For pre-2019 data (2015-2018), use SOC 2010 codes and map to 2018
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
            WHERE year < 2019
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

    # Remap BLS combined codes to their detail equivalents
    if combined_remap:
        result["occ_code"] = result["occ_code"].map(
            lambda x: combined_remap.get(x, x)
        )
        # Re-aggregate after remapping (combined code may merge with detail code)
        result = result.groupby(["year", "occ_code"]).agg({
            "occ_title": "first",
            "tot_emp": "sum",
            "a_mean": "first",
            "a_median": "first",
        }).reset_index()

    # Update occ_title: use most recent year's title (most accurate post-reclassification)
    title_source = result.sort_values("year", ascending=False).drop_duplicates("occ_code")
    title_map = title_source.set_index("occ_code")["occ_title"].to_dict()
    result["occ_title"] = result["occ_code"].map(lambda x: title_map.get(x, x))

    return result.sort_values(["occ_code", "year"]).reset_index(drop=True)


# ── College Scorecard query ──────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=600)
def run_scorecard_query(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Query College Scorecard graduate outcomes for selected filters.

    Matches CIP codes at 4-digit level (XX.XX) since Scorecard data is
    reported at that granularity vs IPEDS 6-digit (XX.XXXX).
    """
    conn = get_conn()
    try:
        conn.execute("SELECT 1 FROM college_scorecard LIMIT 1")
    except Exception:
        conn.close()
        return pd.DataFrame()

    params: list = []
    where = ["sc.earn_mdn_4yr IS NOT NULL"]

    # ── CIP filter (truncate 6-digit → 4-digit) ──────────────────────────
    if cip_patterns:
        sc_cip_set: set[str] = set()
        like_clauses: list[str] = []
        for p in cip_patterns:
            if "%" in p:
                # Wildcard: "52.%" or "52.01%" → use LIKE
                like_clauses.append("sc.cipcode LIKE ?")
                params.append(p[:5] if len(p) > 5 else p)
            else:
                # Exact 6-digit: "52.0101" → truncate to "52.01"
                sc_cip_set.add(p[:5])

        cip_parts: list[str] = list(like_clauses)
        if sc_cip_set:
            ph = ",".join("?" * len(sc_cip_set))
            cip_parts.append(f"sc.cipcode IN ({ph})")
            params.extend(sorted(sc_cip_set))
        if cip_parts:
            where.append(f"({' OR '.join(cip_parts)})")

    # ── Award level filter (pre-mapped in table) ─────────────────────────
    if awlevels:
        ph = ",".join("?" * len(awlevels))
        where.append(f"sc.awlevel IN ({ph})")
        params.extend(awlevels)

    # ── Geography filter (join institutions for state/metro) ─────────────
    # Always join institutions to exclude territories (PR, VI, GU, etc.)
    # Use each institution's most recent IPEDS year so closed/merged schools
    # still match rather than requiring all to exist in the global max year.
    join_inst = (
        "INNER JOIN ("
        "  SELECT unitid, MAX(year) AS max_year"
        "  FROM institutions GROUP BY unitid"
        ") imax ON sc.unitid = imax.unitid "
        "INNER JOIN institutions i "
        "ON i.unitid = imax.unitid AND i.year = imax.max_year"
    )
    territory_ph = ",".join("?" * len(EXCLUDED_TERRITORIES))
    where.append(f"i.stabbr NOT IN ({territory_ph})")
    params.extend(sorted(EXCLUDED_TERRITORIES))

    if geo_key == "state" and geo_values:
        ph = ",".join("?" * len(geo_values))
        where.append(f"i.stabbr IN ({ph})")
        params.extend(geo_values)
    elif geo_key == "metro" and geo_values:
        ph = ",".join("?" * len(geo_values))
        where.append(f"i.cbsa IN ({ph})")
        params.extend(geo_values)

    where_sql = " AND ".join(where)

    sql = f"""
        SELECT
            sc.unitid,
            sc.instnm,
            i.city || ', ' || i.stabbr AS city,
            CASE i.control
                WHEN 1 THEN 'Public'
                WHEN 2 THEN 'Private'
                WHEN 3 THEN 'For-Profit'
                ELSE 'Unknown'
            END AS control_name,
            sc.earn_mdn_4yr,
            sc.debt_all_stgp_eval_mdn,
            sc.debt_to_earnings,
            sc.distance
        FROM college_scorecard sc
        {join_inst}
        WHERE {where_sql}
        ORDER BY sc.debt_to_earnings ASC
    """

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


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


@st.cache_data(show_spinner=False, ttl=600)
def get_emp_proj_cagr(
    cip_patterns: tuple,
    awlevels: tuple,
    geo_key: str,
    geo_values: tuple,
) -> float | None:
    """Return weighted-average employment projection CAGR for SOC codes
    mapped to the given CIP codes/geography, or None if unavailable."""
    if not cip_patterns:
        return None  # "All CIPs" → no meaningful SOC mapping

    conn = get_conn()

    # Award-level group filter (mirrors run_employment_query)
    UNDERGRAD = {1, 2, 3, 4, 5}
    GRADUATE = {6, 7, 8, 17, 18, 19}
    has_ug = bool(set(awlevels) & UNDERGRAD)
    has_gr = bool(set(awlevels) & GRADUATE)
    if has_ug and has_gr:
        awf = ""
    elif has_gr:
        awf = " AND awlevel_group IN ('all', 'graduate')"
    else:
        awf = " AND awlevel_group = 'all'"

    # SOC 2018 codes from CIP-SOC crosswalk
    cip_clauses, cip_params = [], []
    for p in cip_patterns:
        cip_clauses.append("cipcode LIKE ?" if "%" in p else "cipcode = ?")
        cip_params.append(p)
    cip_where = " OR ".join(cip_clauses)
    soc_rows = conn.execute(
        f"SELECT DISTINCT soc_code FROM cip_soc_crosswalk WHERE ({cip_where}){awf}",
        cip_params,
    ).fetchall()
    soc_codes = tuple(r[0] for r in soc_rows)
    if not soc_codes:
        conn.close()
        return None

    # Latest-year employment weights
    soc_ph = ",".join("?" * len(soc_codes))
    area_where, area_params = "", []
    if geo_key == "national":
        area_where = "AND area_type = 1"
    elif geo_key == "state" and geo_values:
        fips = [STABBR_TO_FIPS.get(s, "") for s in geo_values]
        fips = [f for f in fips if f]
        if fips:
            area_where = f"AND area_type = 2 AND area_code IN ({','.join('?' * len(fips))})"
            area_params = fips
    elif geo_key == "metro" and geo_values:
        bls = ["00" + str(c).zfill(5) for c in geo_values]
        area_where = f"AND area_type = 4 AND area_code IN ({','.join('?' * len(bls))})"
        area_params = bls

    latest_emp = pd.read_sql_query(
        f"""SELECT occ_code, SUM(tot_emp) AS tot_emp
            FROM oes_employment
            WHERE year = (SELECT MAX(year) FROM oes_employment)
              AND occ_code IN ({soc_ph}) {area_where}
            GROUP BY occ_code""",
        conn,
        params=list(soc_codes) + area_params,
    )
    conn.close()

    if latest_emp.empty:
        return None

    # Employment projections
    df_proj = get_employment_projections(
        soc_codes=soc_codes, geo_key=geo_key, geo_values=tuple(geo_values),
    )
    if df_proj.empty or "cagr" not in df_proj.columns:
        return None

    merged = df_proj.merge(latest_emp, on="occ_code", how="inner")
    merged = merged.dropna(subset=["cagr", "tot_emp"])
    if merged.empty or merged["tot_emp"].sum() <= 0:
        return None

    return float(
        (merged["cagr"] * merged["tot_emp"]).sum() / merged["tot_emp"].sum()
    )


@st.cache_data(show_spinner=False, ttl=600)
def run_google_trends_query(
    cip_patterns: tuple,
    geo_key: str,
    geo_values: tuple,
):
    """Query Google Trends interest data for selected CIP codes and geography.

    Returns dict with:
      - 'time_series': DataFrame(date, interest) — national monthly averages
      - 'geo_interest': float or None — interest index for selected geography
      - 'search_terms': list[str] — search terms used
      - 'state_data': DataFrame(state_abbr, interest) — all states
      - 'top_metros': DataFrame(cbsa_code, cbsa_name, interest) — top 15 metros
      - 'per_cip_time': DataFrame(date, cipcode, search_term, interest) — per-CIP
    Or None if no data is available.
    """
    conn = get_conn()
    try:
        conn.execute("SELECT 1 FROM google_trends_time LIMIT 1")
    except Exception:
        conn.close()
        return None

    if not cip_patterns:
        conn.close()
        return None

    # Build CIP filter
    cip_clauses, cip_params = [], []
    for p in cip_patterns:
        cip_clauses.append("cipcode LIKE ?" if "%" in p else "cipcode = ?")
        cip_params.append(p)
    cip_where = " OR ".join(cip_clauses)

    # 1. National time series (aggregate to monthly, average across matched CIPs)
    time_sql = f"""
        SELECT SUBSTR(date, 1, 7) AS month, AVG(interest) AS interest
        FROM google_trends_time
        WHERE ({cip_where})
        GROUP BY month
        ORDER BY month
    """
    df_time = pd.read_sql_query(time_sql, conn, params=cip_params)
    if not df_time.empty:
        df_time["date"] = pd.to_datetime(df_time["month"] + "-01")
        df_time = df_time[["date", "interest"]]

    # 2. Geographic interest
    geo_interest = None
    if geo_key == "national":
        if not df_time.empty:
            geo_interest = df_time.tail(12)["interest"].mean()
    elif geo_key == "state" and geo_values:
        st_ph = ",".join("?" * len(geo_values))
        row = conn.execute(
            f"SELECT AVG(interest) FROM google_trends_state "
            f"WHERE ({cip_where}) AND state_abbr IN ({st_ph})",
            cip_params + list(geo_values),
        ).fetchone()
        if row and row[0] is not None:
            geo_interest = row[0]
    elif geo_key == "metro" and geo_values:
        cbsa_ph = ",".join("?" * len(geo_values))
        try:
            row = conn.execute(
                f"""SELECT AVG(cbsa_interest) FROM (
                    SELECT gt.cipcode,
                        SUM(gt.interest * w.weight) / SUM(w.weight) AS cbsa_interest
                    FROM google_trends_dma gt
                    JOIN dma_cbsa_weights w ON gt.dma_code = w.dma_code
                    WHERE ({cip_where}) AND w.cbsa_code IN ({cbsa_ph})
                    GROUP BY gt.cipcode
                )""",
                cip_params + list(geo_values),
            ).fetchone()
            if row and row[0] is not None:
                geo_interest = row[0]
        except Exception:
            pass  # dma_cbsa_weights table may not exist

    # 3. Search terms used
    terms = [
        r[0] for r in conn.execute(
            f"SELECT DISTINCT search_term FROM google_trends_time WHERE ({cip_where})",
            cip_params,
        ).fetchall()
    ]

    # 4. State-level interest (all states, for choropleth map)
    state_sql = f"""
        SELECT state_abbr, AVG(interest) AS interest
        FROM google_trends_state
        WHERE ({cip_where})
        GROUP BY state_abbr
        ORDER BY interest DESC
    """
    df_states = pd.read_sql_query(state_sql, conn, params=cip_params)

    # 5. Top metro markets (DMA interest weighted into CBSAs, top 15)
    try:
        metro_sql = f"""
            SELECT w.cbsa_code, w.cbsa_name,
                   SUM(gt.interest * w.weight) / SUM(w.weight) AS interest
            FROM google_trends_dma gt
            JOIN dma_cbsa_weights w ON gt.dma_code = w.dma_code
            WHERE ({cip_where}) AND gt.interest > 0
            GROUP BY w.cbsa_code
            ORDER BY interest DESC
            LIMIT 15
        """
        df_metros = pd.read_sql_query(metro_sql, conn, params=cip_params)
    except Exception:
        df_metros = pd.DataFrame(columns=["cbsa_code", "cbsa_name", "interest"])

    # 6. Per-CIP time series (for multi-program comparison)
    per_cip_sql = f"""
        SELECT SUBSTR(date, 1, 7) AS month, cipcode, search_term,
               AVG(interest) AS interest
        FROM google_trends_time
        WHERE ({cip_where})
        GROUP BY cipcode, month
        ORDER BY cipcode, month
    """
    df_per_cip = pd.read_sql_query(per_cip_sql, conn, params=cip_params)
    if not df_per_cip.empty:
        df_per_cip["date"] = pd.to_datetime(df_per_cip["month"] + "-01")
        df_per_cip = df_per_cip[["date", "cipcode", "search_term", "interest"]]

    if df_time.empty:
        conn.close()
        return None

    # ── Volume calibration ────────────────────────────────────────────────
    # Check if search_volume_calibration table exists and load ratios
    has_volume = False
    volume_series = None
    per_cip_volume = None
    geo_volume = None
    est_monthly_vol = None
    state_volume_data = None
    metro_volume_data = None
    try:
        cal_sql = f"""
            SELECT sv.cipcode, sv.est_monthly_vol, sv.anchor_ratio
            FROM search_volume_calibration sv
            WHERE sv.cipcode IN (
                SELECT DISTINCT cipcode FROM google_trends_time WHERE ({cip_where})
            )
        """
        df_cal = pd.read_sql_query(cal_sql, conn, params=cip_params)
        if not df_cal.empty:
            has_volume = True
            # Weighted average est_monthly_vol across selected CIPs
            est_monthly_vol = df_cal["est_monthly_vol"].mean()

            # For time series volume: scale the aggregate interest index
            # Volume = (interest / interest_at_anchor_month) * est_monthly_vol
            anchor_month_interest = None
            march_row = df_time[df_time["date"] == pd.Timestamp("2025-03-01")]
            if not march_row.empty:
                anchor_month_interest = march_row["interest"].iloc[0]
            if anchor_month_interest and anchor_month_interest > 0:
                volume_series = df_time.copy()
                volume_series["volume"] = (
                    volume_series["interest"] / anchor_month_interest * est_monthly_vol
                ).round(0).astype(int)
            else:
                # Fallback: use the series max as reference
                max_interest = df_time["interest"].max()
                if max_interest > 0:
                    volume_series = df_time.copy()
                    # Scale so that peak = est_monthly_vol * (100 / avg_peak_ratio)
                    volume_series["volume"] = (
                        volume_series["interest"] / max_interest
                        * est_monthly_vol * (100 / df_time["interest"].mean())
                    ).round(0).astype(int)

            # Per-CIP volume series
            if not df_per_cip.empty:
                per_cip_volume = df_per_cip.merge(
                    df_cal[["cipcode", "est_monthly_vol"]],
                    on="cipcode", how="left",
                )
                # For each CIP, find its March 2025 interest as anchor
                per_cip_volume["volume"] = 0
                for cip in per_cip_volume["cipcode"].unique():
                    mask = per_cip_volume["cipcode"] == cip
                    cip_data = per_cip_volume.loc[mask]
                    march = cip_data[cip_data["date"] == pd.Timestamp("2025-03-01")]
                    cip_vol = cip_data["est_monthly_vol"].iloc[0]
                    if not march.empty and march["interest"].iloc[0] > 0:
                        anchor_int = march["interest"].iloc[0]
                    else:
                        anchor_int = cip_data["interest"].max()
                    if anchor_int > 0 and pd.notna(cip_vol):
                        per_cip_volume.loc[mask, "volume"] = (
                            cip_data["interest"] / anchor_int * cip_vol
                        ).round(0).astype(int)

            # Geographic volume: scale geo_interest by volume
            if geo_interest is not None and anchor_month_interest and anchor_month_interest > 0:
                geo_volume = round(geo_interest / anchor_month_interest * est_monthly_vol)

            # State-level volume: weight interest by state population
            # volume_share = (interest × population) / Σ(interest × population)
            # state_volume = volume_share × national_monthly_vol
            state_volume_data = None
            if not df_states.empty:
                try:
                    df_state_pop = pd.read_sql_query(
                        "SELECT state_abbr, population FROM state_populations",
                        conn,
                    )
                    sv = df_states.merge(df_state_pop, on="state_abbr", how="inner")
                    sv["weighted"] = sv["interest"] * sv["population"]
                    total_weighted = sv["weighted"].sum()
                    if total_weighted > 0:
                        sv["volume"] = (
                            sv["weighted"] / total_weighted * est_monthly_vol
                        ).round(0).astype(int)
                        state_volume_data = sv[["state_abbr", "interest", "volume"]]
                except Exception:
                    pass

            # Metro-level volume: compute for ALL metros (not just top 15)
            # so volume shares are accurate, then take top 15 by volume
            metro_volume_data = None
            try:
                all_metro_sql = f"""
                    SELECT w.cbsa_code, w.cbsa_name,
                           SUM(gt.interest * w.weight) / SUM(w.weight) AS interest
                    FROM google_trends_dma gt
                    JOIN dma_cbsa_weights w ON gt.dma_code = w.dma_code
                    WHERE ({cip_where}) AND gt.interest > 0
                    GROUP BY w.cbsa_code
                """
                df_all_metros = pd.read_sql_query(
                    all_metro_sql, conn, params=cip_params,
                )
                if not df_all_metros.empty:
                    df_cbsa_pop = pd.read_sql_query(
                        "SELECT cbsa_code, population FROM cbsa_populations",
                        conn,
                    )
                    mv = df_all_metros.merge(
                        df_cbsa_pop, on="cbsa_code", how="inner",
                    )
                    mv["weighted"] = mv["interest"] * mv["population"]
                    total_weighted = mv["weighted"].sum()
                    if total_weighted > 0:
                        mv["volume"] = (
                            mv["weighted"] / total_weighted * est_monthly_vol
                        ).round(0).astype(int)
                        # Top 15 by volume for display
                        metro_volume_data = (
                            mv[["cbsa_code", "cbsa_name", "interest", "volume"]]
                            .sort_values("volume", ascending=False)
                            .head(15)
                        )
            except Exception:
                pass
    except Exception as _vol_err:
        import traceback as _tb
        _vol_debug = _tb.format_exc()
        # Store debug info so the UI can display it
        has_volume = False
        _vol_error_msg = f"{type(_vol_err).__name__}: {_vol_err}\n{_vol_debug}"
    else:
        _vol_error_msg = None

    conn.close()

    return {
        "time_series": df_time,
        "geo_interest": round(geo_interest, 1) if geo_interest is not None else None,
        "search_terms": terms,
        "state_data": df_states,
        "top_metros": df_metros,
        "per_cip_time": df_per_cip,
        "has_volume": has_volume,
        "volume_series": volume_series,
        "per_cip_volume": per_cip_volume,
        "geo_volume": geo_volume,
        "est_monthly_vol": est_monthly_vol,
        "state_volume_data": state_volume_data if has_volume else None,
        "metro_volume_data": metro_volume_data if has_volume else None,
        "_vol_error_msg": _vol_error_msg,
    }



# ── Excel export helper ──────────────────────────────────────────────────────

_VI_ORANGE = "F26822"
_VI_DARK = "333333"
_HEADER_FILL = PatternFill(start_color=_VI_ORANGE, end_color=_VI_ORANGE, fill_type="solid")
_HEADER_FONT = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
_BODY_FONT = Font(name="Calibri", size=10, color=_VI_DARK)
_THIN_BORDER = Border(
    bottom=Side(style="thin", color="DDDDDD"),
)
_PCT_FMT = '0.0%'
_MONEY_FMT = '$#,##0'
_NUM_FMT = '#,##0'


def _style_sheet(ws, df, pct_cols=None, money_cols=None, num_cols=None):
    """Apply VI-branded formatting to a worksheet built from a DataFrame."""
    pct_cols = set(pct_cols or [])
    money_cols = set(money_cols or [])
    num_cols = set(num_cols or [])

    # Header row
    for col_idx, col_name in enumerate(df.columns, 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.fill = _HEADER_FILL
        cell.font = _HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Body rows
    for row_idx in range(2, ws.max_row + 1):
        for col_idx in range(1, ws.max_column + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            cell.font = _BODY_FONT
            cell.border = _THIN_BORDER
            col_name = df.columns[col_idx - 1]
            if col_name in pct_cols:
                cell.number_format = _PCT_FMT
            elif col_name in money_cols:
                cell.number_format = _MONEY_FMT
            elif col_name in num_cols:
                cell.number_format = _NUM_FMT

    # Auto-width columns (capped at 40)
    for col_idx, col_name in enumerate(df.columns, 1):
        max_len = len(str(col_name))
        for row_idx in range(2, min(ws.max_row + 1, 102)):
            val = ws.cell(row=row_idx, column=col_idx).value
            if val is not None:
                max_len = max(max_len, len(str(val)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 3, 40)

    # Freeze header row
    ws.freeze_panes = "A2"


def build_export_workbook(sheets_data):
    """Build an openpyxl Workbook from a list of (sheet_name, df, fmt_opts) tuples.

    fmt_opts is a dict with optional keys: pct_cols, money_cols, num_cols.
    Returns bytes of the .xlsx file.
    """
    from openpyxl import Workbook

    wb = Workbook()
    # Remove the default sheet
    wb.remove(wb.active)

    for sheet_name, df, fmt_opts in sheets_data:
        if df is None or df.empty:
            continue
        # Truncate sheet name to 31 chars (Excel limit)
        safe_name = sheet_name[:31]
        ws = wb.create_sheet(title=safe_name)

        # Write header
        for col_idx, col_name in enumerate(df.columns, 1):
            ws.cell(row=1, column=col_idx, value=col_name)

        # Write data
        for row_idx, row in enumerate(df.itertuples(index=False), 2):
            for col_idx, value in enumerate(row, 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                # Convert numpy types to native Python
                if isinstance(value, (np.integer,)):
                    value = int(value)
                elif isinstance(value, (np.floating,)):
                    value = float(value) if pd.notna(value) else None
                elif pd.isna(value) if isinstance(value, float) else False:
                    value = None
                cell.value = value

        _style_sheet(
            ws, df,
            pct_cols=fmt_opts.get("pct_cols"),
            money_cols=fmt_opts.get("money_cols"),
            num_cols=fmt_opts.get("num_cols"),
        )

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


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
    # ── Preset program definitions ──────────────────────────────────────────
    # Each preset: display name -> dict with "cips" (list of 6-digit codes)
    # and "level" (label matching AWARD_LEVELS values or AGGREGATE_LEVELS keys)
    PROGRAM_PRESETS = {
        "MBA": {
            "cips": ["52.0101", "52.0201", "52.1301"],
            "level": "Master's degree",
        },
    }

    with st.sidebar:
        st.markdown("## 🎓 IPEDS Explorer")
        st.caption("Completions 2014–15 → 2023–24")

        # DB version diagnostic (small caption at top of sidebar)
        try:
            _diag_conn = get_conn()
            _diag_vol = _diag_conn.execute(
                "SELECT COUNT(*) FROM search_volume_calibration"
            ).fetchone()[0]
            _diag_conn.close()
            st.caption(f"DB: v1.5 | Volume cal: {_diag_vol} keywords")
        except Exception:
            st.caption("DB: pre-v1.5 (no volume calibration)")

        # Quick-select presets
        preset_names = ["— Select a program —"] + list(PROGRAM_PRESETS.keys())
        chosen_preset = st.selectbox(
            "Quick Select",
            options=preset_names,
            index=0,
            key="preset_select",
        )

        if chosen_preset != "— Select a program —":
            preset = PROGRAM_PRESETS[chosen_preset]
            st.session_state["_preset_cips"] = preset["cips"]
            st.session_state["_preset_level"] = preset["level"]
        else:
            st.session_state.pop("_preset_cips", None)
            st.session_state.pop("_preset_level", None)

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
            # Use preset CIP codes if a quick-select is active, else default
            if "_preset_cips" in st.session_state:
                _pcips = st.session_state["_preset_cips"]
                default_cip_labels = [
                    l for _, l in cip_options
                    if any(l.startswith(c) for c in _pcips)
                ]
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
            # Use preset level if a quick-select is active, else default
            if "_preset_level" in st.session_state:
                default_levels = [st.session_state["_preset_level"]]
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
        program_counts = run_program_count_query(
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

    # ── Compute unified projection ───────────────────────────────────────────
    df_totals = df.groupby("year")["completions"].sum()
    sel_dict = df_totals.to_dict()

    # Get employment CAGR if specific CIPs selected
    emp_cagr_for_completions = None
    if not all_cips:
        emp_cagr_for_completions = get_emp_proj_cagr(
            cip_patterns=cip_patterns,
            awlevels=selected_awlevels,
            geo_key=geo_key,
            geo_values=tuple(geo_values),
        )

    projection, proj_components = compute_unified_projection(
        sel_dict,
        emp_cagr=emp_cagr_for_completions,
        program_counts=program_counts,
    )

    # Extract capacity CAGR for the projected program count bars
    capacity_cagr = proj_components.get("capacity")

    # ── Export All to Excel button ────────────────────────────────────────────
    def _build_excel_export():
        """Collect data from all sections and build a multi-tab Excel file."""
        sheets = []

        # 1. Completions Trend
        if "award_level_name" in df.columns:
            trend_df = df[["year", "award_level_name", "completions"]].copy()
            trend_df["year"] = trend_df["year"].apply(yr_label)
            trend_df.columns = ["Year", "Award Level", "Completions"]
        else:
            trend_df = df.groupby("year")["completions"].sum().reset_index()
            trend_df["year"] = trend_df["year"].apply(yr_label)
            trend_df.columns = ["Year", "Completions"]

        # Add YoY %
        _totals = df.groupby("year")["completions"].sum().reset_index()
        _totals = _totals.sort_values("year")
        _totals["YoY % Change"] = _totals["completions"].pct_change()
        _totals["year"] = _totals["year"].apply(yr_label)
        _yoy_map = dict(zip(_totals["year"], _totals["YoY % Change"]))
        trend_df["YoY % Change"] = trend_df["Year"].map(_yoy_map)

        sheets.append(("Completions Trend", trend_df, {
            "num_cols": ["Completions"],
            "pct_cols": ["YoY % Change"],
        }))

        # 2. By Institution
        if not df_inst.empty:
            meta = (
                df_inst.sort_values("year")
                .groupby("unitid")[["instnm", "city", "stabbr", "control_name"]]
                .last()
                .reset_index()
            )
            pivot = df_inst.pivot_table(
                index="unitid", columns="year", values="completions",
                aggfunc="sum", fill_value=0,
            ).reset_index()
            pivot = pivot.merge(meta, on="unitid", how="left")
            pivot.columns.name = None
            yr_cols = sorted([c for c in pivot.columns if isinstance(c, int)])
            first_col, last_col = yr_cols[0], yr_cols[-1]
            n_years = last_col - first_col
            col_3ago = last_col - 3

            def _inst_cagr(row, start_col, n):
                fv, lv = row[start_col], row[last_col]
                if fv > 0 and lv > 0 and n > 0:
                    return ((lv / fv) ** (1 / n) - 1)
                return None

            if col_3ago in yr_cols:
                pivot["3-yr CAGR"] = pivot.apply(lambda r: _inst_cagr(r, col_3ago, 3), axis=1)
            pivot["10-yr CAGR"] = pivot.apply(lambda r: _inst_cagr(r, first_col, n_years), axis=1)
            pivot = pivot.rename(columns={y: yr_label(y) for y in yr_cols})
            control_map = {"Public": "Public", "Private nonprofit": "Private", "Private for-profit": "For-Profit"}
            pivot["control_name"] = pivot["control_name"].map(control_map).fillna(pivot["control_name"])
            pivot["city"] = pivot["city"] + ", " + pivot["stabbr"]
            pivot = pivot.drop(columns=["unitid", "stabbr"])
            pivot = pivot.rename(columns={"instnm": "Institution", "city": "City", "control_name": "Control"})
            yr_labels = [yr_label(y) for y in yr_cols]
            cagr_cols = [c for c in ["3-yr CAGR", "10-yr CAGR"] if c in pivot.columns]
            pivot = pivot[["Institution", "City", "Control"] + yr_labels + cagr_cols]
            last_yr_lbl = yr_label(last_col)
            pivot = pivot.sort_values(last_yr_lbl, ascending=False, na_position="last").reset_index(drop=True)

            sheets.append(("By Institution", pivot, {
                "num_cols": yr_labels,
                "pct_cols": cagr_cols,
            }))

        # 3. Distance Education Programs
        if not all_cips:
            try:
                _dep_conn = get_conn()
                _dep_conn.execute("SELECT 1 FROM completions_dep LIMIT 1")
                _dep_conn.close()
                _dep_df = run_dep_query(
                    cip_patterns=cip_patterns,
                    awlevels=selected_awlevels,
                    geo_key=geo_key,
                    geo_values=tuple(geo_values),
                )
                if _dep_df is not None and not _dep_df.empty:
                    dep_export = _dep_df.copy()
                    dep_export["year"] = dep_export["year"].apply(yr_label)
                    dep_export = dep_export.rename(columns={
                        "year": "Year",
                        "programs": "Total Programs",
                        "programs_de_any": "DE Programs (Any)",
                        "pct_de_any": "DE Share %",
                    })
                    cols_to_keep = [c for c in ["Year", "Total Programs", "DE Programs (Any)", "DE Share %"] if c in dep_export.columns]
                    dep_export = dep_export[cols_to_keep]
                    sheets.append(("Distance Education", dep_export, {
                        "num_cols": ["Total Programs", "DE Programs (Any)"],
                    }))
            except Exception:
                pass

        # 4. Graduate Outcomes (Scorecard)
        if not all_cips:
            try:
                _sc_conn = get_conn()
                _sc_conn.execute("SELECT 1 FROM college_scorecard LIMIT 1")
                _sc_conn.close()
                _sc_df = run_scorecard_query(
                    cip_patterns=cip_patterns,
                    awlevels=selected_awlevels,
                    geo_key=geo_key,
                    geo_values=tuple(geo_values),
                )
                if not _sc_df.empty:
                    _sc_df = (
                        _sc_df.sort_values("earn_mdn_4yr", ascending=False)
                        .drop_duplicates(subset=["unitid"], keep="first")
                    )
                    sc_export = _sc_df.rename(columns={
                        "instnm": "Institution",
                        "city": "City",
                        "control_name": "Control",
                        "earn_mdn_4yr": "Median Earnings (4yr)",
                        "debt_all_stgp_eval_mdn": "Median Debt",
                        "debt_to_earnings": "Debt/Earnings",
                    })
                    sc_export = sc_export.sort_values(
                        "Debt/Earnings", ascending=True, na_position="last"
                    )
                    sc_export = sc_export[
                        ["Institution", "City", "Control",
                         "Median Earnings (4yr)", "Median Debt", "Debt/Earnings"]
                    ].reset_index(drop=True)
                    sheets.append(("Graduate Outcomes", sc_export, {
                        "money_cols": ["Median Earnings (4yr)", "Median Debt"],
                    }))
            except Exception:
                pass

        # 5. Employment by Occupation
        if not all_cips:
            try:
                _conn = get_conn()
                _conn.execute("SELECT 1 FROM oes_employment LIMIT 1")
                _conn.execute("SELECT 1 FROM cip_soc_crosswalk LIMIT 1")
                _conn.close()
                _emp_df = run_employment_query(
                    cip_patterns=cip_patterns,
                    awlevels=selected_awlevels,
                    geo_key=geo_key,
                    geo_values=tuple(geo_values),
                )
                if not _emp_df.empty:
                    _latest_yr = _emp_df["year"].max()
                    _emp_latest = _emp_df[_emp_df["year"] == _latest_yr].copy()
                    emp_export = _emp_latest.rename(columns={
                        "occ_code": "SOC Code",
                        "occ_title": "Occupation",
                        "tot_emp": "Total Employment",
                        "a_median": "Median Annual Wage",
                    })
                    cols = [c for c in ["SOC Code", "Occupation", "Total Employment", "Median Annual Wage"] if c in emp_export.columns]
                    emp_export = emp_export[cols].sort_values("Total Employment", ascending=False).reset_index(drop=True)
                    sheets.append(("Employment", emp_export, {
                        "num_cols": ["Total Employment"],
                        "money_cols": ["Median Annual Wage"],
                    }))
            except Exception:
                pass

        # 6. Summary / metadata tab
        summary_rows = [
            {"Field": "Report Generated", "Value": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")},
            {"Field": "Geography", "Value": geo_label},
            {"Field": "CIP Code(s)", "Value": "All Programs" if all_cips else "; ".join(selected_cip_labels)},
            {"Field": "Award Level(s)", "Value": "All Award Levels" if all_levels else "; ".join(selected_level_labels)},
        ]
        summary_df = pd.DataFrame(summary_rows)
        sheets.insert(0, ("Summary", summary_df, {}))

        return build_export_workbook(sheets)

    cip_slug = "all_programs" if all_cips else (
        "_".join(cip_label_to_code[l] for l in selected_cip_labels) or "completions"
    )
    excel_fname = (
        f"ipeds_{cip_slug}"
        f"_{geo_label.replace(', ', '_').replace(' ', '_')}.xlsx"
    )

    # Place the button in the top-right area
    _btn_col1, _btn_col2 = st.columns([5, 1])
    with _btn_col2:
        if st.button("📊 Export All to Excel", type="primary", use_container_width=True):
            with st.spinner("Building Excel export…"):
                excel_bytes = _build_excel_export()
            st.session_state["_excel_export"] = excel_bytes
            st.session_state["_excel_fname"] = excel_fname

    if "_excel_export" in st.session_state:
        with _btn_col2:
            st.download_button(
                "⬇️ Download Excel",
                data=st.session_state["_excel_export"],
                file_name=st.session_state["_excel_fname"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_excel_all",
            )

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

    # Projected CAGR – use blended rate directly when available so the
    # header metric matches the footnote (avoids rounding drift from
    # back-computing CAGR out of integer-rounded projection values).
    if proj_components and "blended_rate" in proj_components:
        cagr_proj = proj_components["blended_rate"]
        proj_last_yr = last_yr + (len(projection) if projection else 5)
    elif projection and last_val > 0:
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
        f"Proj. CAGR ({yr_label(last_yr)} → {yr_label(proj_last_yr)})",
        f"{cagr_proj:+.1%}" if cagr_proj is not None else "N/A",
    )
    m5.metric("Reporting Institutions", f"{n_inst:,}")

    st.divider()

    # ── Chart (dual-axis: completions line + programs offered bars) ──────────
    chart_title = (
        f"<b>{cip_display}</b>"
        f"<br><sup style='color:#999999'>{level_str} · {geo_label}</sup>"
    )

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # ── Programs Offered bars (secondary y-axis, behind everything) ───────
    _pc_years = sorted(program_counts.keys())
    _pc_vals = [program_counts[y] for y in _pc_years]
    if _pc_years:
        fig.add_trace(
            go.Bar(
                x=_pc_years,
                y=_pc_vals,
                name="Programs Offered",
                marker=dict(color="rgba(15, 134, 193, 0.18)"),
                hovertemplate="%{y:,} programs<extra></extra>",
                showlegend=True,
            ),
            secondary_y=True,
        )

    # ── Completions line(s) (primary y-axis) ──────────────────────────────
    if "award_level_name" in df.columns:
        for idx, (level_name, grp) in enumerate(
            df.groupby("award_level_name", sort=False)
        ):
            _color = CHART_COLORS[idx % len(CHART_COLORS)]
            fig.add_trace(
                go.Scatter(
                    x=grp["year"],
                    y=grp["completions"],
                    mode="lines+markers+text",
                    name=level_name,
                    line=dict(width=2.5, color=_color),
                    marker=dict(size=8, color=_color),
                    text=[f"{v:,}" for v in grp["completions"]],
                    textposition="top center",
                    textfont=dict(size=11),
                    hovertemplate=(
                        f"<b>{level_name}</b><br>"
                        "%{y:,.0f} completions<extra></extra>"
                    ),
                    showlegend=True,
                ),
                secondary_y=False,
            )
    else:
        df_agg = df.groupby("year")["completions"].sum().reset_index()
        fig.add_trace(
            go.Scatter(
                x=df_agg["year"],
                y=df_agg["completions"],
                mode="lines+markers+text",
                name="Completions",
                line=dict(width=2.5, color="#f26822"),
                marker=dict(size=9, color="#f26822"),
                text=[f"{v:,}" for v in df_agg["completions"]],
                textposition="top center",
                textfont=dict(size=11),
                hovertemplate="%{y:,.0f} completions<extra></extra>",
                showlegend=True,
            ),
            secondary_y=False,
        )

    # ── Projection (single unified line) ─────────────────────────────────────
    chart_years = list(all_years)
    last_actual_val = int(df_totals[all_years[-1]]) if projection else 0

    if projection:
        proj_years_list = [p[0] for p in projection]
        proj_vals = [p[1] for p in projection]
        chart_years = list(all_years) + proj_years_list

        # Gray shaded projection region
        fig.add_vrect(
            x0=all_years[-1] + 0.5,
            x1=proj_years_list[-1] + 0.5,
            fillcolor="#E5E7EB", opacity=0.3, layer="below", line_width=0,
        )

        # Projected program count bars (semi-transparent)
        if program_counts and capacity_cagr is not None:
            _last_pc = program_counts.get(all_years[-1], _pc_vals[-1] if _pc_vals else 0)
            _proj_pc_yrs = []
            _proj_pc_vals = []
            for i, y in enumerate(proj_years_list):
                _proj_pc_yrs.append(y)
                _proj_pc_vals.append(
                    max(int(round(_last_pc * (1 + capacity_cagr) ** (i + 1))), 0)
                )
            fig.add_trace(
                go.Bar(
                    x=_proj_pc_yrs,
                    y=_proj_pc_vals,
                    name="Programs (projected)",
                    marker=dict(
                        color="rgba(15, 134, 193, 0.08)",
                        line=dict(color="rgba(15, 134, 193, 0.25)", width=1),
                    ),
                    hovertemplate="%{y:,} programs (projected)<extra></extra>",
                    showlegend=False,
                ),
                secondary_y=True,
            )

        # Single projection line
        fig.add_trace(
            go.Scatter(
                x=[all_years[-1]] + proj_years_list,
                y=[last_actual_val] + proj_vals,
                mode="lines+markers+text",
                name="Projection",
                line=dict(color="rgba(242, 104, 34, 0.6)", width=3, dash="dash"),
                marker=dict(size=7, symbol="diamond", color="rgba(242, 104, 34, 0.6)"),
                text=[""] + [f"{v:,}" for v in proj_vals],
                textposition="top center",
                textfont=dict(size=10, color="rgba(242, 104, 34, 0.75)"),
                hovertemplate="%{y:,.0f} (projected)<extra></extra>",
                showlegend=False,
            ),
            secondary_y=False,
        )

    chart_tick_labels = [yr_label(y) for y in chart_years]

    fig.update_layout(
        title=dict(text=chart_title, font=dict(size=15), x=0, xanchor="left"),
        xaxis=dict(
            tickmode="array",
            tickvals=chart_years,
            ticktext=chart_tick_labels,
            tickangle=-30,
            showgrid=True,
            gridcolor="#F3F4F6",
            gridwidth=1,
        ),
        hovermode="x unified",
        showlegend=False,
        height=540,
        margin=dict(t=100, b=60, l=70, r=70),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Montserrat, Arial, sans-serif", size=13, color="#333333"),
        bargap=0.35,
    )
    fig.update_yaxes(
        title_text="Total Completions",
        tickformat=",",
        showgrid=True,
        gridcolor="#F3F4F6",
        gridwidth=1,
        zeroline=False,
        rangemode="tozero",
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="Programs Offered",
        tickformat=",",
        showgrid=False,
        zeroline=False,
        rangemode="tozero",
        secondary_y=True,
    )

    st.plotly_chart(fig, use_container_width=True)

    # Projection methodology note
    if projection and proj_components:
        _parts = []
        _w = proj_components.get("weights", {})
        if "trend" in _w:
            _trend_rate = proj_components.get("trend")
            _trend_str = f" {_trend_rate:+.1%}/yr" if _trend_rate is not None else ""
            _parts.append(f"completions trend{_trend_str} ({_w['trend']:.0%})")
        if "employment" in _w:
            _parts.append(
                f"BLS employment growth {emp_cagr_for_completions:+.1%}/yr ({_w['employment']:.0%})"
            )
        if "capacity" in _w:
            _cap = proj_components["capacity"]
            _parts.append(
                f"program capacity {_cap:+.1%}/yr ({_w['capacity']:.0%})"
            )
        _blend_rate = proj_components.get("blended_rate")
        _rate_str = f" Blended rate: {_blend_rate:+.1%}/yr." if _blend_rate is not None else ""
        st.caption(
            f"**Projection** blends {', '.join(_parts)}.{_rate_str}"
        )

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
    _yoy_proj = projection
    if _yoy_proj:
        last_actual = int(df_totals[all_years[-1]])
        proj_chain = [last_actual] + [p[1] for p in _yoy_proj]
        proj_yoy_years = [p[0] for p in _yoy_proj]
        proj_yoy_vals = [
            ((proj_chain[i + 1] - proj_chain[i]) / proj_chain[i] * 100)
            if proj_chain[i] > 0 else 0
            for i in range(len(_yoy_proj))
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

    # Footnote listing CIP codes and award levels included
    if all_cips:
        _cip_note = "All CIP codes"
    else:
        _cip_bullets = "  \n".join(f"- **{l}**" for l in selected_cip_labels)
        _cip_note = f"**{len(selected_cip_labels)}** CIP code(s):  \n{_cip_bullets}"
    _level_note = "All award levels" if all_levels else ", ".join(selected_level_labels)
    st.caption(f"Includes {_cip_note}  \nAward level(s): {_level_note}")

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

    # ── Distance Education Programs ──────────────────────────────────────────
    st.divider()
    st.subheader("Distance Education Programs")
    st.caption(
        "Number of programs offered and share available via distance education "
        "| Source: IPEDS Completions DEP Survey"
    )

    _dep_ok = False
    try:
        _dep_conn = get_conn()
        _dep_conn.execute("SELECT 1 FROM completions_dep LIMIT 1")
        _dep_conn.close()
        _dep_ok = True
    except Exception:
        pass

    if not _dep_ok:
        st.info("Distance education program data not loaded.")
    elif all_cips:
        st.info(
            "Distance education data is shown when specific CIP code(s) are "
            "selected. Deselect **All CIP codes** and choose program(s)."
        )
    else:
        with st.spinner("Querying distance education data..."):
            df_dep = run_dep_query(
                cip_patterns=cip_patterns,
                awlevels=selected_awlevels,
                geo_key=geo_key,
                geo_values=tuple(geo_values),
            )

        if df_dep is None or df_dep.empty:
            st.info("No distance education program data for these filters.")
        else:
            # ── Metrics ──────────────────────────────────────────────
            _dep_latest = df_dep.iloc[-1]
            _dep_earliest = df_dep.iloc[0]
            _latest_yr = int(_dep_latest["year"])
            _earliest_yr = int(_dep_earliest["year"])

            _total_progs = int(_dep_latest["programs"])
            _de_any = int(_dep_latest["programs_de_any"])
            _pct_de = _dep_latest["pct_de_any"]

            # Change in DE % over full period
            _pct_de_first = _dep_earliest["pct_de_any"]
            _pct_change = (
                _pct_de - _pct_de_first
                if pd.notna(_pct_de) and pd.notna(_pct_de_first) else None
            )

            d1, d2, d3, d4 = st.columns(4)
            d1.metric(
                f"Programs Offered ({yr_label(_latest_yr)})",
                f"{_total_progs:,}",
            )
            d2.metric(
                "DE Programs (Any)",
                f"{_de_any:,}",
                help="Programs completable entirely or partially "
                     "via distance education.",
            )
            d3.metric(
                "DE Share",
                f"{_pct_de:.1f}%",
            )
            d4.metric(
                f"DE Share Change ({yr_label(_earliest_yr)}-{yr_label(_latest_yr)})",
                f"{_pct_change:+.1f} pp" if _pct_change is not None else "N/A",
                help="Percentage point change in DE share over the period.",
            )

            # ── Dual-axis chart: programs (bars) + DE % (line) ───────
            fig_dep = make_subplots(specs=[[{"secondary_y": True}]])

            fig_dep.add_trace(go.Bar(
                x=df_dep["year"].apply(yr_label),
                y=df_dep["programs"],
                name="Total Programs",
                marker=dict(color="rgba(15, 134, 193, 0.25)"),
                hovertemplate="%{y:,} programs<extra></extra>",
            ), secondary_y=False)

            fig_dep.add_trace(go.Bar(
                x=df_dep["year"].apply(yr_label),
                y=df_dep["programs_de_any"],
                name="DE Programs (Any)",
                marker=dict(color="rgba(139, 92, 246, 0.6)"),
                hovertemplate="%{y:,} DE programs<extra></extra>",
            ), secondary_y=False)

            fig_dep.add_trace(go.Scatter(
                x=df_dep["year"].apply(yr_label),
                y=df_dep["pct_de_any"],
                name="DE Share %",
                mode="lines+markers",
                line=dict(width=2.5, color="#f26822"),
                marker=dict(size=6, color="#f26822"),
                hovertemplate="%{y:.1f}%<extra></extra>",
            ), secondary_y=True)

            fig_dep.update_yaxes(
                title_text="Programs Offered",
                showgrid=True, gridcolor="#F3F4F6", gridwidth=1,
                rangemode="tozero",
                secondary_y=False,
            )
            fig_dep.update_yaxes(
                title_text="DE Share (%)",
                showgrid=False,
                rangemode="tozero",
                ticksuffix="%",
                secondary_y=True,
            )
            fig_dep.update_layout(
                title=dict(
                    text="<b>Distance Education Programs Over Time</b>",
                    font=dict(size=15), x=0, xanchor="left",
                ),
                xaxis=dict(title="", showgrid=False),
                height=400,
                margin=dict(t=60, b=40, l=60, r=60),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(
                    family="Montserrat, Arial, sans-serif",
                    size=12, color="#333333",
                ),
                barmode="overlay",
                showlegend=True,
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02,
                    xanchor="left", x=0, font=dict(size=11),
                ),
                hovermode="x unified",
            )
            st.plotly_chart(fig_dep, use_container_width=True)

    # ── Graduate Outcomes (College Scorecard) ────────────────────────────────
    st.divider()
    st.subheader("Graduate Outcomes")
    st.caption(
        "Median earnings 4 years post-graduation and median debt at completion "
        "| Source: U.S. Department of Education College Scorecard"
    )

    _scorecard_ok = False
    try:
        _sc_conn = get_conn()
        _sc_conn.execute("SELECT 1 FROM college_scorecard LIMIT 1")
        _sc_conn.close()
        _scorecard_ok = True
    except Exception:
        pass

    if not _scorecard_ok:
        st.info("Scorecard outcomes data not available.")
    elif all_cips:
        st.info(
            "Graduate outcomes data is shown when specific CIP code(s) are selected. "
            "Deselect **All CIP codes** and choose program(s) to see outcomes."
        )
    else:
        with st.spinner("Querying graduate outcomes..."):
            df_sc = run_scorecard_query(
                cip_patterns=cip_patterns,
                awlevels=selected_awlevels,
                geo_key=geo_key,
                geo_values=tuple(geo_values),
            )

        if df_sc.empty:
            st.info(
                "No graduate outcomes data available for these filters. "
                "Scorecard data is reported at the 4-digit CIP level and may not "
                "cover all institution / program combinations."
            )
        else:
            # Deduplicate across distance modes (keep highest earnings per institution)
            df_sc = (
                df_sc
                .sort_values("earn_mdn_4yr", ascending=False)
                .drop_duplicates(subset=["unitid"], keep="first")
            )

            # ── Metrics row ───────────────────────────────────────────────
            n_inst = df_sc["unitid"].nunique()
            med_earn = df_sc["earn_mdn_4yr"].median()

            _sc_debt = df_sc.dropna(subset=["debt_all_stgp_eval_mdn"])
            med_debt = _sc_debt["debt_all_stgp_eval_mdn"].median() if not _sc_debt.empty else None

            # Compute D/E from the displayed medians so the ratio is consistent
            # with the earnings and debt metrics shown (ratio-of-medians, not
            # median-of-ratios which can diverge due to Simpson's-paradox effects).
            if med_earn and med_earn > 0 and med_debt is not None:
                med_dte = med_debt / med_earn
            else:
                med_dte = None

            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric(
                "Median Earnings (4yr Post-Grad)",
                f"${med_earn:,.0f}" if med_earn else "N/A",
            )
            sc2.metric(
                "Median Debt at Completion",
                f"${med_debt:,.0f}" if med_debt else "N/A",
            )
            sc3.metric(
                "Median Debt-to-Earnings",
                f"{med_dte:.2f}x" if med_dte else "N/A",
            )
            sc4.metric("Institutions with Data", f"{n_inst:,}")

            # ── Detail table ──────────────────────────────────────────────
            sc_display = df_sc.rename(columns={
                "instnm": "Institution",
                "city": "City",
                "control_name": "Control",
                "earn_mdn_4yr": "Median Earnings (4yr)",
                "debt_all_stgp_eval_mdn": "Median Debt",
                "debt_to_earnings": "Debt/Earnings",
            })
            sc_display = sc_display.sort_values(
                "Debt/Earnings", ascending=True, na_position="last"
            )
            sc_display = sc_display[
                ["Institution", "City", "Control",
                 "Median Earnings (4yr)", "Median Debt", "Debt/Earnings"]
            ].reset_index(drop=True)

            # Check if any selected 6-digit CIPs share the same 4-digit prefix
            # (Scorecard data is at 4-digit granularity)
            if cip_patterns:
                _sc_4digit = {p[:5] for p in cip_patterns if "%" not in p}
                _sc_6digit = {p for p in cip_patterns if "%" not in p and len(p) > 5}
                if len(_sc_6digit) > 0 and len(_sc_4digit) < len(_sc_6digit):
                    st.caption(
                        f":information_source: College Scorecard reports outcomes at the 4-digit CIP level "
                        f"(e.g. {sorted(_sc_4digit)[0]}), so results may include related programs "
                        f"that share the same prefix."
                    )

            st.caption(f"{len(sc_display):,} program–institution combinations with earnings data")

            sc_col_cfg = {
                "Institution": st.column_config.TextColumn("Institution", width=250),
                "City": st.column_config.TextColumn("City", width=160),
                "Control": st.column_config.TextColumn("Control", width=85),
                "Median Earnings (4yr)": st.column_config.NumberColumn(
                    "Median Earnings (4yr)", format="$%,.0f", width=155,
                ),
                "Median Debt": st.column_config.NumberColumn(
                    "Median Debt", format="$%,.0f", width=120,
                ),
                "Debt/Earnings": st.column_config.NumberColumn(
                    "Debt/Earnings", format="%.2fx", width=115,
                ),
            }

            st.dataframe(
                sc_display,
                use_container_width=True,
                hide_index=True,
                column_config=sc_col_cfg,
                height=min(len(sc_display) * 35 + 40, 600),
            )

            # ── Download CSV ──────────────────────────────────────────────
            sc_slug = "all_programs" if all_cips else (
                "_".join(
                    cip_label_to_code[l] for l in selected_cip_labels
                ) or "outcomes"
            )
            sc_fname = (
                f"scorecard_{sc_slug}"
                f"_{geo_label.replace(', ', '_').replace(' ', '_')}.csv"
            )
            st.download_button(
                "Download CSV",
                data=sc_display.to_csv(index=False),
                file_name=sc_fname,
                mime="text/csv",
                key="dl_scorecard",
            )

    # ── Search Interest Trends ────────────────────────────────────────────────
    st.divider()
    st.subheader("Search Interest Trends")

    _trends_ok = False
    try:
        _t_conn = get_conn()
        _t_conn.execute("SELECT 1 FROM google_trends_time LIMIT 1")
        _t_conn.close()
        _trends_ok = True
    except Exception:
        pass

    if not _trends_ok:
        st.info(
            "Google Trends data not loaded. Run `python load_google_trends.py` "
            "to download search interest data."
        )
    elif all_cips:
        st.info(
            "Search interest data is shown when specific CIP code(s) are selected. "
            "Deselect **All CIP codes** and choose program(s) to see search trends."
        )
    else:
        with st.spinner("Querying search interest data..."):
            trends_data = run_google_trends_query(
                cip_patterns=cip_patterns,
                geo_key=geo_key,
                geo_values=tuple(geo_values),
            )

        if trends_data is None:
            st.info("No search interest data available for the selected program(s).")
        else:
            df_trend = trends_data["time_series"]
            _geo_interest = trends_data["geo_interest"]
            _search_terms = trends_data["search_terms"]
            _has_volume = trends_data.get("has_volume", False)
            _volume_series = trends_data.get("volume_series")
            _per_cip_volume = trends_data.get("per_cip_volume")
            _geo_volume = trends_data.get("geo_volume")
            _est_monthly_vol = trends_data.get("est_monthly_vol")

            # Volume is default when calibration data exists
            _show_volume = (
                _has_volume
                and _volume_series is not None
                and not _volume_series.empty
            )

            # Helper to format volume numbers
            def _fmt_vol(v):
                if v is None:
                    return "N/A"
                if v >= 1_000_000:
                    return f"{v / 1_000_000:.1f}M"
                if v >= 1_000:
                    return f"{v / 1_000:.1f}K"
                return f"{v:,.0f}"

            # ── Rolling comparison metrics (all based on volume) ─────────
            _vol_src = _volume_series if _show_volume else None
            _int_src = df_trend

            # Monthly volume = rolling 12-month average
            _avg_monthly_vol = None
            if _vol_src is not None and len(_vol_src) >= 12:
                _avg_monthly_vol = _vol_src.tail(12)["volume"].mean()

            # MoM: most recent month vs previous month
            _mom_change = None
            if _vol_src is not None and len(_vol_src) >= 2:
                _cur_m = _vol_src["volume"].iloc[-1]
                _prev_m = _vol_src["volume"].iloc[-2]
                if _prev_m > 0:
                    _mom_change = (_cur_m - _prev_m) / _prev_m

            # QoQ: most recent 3 months vs prior 3 months
            _qoq_change = None
            if _vol_src is not None and len(_vol_src) >= 6:
                _cur_q = _vol_src.tail(3)["volume"].mean()
                _prev_q = _vol_src.iloc[-6:-3]["volume"].mean()
                if _prev_q > 0:
                    _qoq_change = (_cur_q - _prev_q) / _prev_q

            # YoY: most recent 12 months vs prior 12 months
            _yoy_change = None
            if _vol_src is not None and len(_vol_src) >= 24:
                _cur_y = _vol_src.tail(12)["volume"].mean()
                _prev_y = _vol_src.iloc[-24:-12]["volume"].mean()
                if _prev_y > 0:
                    _yoy_change = (_cur_y - _prev_y) / _prev_y
            elif len(_int_src) >= 24:
                _cur_y = _int_src.tail(12)["interest"].mean()
                _prev_y = _int_src.iloc[-24:-12]["interest"].mean()
                if _prev_y > 0:
                    _yoy_change = (_cur_y - _prev_y) / _prev_y

            # Metrics — 4 columns when volume is available
            if _show_volume and _avg_monthly_vol is not None:
                m1, m2, m3, m4 = st.columns(4)
                m1.metric(
                    "Monthly Search Volume",
                    _fmt_vol(round(_avg_monthly_vol)),
                    help="Average estimated monthly searches over the "
                         "most recent 12 months.",
                )
                m2.metric(
                    "MoM Change",
                    f"{_mom_change:+.1%}" if _mom_change is not None else "N/A",
                    help="Most recent month vs. previous month.",
                )
                m3.metric(
                    "QoQ Change",
                    f"{_qoq_change:+.1%}" if _qoq_change is not None else "N/A",
                    help="Most recent 3 months vs. prior 3 months.",
                )
                m4.metric(
                    "YoY Change",
                    f"{_yoy_change:+.1%}" if _yoy_change is not None else "N/A",
                    help="Most recent 12 months vs. prior 12 months.",
                )
            else:
                # Fallback: interest-only metrics
                _peak_idx = df_trend["interest"].idxmax()
                _peak_date = df_trend.loc[_peak_idx, "date"]
                t1, t2, t3 = st.columns(3)
                t1.metric(
                    f"Search Interest ({geo_label})",
                    f"{_geo_interest:.0f}/100"
                    if _geo_interest is not None else "N/A",
                    help="Google Trends relative interest (0=none, 100=peak).",
                )
                t2.metric(
                    "YoY Change",
                    f"{_yoy_change:+.1%}" if _yoy_change is not None else "N/A",
                )
                t3.metric(
                    "Peak Interest",
                    f"{_peak_date.strftime('%b %Y')}"
                    if pd.notna(_peak_date) else "N/A",
                )

            # ── Chart: Dual-axis (volume left, index right) or index-only
            df_per_cip = trends_data["per_cip_time"]
            _multi_cip = (
                not df_per_cip.empty
                and df_per_cip["cipcode"].nunique() > 1
            )

            _trend_colors = [
                "#8B5CF6", "#0f86c1", "#e87537", "#10B981", "#EF4444",
                "#F59E0B", "#EC4899", "#14B8A6", "#6366F1", "#F97316",
            ]

            if _show_volume:
                # ── Dual-axis chart: volume (left) + interest index (right)
                fig_trend = make_subplots(specs=[[{"secondary_y": True}]])

                if _multi_cip:
                    _use_vol = (
                        _per_cip_volume is not None
                        and "volume" in _per_cip_volume.columns
                    )
                    _vdf = _per_cip_volume if _use_vol else df_per_cip
                    for idx, (cip, grp) in enumerate(
                        _vdf.groupby("cipcode", sort=False)
                    ):
                        _color = _trend_colors[idx % len(_trend_colors)]
                        _label = grp["search_term"].iloc[0]
                        # Volume line (left axis)
                        if _use_vol:
                            fig_trend.add_trace(go.Scatter(
                                x=grp["date"], y=grp["volume"],
                                mode="lines", name=_label,
                                line=dict(width=2, color=_color),
                                hovertemplate=(
                                    f"<b>{_label}</b><br>"
                                    "%{x|%b %Y}<br>"
                                    "Volume: %{y:,.0f}<extra></extra>"
                                ),
                            ), secondary_y=False)

                    # Interest index as dashed line on right axis
                    fig_trend.add_trace(go.Scatter(
                        x=df_trend["date"], y=df_trend["interest"],
                        mode="lines", name="Interest Index",
                        line=dict(width=1.5, color="#9CA3AF", dash="dot"),
                        hovertemplate=(
                            "<b>Interest Index</b><br>"
                            "%{x|%b %Y}<br>"
                            "Index: %{y:.0f}/100<extra></extra>"
                        ),
                    ), secondary_y=True)

                    _show_legend = True
                    _chart_title = (
                        "<b>Estimated Monthly Search Volume by Program</b>"
                    )
                else:
                    # Single CIP: volume area + interest dashed line
                    fig_trend.add_trace(go.Scatter(
                        x=_volume_series["date"],
                        y=_volume_series["volume"],
                        mode="lines", name="Est. Volume",
                        line=dict(width=2, color="#8B5CF6"),
                        fill="tozeroy",
                        fillcolor="rgba(139, 92, 246, 0.1)",
                        hovertemplate=(
                            "<b>%{x|%b %Y}</b><br>"
                            "Volume: %{y:,.0f}<extra></extra>"
                        ),
                    ), secondary_y=False)

                    fig_trend.add_trace(go.Scatter(
                        x=df_trend["date"], y=df_trend["interest"],
                        mode="lines", name="Interest Index",
                        line=dict(width=1.5, color="#9CA3AF", dash="dot"),
                        hovertemplate=(
                            "<b>%{x|%b %Y}</b><br>"
                            "Index: %{y:.0f}/100<extra></extra>"
                        ),
                    ), secondary_y=True)

                    _show_legend = True
                    _chart_title = (
                        "<b>Estimated Monthly Search Volume Over Time</b>"
                    )

                fig_trend.update_yaxes(
                    title_text="Est. Monthly Searches",
                    showgrid=True, gridcolor="#F3F4F6", gridwidth=1,
                    rangemode="tozero",
                    secondary_y=False,
                )
                fig_trend.update_yaxes(
                    title_text="Interest Index (0-100)",
                    showgrid=False,
                    rangemode="tozero", range=[0, 105],
                    secondary_y=True,
                )

            else:
                # ── Interest-only chart (no volume calibration) ──────────
                fig_trend = go.Figure()

                if _multi_cip:
                    for idx, (cip, grp) in enumerate(
                        df_per_cip.groupby("cipcode", sort=False)
                    ):
                        _color = _trend_colors[idx % len(_trend_colors)]
                        _label = grp["search_term"].iloc[0]
                        fig_trend.add_trace(go.Scatter(
                            x=grp["date"], y=grp["interest"],
                            mode="lines", name=_label,
                            line=dict(width=2, color=_color),
                            hovertemplate=(
                                f"<b>{_label}</b><br>"
                                "%{x|%b %Y}<br>"
                                "Interest: %{y:.0f}<extra></extra>"
                            ),
                        ))
                    fig_trend.add_trace(go.Scatter(
                        x=df_trend["date"], y=df_trend["interest"],
                        mode="lines", name="Average (all selected)",
                        line=dict(width=2.5, color="#333333", dash="dash"),
                        hovertemplate=(
                            "<b>Average</b><br>"
                            "%{x|%b %Y}<br>"
                            "Interest: %{y:.0f}<extra></extra>"
                        ),
                    ))
                    _show_legend = True
                    _chart_title = "<b>National Search Interest by Program</b>"
                else:
                    fig_trend.add_trace(go.Scatter(
                        x=df_trend["date"], y=df_trend["interest"],
                        mode="lines", name="Search Interest",
                        line=dict(width=2, color="#8B5CF6"),
                        fill="tozeroy",
                        fillcolor="rgba(139, 92, 246, 0.1)",
                        hovertemplate=(
                            "<b>%{x|%b %Y}</b><br>"
                            "Interest: %{y:.0f}<extra></extra>"
                        ),
                    ))
                    _show_legend = False
                    _chart_title = "<b>National Search Interest Over Time</b>"

                fig_trend.update_yaxes(
                    title="Interest (0-100)",
                    showgrid=True, gridcolor="#F3F4F6", gridwidth=1,
                    rangemode="tozero", range=[0, 105],
                )

            fig_trend.update_layout(
                title=dict(
                    text=_chart_title,
                    font=dict(size=15), x=0, xanchor="left",
                ),
                xaxis=dict(
                    title="", showgrid=True,
                    gridcolor="#F3F4F6", gridwidth=1,
                ),
                height=400,
                margin=dict(t=60, b=40, l=60, r=60),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(
                    family="Montserrat, Arial, sans-serif",
                    size=12, color="#333333",
                ),
                showlegend=_show_legend,
                legend=dict(
                    orientation="h", yanchor="bottom", y=1.02,
                    xanchor="left", x=0, font=dict(size=11),
                ),
                hovermode="x unified",
            )
            st.plotly_chart(fig_trend, use_container_width=True)

            # ── Chart 2 & 3: State Map + Top Metro Markets (side by side)
            df_states = trends_data["state_data"]
            df_metros = trends_data["top_metros"]
            _state_vol = trends_data.get("state_volume_data")
            _metro_vol = trends_data.get("metro_volume_data")

            _has_state_chart = not df_states.empty
            _has_metro_chart = not df_metros.empty and len(df_metros) > 0

            if _has_state_chart or _has_metro_chart:
                if _has_state_chart and _has_metro_chart:
                    col_map, col_bar = st.columns([3, 2])
                elif _has_state_chart:
                    col_map = st.container()
                else:
                    col_bar = st.container()

                # ── Choropleth map ────────────────────────────────────
                if _has_state_chart:
                    with col_map:
                        _use_state_vol = (
                            _show_volume
                            and _state_vol is not None
                            and not _state_vol.empty
                        )
                        _map_z = (
                            _state_vol["volume"] if _use_state_vol
                            else df_states["interest"]
                        )
                        _map_locs = (
                            _state_vol["state_abbr"] if _use_state_vol
                            else df_states["state_abbr"]
                        )
                        _map_cbar_title = (
                            "Est. Searches" if _use_state_vol else "Interest"
                        )
                        _map_hover = (
                            "<b>%{location}</b><br>"
                            "Est. Searches: %{z:,.0f}"
                            "<extra></extra>"
                        ) if _use_state_vol else (
                            "<b>%{location}</b><br>"
                            "Interest: %{z:.0f}/100"
                            "<extra></extra>"
                        )
                        _map_title = (
                            "<b>Estimated Search Volume by State</b>"
                            if _use_state_vol
                            else "<b>Search Interest by State</b>"
                        )

                        fig_map = go.Figure(go.Choropleth(
                            locations=_map_locs,
                            z=_map_z,
                            locationmode="USA-states",
                            colorscale=[
                                [0, "#F5F3FF"],
                                [0.25, "#DDD6FE"],
                                [0.5, "#A78BFA"],
                                [0.75, "#7C3AED"],
                                [1, "#4C1D95"],
                            ],
                            colorbar=dict(
                                title=_map_cbar_title,
                                thickness=12,
                                len=0.6,
                                tickfont=dict(size=10),
                            ),
                            hovertemplate=_map_hover,
                        ))
                        fig_map.update_layout(
                            title=dict(
                                text=_map_title,
                                font=dict(size=14), x=0, xanchor="left",
                            ),
                            geo=dict(
                                scope="usa",
                                bgcolor="white",
                                lakecolor="white",
                                showlakes=True,
                                landcolor="#FAFAFA",
                                projection_type="albers usa",
                            ),
                            height=340,
                            margin=dict(t=50, b=10, l=10, r=10),
                            paper_bgcolor="white",
                            font=dict(
                                family="Montserrat, Arial, sans-serif",
                                size=12, color="#333333",
                            ),
                        )
                        st.plotly_chart(
                            fig_map, use_container_width=True
                        )

                # ── Top Metro Markets bar chart ───────────────────────
                if _has_metro_chart:
                    with col_bar:
                        _use_metro_vol = (
                            _show_volume
                            and _metro_vol is not None
                            and not _metro_vol.empty
                        )
                        _bar_src = (
                            _metro_vol if _use_metro_vol else df_metros
                        )
                        _bar_val_col = "volume" if _use_metro_vol else "interest"

                        # Reverse for horizontal bar (highest at top)
                        df_bar = _bar_src.sort_values(
                            _bar_val_col, ascending=True
                        ).tail(15)
                        # Truncate long CBSA names for display
                        df_bar["label"] = df_bar["cbsa_name"].apply(
                            lambda n: (n[:28] + "...") if len(n) > 30 else n
                        )

                        _bar_hover = (
                            "<b>%{y}</b><br>"
                            "Est. Searches: %{x:,.0f}"
                            "<extra></extra>"
                        ) if _use_metro_vol else (
                            "<b>%{y}</b><br>"
                            "Interest: %{x:.0f}/100"
                            "<extra></extra>"
                        )
                        _bar_x_title = (
                            "Est. Monthly Searches"
                            if _use_metro_vol else "Interest (0-100)"
                        )
                        _bar_x_range = (
                            None if _use_metro_vol else [0, 105]
                        )
                        _bar_title = (
                            "<b>Top Metro Markets (by Volume)</b>"
                            if _use_metro_vol
                            else "<b>Top Metro Markets</b>"
                        )

                        fig_bar = go.Figure(go.Bar(
                            x=df_bar[_bar_val_col],
                            y=df_bar["label"],
                            orientation="h",
                            marker=dict(
                                color=df_bar[_bar_val_col],
                                colorscale=[
                                    [0, "#DDD6FE"],
                                    [0.5, "#A78BFA"],
                                    [1, "#7C3AED"],
                                ],
                            ),
                            hovertemplate=_bar_hover,
                        ))
                        fig_bar.update_layout(
                            title=dict(
                                text=_bar_title,
                                font=dict(size=14), x=0, xanchor="left",
                            ),
                            xaxis=dict(
                                title=_bar_x_title,
                                showgrid=True, gridcolor="#F3F4F6",
                                gridwidth=1, range=_bar_x_range,
                            ),
                            yaxis=dict(
                                title="",
                                tickfont=dict(size=10),
                            ),
                            height=340,
                            margin=dict(t=50, b=40, l=10, r=20),
                            plot_bgcolor="white",
                            paper_bgcolor="white",
                            font=dict(
                                family="Montserrat, Arial, sans-serif",
                                size=12, color="#333333",
                            ),
                            showlegend=False,
                            bargap=0.25,
                        )
                        st.plotly_chart(
                            fig_bar, use_container_width=True
                        )

            _terms_display = ", ".join(
                f"**{t}**" for t in _search_terms[:5]
            )
            _more_terms = (
                f" (+{len(_search_terms) - 5} more)"
                if len(_search_terms) > 5 else ""
            )
            if _show_volume:
                st.caption(
                    f"Estimated monthly search volumes are calibrated from "
                    f"Google Trends indices using known keyword volumes "
                    f"(anchor: 'nursing degree' = 146K searches, March 2025). "
                    f"Search term(s): {_terms_display}{_more_terms} "
                    f"| Source: Google Trends + keyword volume calibration"
                )
            else:
                st.caption(
                    f"Search interest reflects relative Google search volume "
                    f"(0 = no interest, 100 = peak over period). "
                    f"Search term(s): {_terms_display}{_more_terms} "
                    f"| Source: Google Trends"
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

                # Footnote listing the occupations included in the aggregate
                occ_list = (
                    df_emp[["occ_code", "occ_title"]]
                    .drop_duplicates("occ_code")
                    .sort_values("occ_code")
                )
                occ_bullets = "  \n".join(
                    f"- **{row.occ_code}** {row.occ_title}" for row in occ_list.itertuples()
                )
                st.caption(
                    f"Aggregate includes **{len(occ_list)}** related occupations "
                    f"(SOC codes mapped via CIP-SOC crosswalk):  \n{occ_bullets}"
                )


    # ── Job Posting Trends (Coresignal) ── admin-only, on-demand ─────────────
    try:
        _cs_api_key = st.secrets["coresignal"]["api_key"]
    except (KeyError, FileNotFoundError):
        _cs_api_key = ""

    _is_admin = st.user.is_logged_in and st.user.email and st.user.email.lower() == "brady.colby@validatedinsights.com"

    if _cs_api_key and _is_admin:
        st.divider()
        st.subheader("Job Posting Trends")
        st.caption(
            "Monthly new job postings for occupations related to the selected program(s) "
            "| Source: Coresignal Base Jobs API"
        )

    if _cs_api_key and _is_admin and all_cips:
        st.info(
            "Job posting data is shown when specific CIP code(s) are selected. "
            "Deselect **All CIP codes** and choose program(s) to see posting trends."
        )
    elif _cs_api_key and _is_admin and not all_cips:
        _cs_key = f"cs_{cip_patterns}_{selected_awlevels}_{geo_key}_{geo_values}"
        if st.button("Pull Job Posting Trends", key="cs_pull"):
            with st.spinner("Querying job posting trends (this may take a moment)..."):
                st.session_state["_cs_data"] = run_coresignal_trend(
                    cip_patterns=cip_patterns,
                    awlevels=selected_awlevels,
                    geo_key=geo_key,
                    geo_values=tuple(geo_values),
                )

        cs_data = st.session_state.get("_cs_data")
        if cs_data is None:
            pass  # no data yet or no results
        elif cs_data == "empty":
            st.info("No job posting data found for the selected program(s) and geography.")
        else:
            trend_df = cs_data["trend_df"]
            current_active = cs_data["current_active"]
            cs_titles = cs_data["search_titles"]
            # ── Metrics row ──────────────────────────────────────────────
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Currently Active Postings", f"{current_active:,}")

            latest_month = trend_df["postings"].iloc[-1]
            mc2.metric("New Postings (latest month)", f"{latest_month:,}")

            # 3-month vs prior 3-month change
            if len(trend_df) >= 6:
                recent_3 = trend_df["postings"].iloc[-3:].sum()
                prior_3 = trend_df["postings"].iloc[-6:-3].sum()
                if prior_3 > 0:
                    qoq_change = (recent_3 - prior_3) / prior_3
                    mc3.metric(
                        "3-Month Change",
                        f"{qoq_change:+.1%}",
                        delta=f"{qoq_change:+.1%}",
                    )

            # ── Trend line chart ─────────────────────────────────────────
            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=trend_df["month"],
                y=trend_df["postings"],
                mode="lines+markers",
                line=dict(color="#f26822", width=3),
                marker=dict(size=7, color="#f26822"),
                hovertemplate="%{x}<br>%{y:,.0f} postings<extra></extra>",
            ))
            fig_trend.update_layout(
                title=dict(
                    text="New Job Postings per Month",
                    font=dict(size=13),
                    x=0,
                    xanchor="left",
                ),
                height=350,
                margin=dict(t=40, b=60, l=70, r=20),
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(family="Montserrat, Arial, sans-serif", size=12, color="#333333"),
                xaxis=dict(
                    title="Month",
                    tickangle=-45,
                    showgrid=True,
                    gridcolor="#F3F4F6",
                    gridwidth=1,
                ),
                yaxis=dict(
                    title="Postings",
                    showgrid=True,
                    gridcolor="#F3F4F6",
                    gridwidth=1,
                    rangemode="tozero",
                ),
                showlegend=False,
            )
            st.plotly_chart(fig_trend, use_container_width=True)

            # ── YoY % change bar chart (if we have 12+ months) ──────────
            if len(trend_df) >= 2:
                trend_df_yoy = trend_df.copy()
                trend_df_yoy["pct_change"] = trend_df_yoy["postings"].pct_change() * 100
                trend_df_yoy = trend_df_yoy.dropna(subset=["pct_change"])

                if not trend_df_yoy.empty:
                    colors = [
                        "#0f86c1" if v >= 0 else "#E74C3C"
                        for v in trend_df_yoy["pct_change"]
                    ]
                    fig_yoy = go.Figure(go.Bar(
                        x=trend_df_yoy["month"],
                        y=trend_df_yoy["pct_change"],
                        marker_color=colors,
                        hovertemplate="%{x}<br>%{y:+.1f}%<extra></extra>",
                    ))
                    fig_yoy.update_layout(
                        title=dict(
                            text="Month-over-Month % Change",
                            font=dict(size=13),
                            x=0,
                            xanchor="left",
                        ),
                        height=220,
                        margin=dict(t=40, b=60, l=70, r=20),
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                        font=dict(family="Montserrat, Arial, sans-serif", size=12, color="#333333"),
                        xaxis=dict(tickangle=-45, showgrid=True, gridcolor="#F3F4F6"),
                        yaxis=dict(
                            ticksuffix="%",
                            tickformat=".0f",
                            showgrid=True,
                            gridcolor="#F3F4F6",
                            zeroline=True,
                            zerolinecolor="#999999",
                            zerolinewidth=1,
                        ),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_yoy, use_container_width=True)

            # ── Caption ──────────────────────────────────────────────────
            titles_str = ", ".join(f"**{t}**" for t in cs_titles)
            st.caption(
                f"Occupation titles searched: {titles_str}. "
                f"Monthly counts reflect new postings created in each month (not cumulative). "
                f"Data refreshes hourly."
            )




if __name__ == "__main__":
    main()
