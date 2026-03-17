"""
Build DMA-to-CBSA population-weighted crosswalk for Google Trends metro mapping.

Downloads:
  1. Census county-to-CBSA delineation file
  2. Census county population estimates
  3. DMA list from Google Trends (one API call)

Matches DMAs to CBSAs by primary city name, then computes population-weighted
mapping so metro-level Google Trends interest can be attributed to CBSA areas.

Creates tables:
  - cbsa_populations   (cbsa_code, cbsa_name, population)
  - dma_cbsa_weights   (dma_code, dma_name, cbsa_code, cbsa_name, weight)

Usage:
    python load_geo_crosswalk.py
"""

import io
import re
import sqlite3
import urllib.request
from pathlib import Path

import pandas as pd

try:
    import openpyxl  # noqa: F401 — needed by pd.read_excel
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "-q"])

DB_PATH = Path(__file__).parent / "ipeds.db"
RAW_DIR = Path(__file__).parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

# ── Data source URLs ──────────────────────────────────────────────────────────
# Census county-to-CBSA delineation (same file used by patch_cbsa_names.py)
CENSUS_DELINEATION_URL = (
    "https://www2.census.gov/programs-surveys/metro-micro/"
    "geographies/reference-files/2023/delineation-files/list1_2023.xlsx"
)
# Census county population estimates (2020s vintage)
CENSUS_POP_URL = (
    "https://www2.census.gov/programs-surveys/popest/datasets/"
    "2020-2024/counties/totals/co-est2024-alldata.csv"
)


# ── Download helpers ──────────────────────────────────────────────────────────

def _download(url: str, dest: Path, label: str) -> Path:
    """Download a file if not already cached in raw/."""
    if dest.exists():
        print(f"  Using cached {label}: {dest.name}")
        return dest
    print(f"  Downloading {label}...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        data = r.read()
    dest.write_bytes(data)
    print(f"  Downloaded {len(data) / 1024:.0f} KB -> {dest.name}")
    return dest


# ── Parse Census delineation → county_fips → (cbsa_code, cbsa_name) ─────────

def parse_census_delineation() -> pd.DataFrame:
    """Return DataFrame(county_fips, cbsa_code, cbsa_name) from Census delineation."""
    path = _download(
        CENSUS_DELINEATION_URL,
        RAW_DIR / "list1_2023.xlsx",
        "Census CBSA delineation file",
    )
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active

    rows_out = []
    header = None
    col_idx = {}

    for row in ws.iter_rows(values_only=True):
        cells = [str(c).strip() if c else "" for c in row]
        if header is None:
            if any("CBSA Code" in s for s in cells):
                header = cells
                for i, s in enumerate(cells):
                    if "CBSA Code" in s:
                        col_idx["cbsa"] = i
                    elif "CBSA Title" in s:
                        col_idx["cbsa_name"] = i
                    elif "FIPS State Code" in s:
                        col_idx["st_fips"] = i
                    elif "FIPS County Code" in s:
                        col_idx["co_fips"] = i
            continue

        cbsa = cells[col_idx["cbsa"]]
        cbsa_name = cells[col_idx["cbsa_name"]]
        st_fips = cells[col_idx["st_fips"]]
        co_fips = cells[col_idx["co_fips"]]

        if cbsa and st_fips and co_fips and cbsa.isdigit():
            county_fips = st_fips.zfill(2) + co_fips.zfill(3)
            rows_out.append((county_fips, cbsa, cbsa_name))

    wb.close()
    df = pd.DataFrame(rows_out, columns=["county_fips", "cbsa_code", "cbsa_name"])
    print(f"  Parsed {len(df):,} county-CBSA rows ({df['cbsa_code'].nunique()} CBSAs)")
    return df


# ── Parse Census population estimates → county_fips → population ─────────────

def parse_census_population() -> pd.DataFrame:
    """Return DataFrame(county_fips, county_pop) from Census county estimates."""
    path = _download(
        CENSUS_POP_URL,
        RAW_DIR / "co-est2024-alldata.csv",
        "Census county population estimates",
    )
    # Census CSV uses encoding cp1252 and has many columns
    df = pd.read_csv(path, encoding="cp1252", dtype=str)
    # Build 5-digit county FIPS from STATE + COUNTY columns
    df["county_fips"] = df["STATE"].str.zfill(2) + df["COUNTY"].str.zfill(3)
    # Use latest population estimate column
    pop_col = "POPESTIMATE2024"
    if pop_col not in df.columns:
        # Fall back to whatever POPESTIMATE column exists
        pop_cols = [c for c in df.columns if c.startswith("POPESTIMATE")]
        pop_col = sorted(pop_cols)[-1] if pop_cols else None
    if pop_col is None:
        raise RuntimeError("No POPESTIMATE column found in Census data")

    df["county_pop"] = pd.to_numeric(df[pop_col], errors="coerce").fillna(0).astype(int)
    # Exclude state-level rows (COUNTY == "000")
    df = df[df["COUNTY"] != "000"].copy()
    result = df[["county_fips", "county_pop"]].copy()
    print(f"  Parsed {len(result):,} county population rows")
    return result


# ── Fetch DMA list from Google Trends ─────────────────────────────────────────

def fetch_dma_list() -> pd.DataFrame:
    """Fetch all 210 DMAs from Google Trends. Returns DataFrame(dma_code, dma_name)."""
    print("  Fetching DMA list from Google Trends (1 API call)...")
    from pytrends.request import TrendReq
    pt = TrendReq(hl="en-US", tz=360)
    pt.build_payload(["degree"], timeframe="today 3-m", geo="US")
    df = pt.interest_by_region(resolution="DMA", inc_geo_code=True)
    df = df.reset_index()
    df = df.rename(columns={"geoName": "dma_name", "geoCode": "dma_code"})
    df["dma_code"] = df["dma_code"].astype(str)
    result = df[["dma_code", "dma_name"]].copy()
    print(f"  Got {len(result)} DMAs from Google Trends")
    return result


# ── Match DMAs to CBSAs by city name + state ─────────────────────────────────

def _parse_dma(dma_name: str) -> tuple[list[str], set[str]]:
    """Extract cities and state abbreviations from a DMA name.

    Examples:
      "New York NY"              -> (["new york"], {"NY"})
      "Washington DC (Hagerstown MD)" -> (["washington"], {"DC","MD"})
      "Albany-Schenectady-Troy NY"   -> (["albany","schenectady","troy"], {"NY"})
    """
    # Handle parenthetical sub-markets: "Washington DC (Hagerstown MD)"
    name = re.sub(r'\(.*?\)', '', dma_name).strip()
    # Collect all 2-letter state abbreviations from original name
    states = set(re.findall(r'\b([A-Z]{2})\b', dma_name))
    # Remove state abbreviations and punctuation from the city portion
    name = re.sub(r'\b[A-Z]{2}\b', '', name)
    name = re.sub(r'[,()]+', ' ', name).strip()
    # Split on hyphens
    cities = [c.strip().lower() for c in name.split("-") if c.strip()]
    return cities, states


def _parse_cbsa(cbsa_name: str) -> tuple[list[str], set[str]]:
    """Extract cities and state abbreviations from a CBSA name.

    Examples:
      "New York-Newark-Jersey City, NY-NJ-PA" -> (["new york","newark","jersey city"], {"NY","NJ","PA"})
      "Miami-Fort Lauderdale-Pompano Beach, FL" -> (["miami","fort lauderdale","pompano beach"], {"FL"})
    """
    states = set(re.findall(r'\b([A-Z]{2})\b', cbsa_name))
    name = re.sub(r'[,\s]+[A-Z]{2}(?:-[A-Z]{2})*\s*$', '', cbsa_name).strip()
    cities = [c.strip().lower() for c in name.split("-") if c.strip()]
    return cities, states


# Abbreviation expansions used in name matching
_ABBREVS = {
    "ft.": "fort", "ft": "fort", "st.": "saint", "st": "saint",
    "mt.": "mount", "mt": "mount",
}


def _normalize(city: str) -> str:
    """Normalize a city name for matching."""
    city = city.lower().strip()
    for abbr, full in _ABBREVS.items():
        city = re.sub(r'\b' + re.escape(abbr) + r'\b', full, city)
    return city


def match_dma_to_cbsa(
    dma_df: pd.DataFrame,
    cbsa_pop_df: pd.DataFrame,
) -> pd.DataFrame:
    """Match DMAs to CBSAs by city name + state overlap, compute population weights.

    Matching rules:
      1. Primary city of DMA must appear in a CBSA's city list (exact or normalized)
      2. DMA and CBSA must share at least one state abbreviation
      3. Weights = CBSA population / sum of all matched CBSA populations for that DMA

    Returns DataFrame(dma_code, dma_name, cbsa_code, cbsa_name, weight).
    """
    # Build CBSA info
    cbsa_info: dict[str, tuple[str, int, list[str], set[str]]] = {}
    for _, row in cbsa_pop_df.iterrows():
        code = row["cbsa_code"]
        name = row["cbsa_name"]
        pop = row["population"]
        cities, states = _parse_cbsa(name)
        norm_cities = [_normalize(c) for c in cities]
        cbsa_info[code] = (name, pop, norm_cities, states)

    matches = []
    matched_dmas = 0

    for _, dma_row in dma_df.iterrows():
        dma_code = dma_row["dma_code"]
        dma_name = dma_row["dma_name"]
        dma_cities, dma_states = _parse_dma(dma_name)

        if not dma_cities:
            continue

        dma_primary = _normalize(dma_cities[0])
        dma_all_norm = [_normalize(c) for c in dma_cities]

        # Find CBSAs where: (a) DMA primary city is in CBSA cities, (b) states overlap
        matched_cbsas = []
        for code, (cname, cpop, ccities, cstates) in cbsa_info.items():
            state_overlap = dma_states & cstates
            if not state_overlap:
                continue
            # Check if DMA primary city matches any CBSA city (exact normalized match)
            if dma_primary in ccities:
                matched_cbsas.append(code)
            # Also check if any DMA city matches any CBSA city (weaker, catch more)
            elif any(dc in ccities for dc in dma_all_norm):
                matched_cbsas.append(code)

        if not matched_cbsas:
            continue

        matched_dmas += 1

        # Compute weights: CBSA pop / sum of all matched CBSA pops
        total_pop = sum(cbsa_info[c][1] for c in matched_cbsas)
        for cbsa_code in matched_cbsas:
            cbsa_name, cbsa_pop = cbsa_info[cbsa_code][0], cbsa_info[cbsa_code][1]
            weight = cbsa_pop / total_pop if total_pop > 0 else 1.0
            matches.append((dma_code, dma_name, cbsa_code, cbsa_name, round(weight, 6)))

    result = pd.DataFrame(
        matches, columns=["dma_code", "dma_name", "cbsa_code", "cbsa_name", "weight"]
    )
    print(f"  Matched {matched_dmas}/{len(dma_df)} DMAs to {result['cbsa_code'].nunique()} CBSAs")
    return result


# ── Load into SQLite ──────────────────────────────────────────────────────────

def load_tables(cbsa_pop_df: pd.DataFrame, weights_df: pd.DataFrame):
    """Write cbsa_populations and dma_cbsa_weights tables to SQLite."""
    conn = sqlite3.connect(DB_PATH)

    # CBSA populations
    conn.execute("DROP TABLE IF EXISTS cbsa_populations")
    conn.execute("""
        CREATE TABLE cbsa_populations (
            cbsa_code   TEXT PRIMARY KEY,
            cbsa_name   TEXT,
            population  INTEGER
        )
    """)
    cbsa_pop_df.to_sql("cbsa_populations", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cbsapop_code ON cbsa_populations(cbsa_code)")

    # DMA-CBSA weights
    conn.execute("DROP TABLE IF EXISTS dma_cbsa_weights")
    conn.execute("""
        CREATE TABLE dma_cbsa_weights (
            dma_code    TEXT NOT NULL,
            dma_name    TEXT,
            cbsa_code   TEXT NOT NULL,
            cbsa_name   TEXT,
            weight      REAL NOT NULL,
            PRIMARY KEY (dma_code, cbsa_code)
        )
    """)
    weights_df.to_sql("dma_cbsa_weights", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dcw_dma ON dma_cbsa_weights(dma_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dcw_cbsa ON dma_cbsa_weights(cbsa_code)")

    conn.commit()
    n_cbsa = conn.execute("SELECT COUNT(*) FROM cbsa_populations").fetchone()[0]
    n_wt = conn.execute("SELECT COUNT(*) FROM dma_cbsa_weights").fetchone()[0]
    conn.close()

    print(f"  Loaded {n_cbsa:,} CBSA populations, {n_wt:,} DMA-CBSA weight rows")


# ── Verification ──────────────────────────────────────────────────────────────

def verify():
    """Spot-check the crosswalk for known metro areas."""
    conn = sqlite3.connect(DB_PATH)

    # Check coverage against bls_oes_areas
    try:
        oes_cbsas = conn.execute(
            "SELECT COUNT(DISTINCT cbsa) FROM bls_oes_areas WHERE area_type = 'metro'"
        ).fetchone()[0]
        mapped_cbsas = conn.execute("""
            SELECT COUNT(DISTINCT w.cbsa_code)
            FROM dma_cbsa_weights w
            INNER JOIN bls_oes_areas b ON w.cbsa_code = b.cbsa
            WHERE b.area_type = 'metro'
        """).fetchone()[0]
        print(f"\n  BLS OES metro areas covered: {mapped_cbsas}/{oes_cbsas} "
              f"({mapped_cbsas/oes_cbsas*100:.0f}%)")
    except Exception as e:
        print(f"  Could not check OES coverage: {e}")

    # Spot checks
    spot_checks = [
        ("35620", "New York"),
        ("31080", "Los Angeles"),
        ("16980", "Chicago"),
        ("33100", "Miami"),
        ("47900", "Washington DC"),
    ]
    print("\n  Spot checks:")
    for cbsa, label in spot_checks:
        rows = conn.execute(
            "SELECT dma_code, dma_name, weight FROM dma_cbsa_weights "
            "WHERE cbsa_code = ? ORDER BY weight DESC",
            (cbsa,),
        ).fetchall()
        if rows:
            top = rows[0]
            print(f"    {label} (CBSA {cbsa}): DMA {top[0]} ({top[1]}), weight={top[2]:.3f}")
        else:
            print(f"    {label} (CBSA {cbsa}): NO MATCH")

    conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Building DMA-CBSA population-weighted crosswalk")
    print("=" * 60)

    # 1. Parse Census data
    print("\n[1/4] Parsing Census county-CBSA delineation...")
    county_cbsa = parse_census_delineation()

    print("\n[2/4] Parsing Census county population estimates...")
    county_pop = parse_census_population()

    # Merge county data and compute CBSA populations
    merged = county_cbsa.merge(county_pop, on="county_fips", how="left")
    merged["county_pop"] = merged["county_pop"].fillna(0).astype(int)
    cbsa_pop = (
        merged.groupby(["cbsa_code", "cbsa_name"])["county_pop"]
        .sum()
        .reset_index()
        .rename(columns={"county_pop": "population"})
    )
    print(f"  Computed populations for {len(cbsa_pop):,} CBSAs")

    # 2. Get DMA list from Google Trends
    print("\n[3/4] Getting DMA list from Google Trends...")
    dma_df = fetch_dma_list()

    # 3. Match and compute weights
    print("\n[4/4] Matching DMAs to CBSAs...")
    weights = match_dma_to_cbsa(dma_df, cbsa_pop)

    # 4. Load into SQLite
    print("\nLoading into SQLite...")
    load_tables(cbsa_pop, weights)

    # 5. Verify
    verify()
    print("\nDone.")


if __name__ == "__main__":
    main()
