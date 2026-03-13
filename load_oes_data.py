"""
Load BLS OES employment data and CIP-SOC crosswalk into the IPEDS SQLite database.

Usage:
    python load_oes_data.py

Downloads:
  - 10 years of BLS OES data (May 2015 – May 2024)
  - CIP 2020 -> SOC 2018 crosswalk from NCES
  - SOC 2010 -> SOC 2018 crosswalk from BLS

Creates tables:
  - oes_employment     (year, area_code, area_type, area_title, occ_code, occ_title, tot_emp, a_mean, a_median)
  - cip_soc_crosswalk  (cipcode, soc_code, soc_title, source)
  - soc_2010_to_2018   (soc_2010, soc_2018)
"""

import io
import re
import sqlite3
import sys
import zipfile
from pathlib import Path

import pandas as pd
import requests

DB_PATH = Path(__file__).parent / "ipeds.db"
RAW_DIR = Path(__file__).parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) research/1.0"}

# OES years: May 2015 (oesm15) through May 2024 (oesm24)
OES_YEARS = list(range(15, 25))  # 15..24 -> 2015..2024

# Columns we need (normalized to uppercase)
KEEP_COLS = [
    "AREA", "AREA_TITLE", "AREA_TYPE", "NAICS", "OCC_CODE", "OCC_TITLE",
    "O_GROUP", "TOT_EMP", "A_MEAN", "A_MEDIAN",
]

# Column name normalization: older files use different names
COL_ALIASES = {
    "occ code": "OCC_CODE",
    "occ title": "OCC_TITLE",
    "group": "O_GROUP",
    "occ_code": "OCC_CODE",
    "occ_title": "OCC_TITLE",
    "o_group": "O_GROUP",
    "area": "AREA",
    "area_title": "AREA_TITLE",
    "area_type": "AREA_TYPE",
    "naics": "NAICS",
    "tot_emp": "TOT_EMP",
    "a_mean": "A_MEAN",
    "a_median": "A_MEDIAN",
}

# State FIPS codes for mapping BLS state area codes to state abbreviations
FIPS_TO_STABBR = {
    1: "AL", 2: "AK", 4: "AZ", 5: "AR", 6: "CA", 8: "CO", 9: "CT", 10: "DE",
    11: "DC", 12: "FL", 13: "GA", 15: "HI", 16: "ID", 17: "IL", 18: "IN",
    19: "IA", 20: "KS", 21: "KY", 22: "LA", 23: "ME", 24: "MD", 25: "MA",
    26: "MI", 27: "MN", 28: "MS", 29: "MO", 30: "MT", 31: "NE", 32: "NV",
    33: "NH", 34: "NJ", 35: "NM", 36: "NY", 37: "NC", 38: "ND", 39: "OH",
    40: "OK", 41: "OR", 42: "PA", 44: "RI", 45: "SC", 46: "SD", 47: "TN",
    48: "TX", 49: "UT", 50: "VT", 51: "VA", 53: "WA", 54: "WV", 55: "WI",
    56: "WY", 72: "PR", 78: "VI", 66: "GU",
}


def download_file(url: str, dest: Path) -> Path:
    """Download a file if not already cached locally."""
    if dest.exists() and dest.stat().st_size > 1000:
        print(f"  [cached] {dest.name}")
        return dest
    print(f"  Downloading {url} ...")
    r = requests.get(url, headers=HEADERS, timeout=600)
    r.raise_for_status()
    dest.write_bytes(r.content)
    print(f"  Saved {dest.name} ({len(r.content) / 1024 / 1024:.1f} MB)")
    return dest


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to uppercase standard."""
    rename = {}
    for col in df.columns:
        col_lower = col.strip().lower()
        if col_lower in COL_ALIASES:
            rename[col] = COL_ALIASES[col_lower]
        elif col.upper() in KEEP_COLS:
            rename[col] = col.upper()
    df = df.rename(columns=rename)
    return df


def parse_numeric(val):
    """Convert BLS suppressed values (*, **, #, N/A) to None, else to int/float."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s in ("*", "**", "#", "N/A", "", "-"):
        return None
    s = s.replace(",", "")
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def load_oes_year(yy: int) -> pd.DataFrame:
    """Download and parse one year of OES data. Returns filtered DataFrame."""
    full_year = 2000 + yy
    zip_path = RAW_DIR / f"oesm{yy}all.zip"
    url = f"https://www.bls.gov/oes/special-requests/oesm{yy}all.zip"
    download_file(url, zip_path)

    z = zipfile.ZipFile(zip_path)
    xlsx_files = [f for f in z.namelist() if f.endswith(".xlsx")]
    if not xlsx_files:
        raise FileNotFoundError(f"No XLSX in {zip_path.name}: {z.namelist()}")

    print(f"  Parsing {xlsx_files[0]} ...")
    data = z.read(xlsx_files[0])
    df = pd.read_excel(io.BytesIO(data), engine="openpyxl", dtype=str)
    df = normalize_columns(df)

    # Check required columns
    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in {full_year}: {missing}. Have: {list(df.columns)}")

    df = df[KEEP_COLS].copy()

    # Filter: cross-industry only, detailed SOC only
    df = df[df["NAICS"].str.strip() == "000000"]
    # Older files use "detail" (with trailing space), newer use "detailed"
    df = df[df["O_GROUP"].str.strip().isin(["detailed", "detail"])]

    # Filter area types: 1=National, 2=State, 4=Metro
    df["AREA_TYPE"] = pd.to_numeric(df["AREA_TYPE"], errors="coerce")
    df = df[df["AREA_TYPE"].isin([1, 2, 4])]

    # Convert numeric fields
    df["TOT_EMP"] = df["TOT_EMP"].apply(parse_numeric)
    df["A_MEAN"] = df["A_MEAN"].apply(parse_numeric)
    df["A_MEDIAN"] = df["A_MEDIAN"].apply(parse_numeric)

    # Drop rows with no employment data
    df = df.dropna(subset=["TOT_EMP"])

    # Normalize area codes
    df["AREA"] = df["AREA"].str.strip()
    df["AREA_TYPE"] = df["AREA_TYPE"].astype(int)
    df["OCC_CODE"] = df["OCC_CODE"].str.strip()
    df["OCC_TITLE"] = df["OCC_TITLE"].str.strip()
    df["AREA_TITLE"] = df["AREA_TITLE"].str.strip()

    # For metro areas, pad AREA to 7 digits (00 + 5-digit CBSA)
    def normalize_area_code(row):
        if row["AREA_TYPE"] == 4:
            return str(row["AREA"]).zfill(7)
        elif row["AREA_TYPE"] == 2:
            return str(row["AREA"]).zfill(2)  # state FIPS
        else:
            return str(row["AREA"])  # national = "99"

    df["AREA"] = df.apply(normalize_area_code, axis=1)
    df["year"] = full_year

    # Determine SOC version
    if full_year <= 2017:
        df["soc_version"] = 2010
    elif full_year == 2018:
        df["soc_version"] = 2010  # 2018 OES still uses SOC 2010 codes
    else:
        df["soc_version"] = 2018

    result = df[["year", "AREA", "AREA_TYPE", "AREA_TITLE", "OCC_CODE", "OCC_TITLE",
                 "TOT_EMP", "A_MEAN", "A_MEDIAN", "soc_version"]].copy()
    result.columns = ["year", "area_code", "area_type", "area_title", "occ_code",
                       "occ_title", "tot_emp", "a_mean", "a_median", "soc_version"]

    print(f"  {full_year}: {len(result):,} rows (nat={len(result[result['area_type']==1]):,}, "
          f"state={len(result[result['area_type']==2]):,}, "
          f"metro={len(result[result['area_type']==4]):,})")
    return result


def load_cip_soc_crosswalk() -> pd.DataFrame:
    """Download and parse the CIP 2020 -> SOC 2018 crosswalk."""
    dest = RAW_DIR / "CIP2020_SOC2018_Crosswalk.xlsx"
    url = "https://nces.ed.gov/ipeds/cipcode/Files/CIP2020_SOC2018_Crosswalk.xlsx"
    download_file(url, dest)

    df = pd.read_excel(dest, sheet_name="CIP-SOC", engine="openpyxl")

    # CIP codes are float (e.g. 51.3801). Convert to XX.XXXX string with leading zero.
    def format_cip(val):
        if pd.isna(val):
            return None
        s = f"{float(val):.4f}"
        parts = s.split(".")
        return f"{int(parts[0]):02d}.{parts[1]}"

    df["cipcode"] = df["CIP2020Code"].apply(format_cip)
    df["soc_code"] = df["SOC2018Code"].str.strip()
    df["soc_title"] = df["SOC2018Title"].str.strip()
    df["source"] = "official"

    result = df[["cipcode", "soc_code", "soc_title", "source"]].dropna(subset=["cipcode", "soc_code"])
    print(f"  CIP-SOC crosswalk: {len(result):,} mappings "
          f"({result['cipcode'].nunique():,} CIPs -> {result['soc_code'].nunique():,} SOCs)")
    return result


def load_soc_2010_to_2018() -> pd.DataFrame:
    """Download and parse the SOC 2010 -> 2018 crosswalk."""
    dest = RAW_DIR / "soc_2010_to_2018_crosswalk.xlsx"
    url = "https://www.bls.gov/soc/2018/soc_2010_to_2018_crosswalk.xlsx"
    download_file(url, dest)

    # Header row is at row 8 (0-indexed); read raw and find it
    df = pd.read_excel(dest, engine="openpyxl", header=None)
    # Find the row containing "2010 SOC Code"
    header_idx = None
    for i, row in df.iterrows():
        vals = [str(v).strip() for v in row.values if pd.notna(v)]
        if any("2010 SOC Code" in v for v in vals):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Could not find header row in SOC crosswalk")

    df.columns = [str(v).strip() for v in df.iloc[header_idx].values]
    df = df.iloc[header_idx + 1:].reset_index(drop=True)

    result = df[["2010 SOC Code", "2018 SOC Code"]].copy()
    result.columns = ["soc_2010", "soc_2018"]
    result = result.dropna()
    result["soc_2010"] = result["soc_2010"].str.strip()
    result["soc_2018"] = result["soc_2018"].str.strip()

    # Keep only valid SOC codes (XX-XXXX pattern)
    soc_pat = re.compile(r"^\d{2}-\d{4}$")
    result = result[result["soc_2010"].apply(lambda x: bool(soc_pat.match(x)))]
    result = result[result["soc_2018"].apply(lambda x: bool(soc_pat.match(x)))]
    result = result.drop_duplicates()

    print(f"  SOC 2010->2018 crosswalk: {len(result):,} mappings")
    return result


# ── Strict CIP-SOC overrides ────────────────────────────────────────────────
# For programs with clear, direct occupational outcomes, restrict the crosswalk
# to only the core practitioner SOC codes. This removes tangential mappings
# (e.g., postsecondary teachers) that inflate employment numbers.
#
# Format: CIP code (or prefix with *) -> list of allowed SOC codes
# CIP codes not listed here keep their full official crosswalk.
STRICT_CIP_SOC = {
    # ── Nursing ──────────────────────────────────────────────
    "51.3801": ["29-1141"],                    # Registered Nursing -> RNs
    "51.3802": ["29-1141", "29-1171"],         # Nursing Administration -> RN + NP
    "51.3803": ["29-1141", "29-1171"],         # Adult Health Nurse -> RN + NP
    "51.3805": ["29-1141", "29-1171"],         # Family Practice Nurse -> RN + NP
    "51.3806": ["29-1141", "29-1171"],         # Maternal/Child Health Nurse -> RN + NP
    "51.3807": ["29-1161"],                    # Nurse Midwife -> Nurse Midwives
    "51.3808": ["29-1141", "29-1171"],         # Nursing Science -> RN + NP
    "51.3809": ["29-1141", "29-1171"],         # Pediatric Nurse -> RN + NP
    "51.3810": ["29-1141", "29-1171"],         # Psychiatric/MH Nurse -> RN + NP
    "51.3811": ["29-1141", "29-1171"],         # Public Health Nurse -> RN + NP
    "51.3812": ["29-1141", "29-1171"],         # Perioperative Nurse -> RN + NP
    "51.3814": ["29-1141", "29-1171"],         # Critical Care Nursing -> RN + NP
    "51.3815": ["29-1141", "29-1171"],         # Occupational Health Nurse -> RN + NP
    "51.3816": ["29-1141", "29-1171"],         # ER/Trauma Nursing -> RN + NP
    "51.3818": ["29-1141", "29-1171"],         # Nursing Practice -> RN + NP
    "51.3819": ["29-1141", "29-1171"],         # Palliative Care Nursing -> RN + NP
    "51.3821": ["29-1141", "29-1171"],         # Geriatric Nurse -> RN + NP
    "51.3822": ["29-1141", "29-1171"],         # Women's Health Nurse -> RN + NP
    "51.3824": ["29-1141", "29-1171"],         # Forensic Nursing -> RN + NP
    "51.3899": ["29-1141", "29-1171"],         # Registered Nursing, Other -> RN + NP
    "51.3203": ["25-1072"],                    # Nursing Education -> Nursing Instructors
    "51.3901": ["29-2061"],                    # LPN/LVN Training -> LPNs
    "51.3902": ["31-1131"],                    # Nursing Assistant -> CNAs
    "51.3999": ["29-2061", "31-1131"],         # Practical Nursing, Other -> LPN + CNA

    # ── Dental ───────────────────────────────────────────────
    "51.0601": ["31-9091"],                    # Dental Assisting -> Dental Assistants
    "51.0602": ["29-1292"],                    # Dental Hygiene -> Dental Hygienists
    "51.0603": ["51-9081"],                    # Dental Lab Tech -> Dental Lab Technicians

    # ── Pharmacy ─────────────────────────────────────────────
    "51.0805": ["29-2052"],                    # Pharmacy Tech -> Pharmacy Technicians
    "51.2001": ["29-1051"],                    # Pharmacy -> Pharmacists
    "51.2008": ["29-1051"],                    # Clinical/Hospital Pharmacy -> Pharmacists

    # ── Physical/Occupational Therapy ────────────────────────
    "51.0806": ["31-2021"],                    # PT Assistant -> PT Assistants
    "51.2306": ["29-1122"],                    # Occupational Therapy -> OTs
    "51.2308": ["29-1123"],                    # Physical Therapy -> PTs
    "51.2605": ["31-2022"],                    # PT Aide -> PT Aides
    "51.0803": ["31-2011"],                    # OT Assistant -> OT Assistants

    # ── Speech/Audiology ─────────────────────────────────────
    "51.0202": ["29-1181"],                    # Audiology -> Audiologists
    "51.0203": ["29-1127"],                    # Speech-Language Pathology -> SLPs
    "51.0204": ["29-1127", "29-1181"],         # Audiology & SLP -> SLPs + Audiologists

    # ── Emergency Medical ────────────────────────────────────
    "51.0904": ["29-2042", "29-2043"],         # EMT/Paramedic -> EMTs + Paramedics

    # ── Respiratory ──────────────────────────────────────────
    "51.0908": ["29-1126"],                    # Respiratory Therapy -> Respiratory Therapists

    # ── Surgical ─────────────────────────────────────────────
    "51.0909": ["29-2055", "29-9093"],         # Surgical Tech -> Surg Techs + Surg Assistants

    # ── Radiology/Imaging ────────────────────────────────────
    "51.0907": ["29-1124", "29-2034"],         # Radiation Therapy -> Rad Therapists + Rad Techs
    "51.0910": ["29-2032"],                    # Diagnostic Sonography -> Sonographers
    "51.0911": ["29-2034"],                    # Radiologic Tech -> Radiologic Technologists
    "51.0912": ["29-1071"],                    # Physician Assistant -> PAs

    # ── Medical Assisting/Support ────────────────────────────
    "51.0801": ["31-9092"],                    # Medical/Clinical Assistant -> Medical Assistants
    "51.0802": ["29-2012"],                    # Clinical Lab Assistant -> Lab Technicians
    "51.0708": ["31-9094"],                    # Medical Transcription -> Transcriptionists
    "51.0710": ["43-6013"],                    # Medical Office Assistant -> Med Secretaries
    "51.1009": ["31-9097"],                    # Phlebotomy -> Phlebotomists
    "51.1801": ["29-2081"],                    # Opticianry -> Dispensing Opticians
    "51.1803": ["29-2057"],                    # Ophthalmic Tech -> Ophthalmic Med Techs

    # ── Clinical Lab ─────────────────────────────────────────
    "51.1004": ["29-2012"],                    # Clinical Lab Technician -> Lab Technicians
    "51.1005": ["29-2011"],                    # Clinical Lab Science/Med Tech -> Lab Technologists

    # ── Veterinary ───────────────────────────────────────────
    "01.8001": ["29-1131"],                    # Veterinary Medicine -> Veterinarians
    "01.8301": ["29-2056", "31-9096"],         # Vet Tech -> Vet Techs + Vet Assistants

    # ── Trades ───────────────────────────────────────────────
    "48.0508": ["51-4121", "51-4122"],         # Welding -> Welders + Welding Machine Ops
    "46.0503": ["47-2152"],                    # Plumbing -> Plumbers
    "12.0401": ["39-5012"],                    # Cosmetology -> Cosmetologists
    "12.0413": ["39-5011", "39-5012"],         # Cosmetology Instructor -> Barbers + Cosmetologists
}


def refine_crosswalk(cip_soc: pd.DataFrame) -> pd.DataFrame:
    """Apply strict CIP-SOC overrides for programs with clear occupational outcomes.

    For CIP codes listed in STRICT_CIP_SOC, removes all official SOC mappings
    except the specified core occupation codes.
    """
    override_count = 0
    removed_count = 0

    for cipcode, allowed_socs in STRICT_CIP_SOC.items():
        mask = cip_soc["cipcode"] == cipcode
        if not mask.any():
            continue

        before = mask.sum()
        # Keep only rows where soc_code is in the allowed list
        keep_mask = mask & cip_soc["soc_code"].isin(allowed_socs)
        remove_mask = mask & ~cip_soc["soc_code"].isin(allowed_socs)

        n_removed = remove_mask.sum()
        if n_removed > 0:
            cip_soc = cip_soc[~remove_mask].copy()
            override_count += 1
            removed_count += n_removed

    print(f"  Strict overrides: {override_count} CIP codes refined, "
          f"{removed_count} tangential mappings removed")
    return cip_soc


def expand_crosswalk(cip_soc: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """Expand the CIP-SOC crosswalk with 'less strict' mappings.

    Adds:
    1. Broad-group expansion: if a CIP maps to 29-1141, also map to other 29-114X codes
    2. Management variants: if a CIP maps to any occupation, also consider 11-XXXX management
       occupations in the same broad group (e.g., if 51.3801 maps to 29-1141 RN, also map
       to 11-9111 Medical and Health Services Managers)
    """
    # Get all known detailed SOC codes from the OES data
    existing_socs = set()
    try:
        rows = conn.execute("SELECT DISTINCT occ_code FROM oes_employment").fetchall()
        existing_socs = {r[0] for r in rows}
    except Exception:
        pass

    new_rows = []

    # --- Broad group expansion ---
    # Group CIP-SOC pairs by broad group (first 6 chars, e.g. "29-114" from "29-1141")
    # This is more conservative than minor group (5 chars) and avoids pulling in
    # unrelated occupations within the same minor group (e.g. all postsecondary teachers)
    for cipcode, group_df in cip_soc.groupby("cipcode"):
        broad_groups = set()
        for soc in group_df["soc_code"]:
            broad = soc[:6]  # e.g. "29-114" from "29-1141"
            broad_groups.add(broad)

        # For each broad group, find other detailed codes in that group from OES
        for broad in broad_groups:
            existing_in_broad = {s for s in existing_socs if s.startswith(broad)}
            already_mapped = set(group_df["soc_code"])
            new_socs = existing_in_broad - already_mapped
            for soc in new_socs:
                new_rows.append({
                    "cipcode": cipcode,
                    "soc_code": soc,
                    "soc_title": "",  # will be filled from OES data later
                    "source": "expanded_broad",
                })

    # --- Curated management mappings ---
    # Map broad occupation families to related management SOC codes
    # These are well-known management occupations that the strict crosswalk often misses
    MANAGEMENT_MAPPINGS = {
        # Healthcare occupations -> Health services managers
        "29-": ["11-9111"],
        # Education occupations -> Education administrators
        "25-": ["11-9032", "11-9033", "11-9039"],
        # Computer occupations -> IT managers
        "15-": ["11-3021"],
        # Business/Financial -> Financial managers
        "13-": ["11-3031"],
        # Engineering -> Architectural/Engineering managers
        "17-": ["11-9041"],
        # Social/Community -> Social/Community service managers
        "21-": ["11-9151"],
    }

    for cipcode, group_df in cip_soc.groupby("cipcode"):
        already_mapped = set(group_df["soc_code"])
        families = {soc[:3] for soc in group_df["soc_code"]}  # e.g. {"29-"}

        for family_prefix, mgmt_codes in MANAGEMENT_MAPPINGS.items():
            if family_prefix in families:
                for mgmt_soc in mgmt_codes:
                    if mgmt_soc not in already_mapped and mgmt_soc in existing_socs:
                        new_rows.append({
                            "cipcode": cipcode,
                            "soc_code": mgmt_soc,
                            "soc_title": "",
                            "source": "expanded_management",
                        })

    if new_rows:
        expanded = pd.DataFrame(new_rows).drop_duplicates(subset=["cipcode", "soc_code"])
        # Fill in SOC titles from OES data
        try:
            title_rows = conn.execute(
                "SELECT DISTINCT occ_code, occ_title FROM oes_employment"
            ).fetchall()
            title_map = {r[0]: r[1] for r in title_rows}
            expanded["soc_title"] = expanded["soc_code"].map(title_map).fillna("")
        except Exception:
            pass

        print(f"  Expanded crosswalk: +{len(expanded):,} new mappings "
              f"(broad_group={len(expanded[expanded['source']=='expanded_broad']):,}, "
              f"management={len(expanded[expanded['source']=='expanded_management']):,})")
        result = pd.concat([cip_soc, expanded], ignore_index=True)
    else:
        result = cip_soc

    # --- Award-level differentiation ---
    # Assign awlevel_group: "all" (default), "graduate"
    # "all" = relevant at any education level (shows for undergrad and graduate)
    # "graduate" = additional roles that only show when graduate programs are selected
    #
    # Strategy: Official crosswalk entries and broad-group expansions stay "all"
    # because the CIP-SOC crosswalk is not education-level-specific — occupations
    # can correspond to CIP codes at both undergraduate and graduate levels.
    # Only expanded management roles are tagged "graduate" since they represent
    # career advancement paths more typical of graduate-level outcomes.
    result["awlevel_group"] = "all"  # default: relevant at any level

    # Expanded management roles are graduate-leaning (career advancement)
    result.loc[
        result["source"] == "expanded_management",
        "awlevel_group",
    ] = "graduate"

    grad_ct = (result["awlevel_group"] == "graduate").sum()
    all_ct = (result["awlevel_group"] == "all").sum()
    print(f"  Award-level groups: {all_ct:,} all-level + {grad_ct:,} graduate-leaning")

    return result


def create_tables(conn: sqlite3.Connection):
    """Create the OES and crosswalk tables."""
    conn.executescript("""
        DROP TABLE IF EXISTS oes_employment;
        CREATE TABLE oes_employment (
            year        INTEGER NOT NULL,
            area_code   TEXT    NOT NULL,
            area_type   INTEGER NOT NULL,
            area_title  TEXT    NOT NULL,
            occ_code    TEXT    NOT NULL,
            occ_title   TEXT    NOT NULL,
            tot_emp     INTEGER,
            a_mean      INTEGER,
            a_median    INTEGER,
            soc_version INTEGER NOT NULL DEFAULT 2018,
            PRIMARY KEY (year, area_code, occ_code)
        );
        CREATE INDEX IF NOT EXISTS idx_oes_occ ON oes_employment(occ_code);
        CREATE INDEX IF NOT EXISTS idx_oes_area ON oes_employment(area_code, area_type);
        CREATE INDEX IF NOT EXISTS idx_oes_year ON oes_employment(year);

        DROP TABLE IF EXISTS cip_soc_crosswalk;
        CREATE TABLE cip_soc_crosswalk (
            cipcode        TEXT NOT NULL,
            soc_code       TEXT NOT NULL,
            soc_title      TEXT,
            source         TEXT NOT NULL DEFAULT 'official',
            awlevel_group  TEXT NOT NULL DEFAULT 'all',
            PRIMARY KEY (cipcode, soc_code)
        );
        CREATE INDEX IF NOT EXISTS idx_cipsoc_cip ON cip_soc_crosswalk(cipcode);
        CREATE INDEX IF NOT EXISTS idx_cipsoc_soc ON cip_soc_crosswalk(soc_code);

        DROP TABLE IF EXISTS soc_2010_to_2018;
        CREATE TABLE soc_2010_to_2018 (
            soc_2010 TEXT NOT NULL,
            soc_2018 TEXT NOT NULL,
            PRIMARY KEY (soc_2010, soc_2018)
        );
    """)


def main():
    print("=" * 60)
    print("BLS OES Data Loader")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    conn.commit()

    # ── Stage 1: Load 10 years of OES employment data ────────────────────────
    print("\n[1/4] Downloading and parsing OES employment data...")
    all_dfs = []
    for yy in OES_YEARS:
        try:
            df = load_oes_year(yy)
            all_dfs.append(df)
        except Exception as e:
            print(f"  ERROR loading 20{yy}: {e}")
            continue

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        print(f"\n  Total OES rows: {len(combined):,}")
        combined.to_sql("oes_employment", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_oes_occ ON oes_employment(occ_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_oes_area ON oes_employment(area_code, area_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_oes_year ON oes_employment(year)")
        conn.commit()
        print("  Loaded into oes_employment table.")

    # ── Stage 2: Load CIP-SOC crosswalk ──────────────────────────────────────
    print("\n[2/4] Loading CIP-SOC crosswalk...")
    cip_soc = load_cip_soc_crosswalk()

    # ── Stage 3: Load SOC 2010->2018 crosswalk ────────────────────────────────
    print("\n[3/4] Loading SOC 2010->2018 crosswalk...")
    soc_xw = load_soc_2010_to_2018()
    soc_xw.to_sql("soc_2010_to_2018", conn, if_exists="replace", index=False)
    conn.commit()
    print("  Loaded into soc_2010_to_2018 table.")

    # ── Stage 4: Refine + expand crosswalk ─────────────────────────────────
    print("\n[4/4] Refining and expanding crosswalk...")
    cip_soc = refine_crosswalk(cip_soc)
    cip_soc_expanded = expand_crosswalk(cip_soc, conn)
    cip_soc_expanded.to_sql("cip_soc_crosswalk", conn, if_exists="replace", index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cipsoc_cip ON cip_soc_crosswalk(cipcode)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cipsoc_soc ON cip_soc_crosswalk(soc_code)")
    conn.commit()

    # ── Summary ──────────────────────────────────────────────────────────────
    oes_ct = conn.execute("SELECT COUNT(*) FROM oes_employment").fetchone()[0]
    cip_ct = conn.execute("SELECT COUNT(*) FROM cip_soc_crosswalk").fetchone()[0]
    soc_ct = conn.execute("SELECT COUNT(*) FROM soc_2010_to_2018").fetchone()[0]
    off_ct = conn.execute("SELECT COUNT(*) FROM cip_soc_crosswalk WHERE source='official'").fetchone()[0]
    exp_ct = cip_ct - off_ct

    print("\n" + "=" * 60)
    print("DONE!")
    print(f"  oes_employment:    {oes_ct:>10,} rows")
    print(f"  cip_soc_crosswalk: {cip_ct:>10,} rows ({off_ct:,} official + {exp_ct:,} expanded)")
    print(f"  soc_2010_to_2018:  {soc_ct:>10,} rows")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
