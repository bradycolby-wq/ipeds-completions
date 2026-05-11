"""
Load the LMI Institute Automation Exposure Index into the IPEDS SQLite
database.

Source: LMII-Automation-Exposure-Index-2019-OES.xlsx (LMI Institute,
https://www.lmiontheweb.org). Each row scores an O*NET-SOC detail code
(e.g. 11-1011.00) on five task-mix dimensions (Abstract Analytical,
Abstract Interpersonal, NonRoutine Manual, Routine Cognitive, Routine
Manual) and assigns a 1-10 automation risk category — 1 = least exposed
to automation, 10 = most exposed.

The IPEDS OES employment data uses BLS SOC codes without the O*NET
detail suffix (XX-XXXX, not XX-XXXX.YY), so we aggregate to that level
by preferring the .00 base row when present and averaging the detail
rows otherwise.

Creates one table:
  occ_automation_risk
      occ_code              TEXT PK    SOC 2018-style XX-XXXX
      risk_score            INTEGER    1-10
      abstract_analytical   REAL
      abstract_interpersonal REAL
      nonroutine_manual     REAL
      routine_cognitive     REAL
      routine_manual        REAL
      composite             REAL       Σ raw component scores
      n_onet_codes          INTEGER    # of O*NET rows aggregated
      source                TEXT
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent / "ipeds.db"
RAW_DIR = Path(__file__).parent / "raw"
SRC_FILE = RAW_DIR / "LMII_Automation_Exposure_Index_2019_OES.xlsx"

SHEET = "automation data"

# Raw columns in the LMII workbook
COL_ONET = "oes 2019"
COL_TITLE = "oes 2019 title"
COL_AA = "abstract analytical"
COL_AI = "abstract interpersonal"
COL_NRM = "nonroutine manual"
COL_RC = "routine cognitive"
COL_RM = "routine manual"
COL_DIFF = "abstract routine difference"
COL_RISK = "automation risk category (1 low, 10 high)"


def load_lmii() -> pd.DataFrame:
    if not SRC_FILE.exists():
        raise FileNotFoundError(
            f"Missing LMII workbook at {SRC_FILE}. Drop the file there and rerun."
        )
    df = pd.read_excel(SRC_FILE, sheet_name=SHEET, engine="openpyxl")
    df = df.rename(columns={c: c.strip() for c in df.columns})

    required = [COL_ONET, COL_TITLE, COL_AA, COL_AI, COL_NRM, COL_RC, COL_RM,
                COL_DIFF, COL_RISK]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in LMII sheet: {missing}. "
                       f"Have: {list(df.columns)}")

    df = df.dropna(subset=[COL_ONET, COL_RISK]).copy()
    df[COL_ONET] = df[COL_ONET].astype(str).str.strip()
    df["soc_code"] = df[COL_ONET].str.split(".").str[0]
    df["is_base"] = df[COL_ONET].str.endswith(".00")
    return df


def aggregate_to_soc(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate O*NET detail rows to BLS SOC codes.

    Prefer the .00 "base" row when present (it represents the headline
    occupation in O*NET). When the SOC only has specialty rows
    (e.g. 11-1011.03 Chief Sustainability Officers, no .00), average the
    components across the available rows.
    """
    out = []
    by_soc = defaultdict(list)
    for _, row in df.iterrows():
        by_soc[row["soc_code"]].append(row)

    for soc, rows in by_soc.items():
        base = [r for r in rows if r["is_base"]]
        chosen = base if base else rows
        n = len(rows)

        def avg(col):
            vals = [r[col] for r in chosen if pd.notna(r[col])]
            return sum(vals) / len(vals) if vals else None

        risk_vals = [r[COL_RISK] for r in chosen if pd.notna(r[COL_RISK])]
        risk = round(sum(risk_vals) / len(risk_vals)) if risk_vals else None

        out.append({
            "occ_code": soc,
            "risk_score": int(risk) if risk is not None else None,
            "abstract_analytical": avg(COL_AA),
            "abstract_interpersonal": avg(COL_AI),
            "nonroutine_manual": avg(COL_NRM),
            "routine_cognitive": avg(COL_RC),
            "routine_manual": avg(COL_RM),
            "composite": avg(COL_DIFF),
            "n_onet_codes": n,
            "source": "LMII 2019 OES Automation Exposure Index",
        })

    result = pd.DataFrame(out)
    result = result.dropna(subset=["risk_score"]).sort_values("occ_code")
    return result.reset_index(drop=True)


def create_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        DROP TABLE IF EXISTS occ_automation_risk;
        CREATE TABLE occ_automation_risk (
            occ_code               TEXT    PRIMARY KEY,
            risk_score             INTEGER NOT NULL,
            abstract_analytical    REAL,
            abstract_interpersonal REAL,
            nonroutine_manual      REAL,
            routine_cognitive      REAL,
            routine_manual         REAL,
            composite              REAL,
            n_onet_codes           INTEGER,
            source                 TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_occ_automation_risk_score
            ON occ_automation_risk(risk_score);
    """)


def main() -> None:
    print("=" * 60)
    print("LMII Automation Exposure Index Loader")
    print("=" * 60)

    print(f"\nReading {SRC_FILE.name} ...")
    raw = load_lmii()
    print(f"  Raw rows: {len(raw):,} O*NET codes")
    print(f"  Distinct SOC codes: {raw['soc_code'].nunique():,}")
    print(f"  With .00 base row : {raw['is_base'].sum():,}")

    agg = aggregate_to_soc(raw)
    print(f"\nAggregated rows: {len(agg):,} SOC codes")
    risk_dist = agg["risk_score"].value_counts().sort_index()
    print("  Risk distribution (1 low, 10 high):")
    for r, c in risk_dist.items():
        bar = "#" * int(c / max(risk_dist) * 30)
        print(f"    {int(r):>2}: {int(c):>4}  {bar}")

    conn = sqlite3.connect(DB_PATH)
    create_table(conn)
    agg.to_sql("occ_automation_risk", conn, if_exists="append", index=False)
    conn.commit()

    # Coverage report against oes_employment
    try:
        oes_socs = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT occ_code FROM oes_employment"
            ).fetchall()
        }
        matched = set(agg["occ_code"]) & oes_socs
        oes_unmatched = oes_socs - set(agg["occ_code"])
        print(
            f"\nCoverage vs oes_employment: "
            f"{len(matched):,} of {len(oes_socs):,} SOC codes have a risk score "
            f"({len(matched) / len(oes_socs):.0%}). "
            f"{len(oes_unmatched):,} OES SOCs unmatched."
        )
    except Exception:
        pass

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
