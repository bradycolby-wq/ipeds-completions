"""
Download Census Bureau CBSA delineation file and populate cbsanm
in the institutions table. Run once after setup_ipeds.py.
"""

import sqlite3
import urllib.request
import io
from pathlib import Path

try:
    import openpyxl
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "-q"])
    import openpyxl

DB_PATH = Path(__file__).parent / "ipeds.db"

# Census Bureau CBSA delineation file (2023 vintage — stable codes)
CENSUS_URL = (
    "https://www2.census.gov/programs-surveys/metro-micro/"
    "geographies/reference-files/2023/delineation-files/list1_2023.xlsx"
)


def download_cbsa_lookup():
    print("Downloading Census CBSA delineation file...")
    req = urllib.request.Request(CENSUS_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        data = r.read()
    print(f"  Downloaded {len(data)/1024:.0f} KB")

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active

    cbsa_map = {}   # cbsa_code (str) → cbsa_name (str)
    header_row = None

    for row in ws.iter_rows(values_only=True):
        # Find header row (contains "CBSA Code" or "CBSA Title")
        if header_row is None:
            row_strs = [str(c).strip() if c else "" for c in row]
            if any("CBSA Code" in s for s in row_strs):
                header_row = row_strs
                # Find column indices
                try:
                    code_idx  = next(i for i, s in enumerate(row_strs) if "CBSA Code" in s)
                    title_idx = next(i for i, s in enumerate(row_strs) if "CBSA Title" in s)
                except StopIteration:
                    header_row = None
            continue

        if header_row is None:
            continue

        code  = str(row[code_idx]).strip()  if row[code_idx]  else ""
        title = str(row[title_idx]).strip() if row[title_idx] else ""
        if code and title and code.isdigit() and len(code) == 5:
            cbsa_map[code] = title

    wb.close()
    print(f"  Loaded {len(cbsa_map):,} CBSA code-to-name mappings")
    return cbsa_map


def update_db(cbsa_map):
    conn = sqlite3.connect(DB_PATH)

    # Add cbsa_names reference table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cbsa_names (
            cbsa        TEXT PRIMARY KEY,
            cbsanm      TEXT,
            cbsatype    TEXT
        )
    """)

    # Populate reference table
    conn.executemany(
        "INSERT OR REPLACE INTO cbsa_names (cbsa, cbsanm) VALUES (?,?)",
        list(cbsa_map.items()),
    )

    # Update cbsanm in institutions table
    updated = 0
    for cbsa_code, cbsa_name in cbsa_map.items():
        cur = conn.execute(
            "UPDATE institutions SET cbsanm=? WHERE cbsa=? AND (cbsanm IS NULL OR cbsanm='')",
            (cbsa_name, cbsa_code),
        )
        updated += cur.rowcount

    conn.commit()

    # Verify
    n_named = conn.execute(
        "SELECT COUNT(*) FROM institutions WHERE cbsanm IS NOT NULL AND cbsanm != ''"
    ).fetchone()[0]
    n_total_cbsa = conn.execute(
        "SELECT COUNT(*) FROM institutions WHERE cbsa IS NOT NULL AND cbsa != ''"
    ).fetchone()[0]

    conn.close()
    print(f"  Updated {updated:,} institution rows with CBSA names")
    print(f"  {n_named:,} / {n_total_cbsa:,} institutions with CBSA now have names")


def main():
    cbsa_map = download_cbsa_lookup()
    update_db(cbsa_map)
    print("Done.")


if __name__ == "__main__":
    main()
