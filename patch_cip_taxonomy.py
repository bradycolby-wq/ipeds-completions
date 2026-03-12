"""
Download NCES CIP 2020 taxonomy and load 6-digit code descriptions
into the cip_taxonomy table in ipeds.db. Run once.
"""

import io
import re
import sqlite3
import urllib.request
from pathlib import Path

import openpyxl

DB_PATH = Path(__file__).parent / "ipeds.db"
# CIP-SOC crosswalk contains CIP2020Code + CIP2020Title columns
CIP_URL = "https://nces.ed.gov/ipeds/cipcode/Files/CIP2020_SOC2018_Crosswalk.xlsx"


def download_and_parse():
    print("Downloading CIP 2020 taxonomy...")
    req = urllib.request.Request(CIP_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        data = r.read()
    print(f"  Downloaded {len(data)/1024:.0f} KB")

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb["CIP-SOC"]

    cip_data = {}  # use dict to deduplicate

    header = None
    for row in ws.iter_rows(values_only=True):
        cells = [str(c).strip() if c is not None else "" for c in row]
        if header is None:
            header = cells
            try:
                code_idx  = header.index("CIP2020Code")
                title_idx = header.index("CIP2020Title")
            except ValueError:
                print("ERROR: Expected columns CIP2020Code, CIP2020Title not found")
                wb.close()
                return []
            continue

        code  = cells[code_idx]  if code_idx  < len(cells) else ""
        title = cells[title_idx] if title_idx < len(cells) else ""

        # Keep only 6-digit codes: XX.XXXX
        if re.match(r"^\d{2}\.\d{4}$", code) and title and title not in ("nan", "None"):
            # Strip trailing period that NCES sometimes adds
            cip_data[code] = title.rstrip(".")

    wb.close()
    result = sorted(cip_data.items())
    print(f"  Parsed {len(result)} distinct 6-digit CIP codes from 'CIP-SOC' sheet")
    return result


def load_to_db(cip_data):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cip_taxonomy (
            cipcode  TEXT PRIMARY KEY,
            ciptitle TEXT
        )
    """)
    conn.executemany(
        "INSERT OR REPLACE INTO cip_taxonomy (cipcode, ciptitle) VALUES (?,?)",
        cip_data,
    )
    conn.commit()

    # How many DB completions codes got a name?
    matched = conn.execute("""
        SELECT COUNT(DISTINCT c.cipcode)
        FROM (SELECT DISTINCT cipcode FROM completions) c
        JOIN cip_taxonomy t ON c.cipcode = t.cipcode
    """).fetchone()[0]
    total_db = conn.execute(
        "SELECT COUNT(DISTINCT cipcode) FROM completions"
    ).fetchone()[0]
    conn.close()

    print(f"  Loaded {len(cip_data):,} CIP 2020 code descriptions")
    print(f"  Matched {matched:,} / {total_db:,} codes in completions DB")


def main():
    cip_data = download_and_parse()
    if not cip_data:
        print("ERROR: No 6-digit CIP codes parsed. Check file format.")
        return
    load_to_db(cip_data)
    print("Done.")


if __name__ == "__main__":
    main()
