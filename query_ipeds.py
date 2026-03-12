#!/usr/bin/env python3
"""
IPEDS Query Helper
==================
Search completions data by CIP code, award level, state, institution type, etc.

Usage:
    python query_ipeds.py                        # interactive prompts
    python query_ipeds.py --help                 # show all flags

Examples:
    # Nursing bachelor's degrees in all states, 2019-2023
    python query_ipeds.py --cip 51.38 --level 5 --years 2019-2023

    # Computer Science (all levels) in Texas at public institutions
    python query_ipeds.py --cip 11.07 --state TX --control 1

    # All health programs at 4-year institutions, grouped by state
    python query_ipeds.py --cip 51 --iclevel 1 --group state

    # Business bachelor's and master's, national totals by year
    python query_ipeds.py --cip 52 --level 5,7 --group year
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "ipeds.db"

# ── Reference data ──────────────────────────────────────────────────────────
AWARD_LEVELS = {
    1:  "< 1-yr certificate",
    2:  "1-2 yr certificate",
    3:  "Associate's",
    4:  "2-4 yr certificate",
    5:  "Bachelor's",
    6:  "Post-bacc certificate",
    7:  "Master's",
    8:  "Post-master's certificate",
    17: "Doctorate (research)",
    18: "Doctorate (professional)",
    19: "Doctorate (other)",
}

SECTORS = {
    1: "Public 4-yr+",    2: "Private nonprofit 4-yr+", 3: "Private for-profit 4-yr+",
    4: "Public 2-yr",     5: "Private nonprofit 2-yr",  6: "Private for-profit 2-yr",
    7: "Public <2-yr",    8: "Private nonprofit <2-yr", 9: "Private for-profit <2-yr",
}

CONTROLS = {1: "Public", 2: "Private nonprofit", 3: "Private for-profit"}
ICLEVELS  = {1: "4-year+", 2: "2-year", 3: "<2-year"}


# ── Core query function ──────────────────────────────────────────────────────
def query(
    cip=None,           # str or list: "11", "11.07", ["51.38", "51.39"]
    awlevel=None,       # int or list: 5, [5, 7]
    stabbr=None,        # str or list: "CA", ["CA","TX"]
    sector=None,        # int or list
    control=None,       # int or list: 1=public, 2=nonprofit, 3=for-profit
    iclevel=None,       # int or list: 1=4yr, 2=2yr, 3=<2yr
    year_start=None,    # int
    year_end=None,      # int
    exclude_closed=True,
    first_major_only=True,
    group_by=None,      # list of columns to group by, e.g. ["year","stabbr"]
    order_by="year, ctotalt DESC",
    limit=None,
):
    if not DB_PATH.exists():
        sys.exit(f"Database not found at {DB_PATH}\nRun setup_ipeds.py first.")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    where, params = [], []

    def add_filter(col, val):
        if val is None:
            return
        if isinstance(val, (list, tuple)):
            placeholders = ",".join("?" * len(val))
            where.append(f"{col} IN ({placeholders})")
            params.extend(val)
        else:
            where.append(f"{col} = ?")
            params.append(val)

    # CIP code: prefix match if no decimal, exact if decimal present
    if cip is not None:
        cip_list = [cip] if isinstance(cip, str) else list(cip)
        cip_clauses = []
        for c in cip_list:
            if "." in str(c):
                cip_clauses.append("cipcode LIKE ?")
                params.append(f"{c}%")
            else:
                cip_clauses.append("cipcode LIKE ?")
                params.append(f"{c}.%")
        where.append(f"({' OR '.join(cip_clauses)})")

    add_filter("awlevel", awlevel if not isinstance(awlevel, list) else awlevel)
    if isinstance(awlevel, list):
        where.pop()  # undo add_filter above
        params = params[:-len(awlevel)]
        placeholders = ",".join("?" * len(awlevel))
        where.append(f"awlevel IN ({placeholders})")
        params.extend(awlevel)

    if stabbr is not None:
        s = [stabbr.upper()] if isinstance(stabbr, str) else [x.upper() for x in stabbr]
        add_filter("stabbr", s if len(s) > 1 else s[0])

    add_filter("sector",  sector)
    add_filter("control", control)
    add_filter("iclevel", iclevel)

    if year_start:
        where.append("year >= ?"); params.append(year_start)
    if year_end:
        where.append("year <= ?"); params.append(year_end)
    if exclude_closed:
        where.append("(closeind IS NULL OR closeind = 0)")
    if first_major_only:
        where.append("majornum = 1")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    if group_by:
        cols   = ", ".join(group_by)
        select = f"{cols}, SUM(ctotalt) AS total_completions"
        sql = f"""
            SELECT {select}
            FROM completions_view
            {where_sql}
            GROUP BY {cols}
            ORDER BY {order_by or cols}
        """
    else:
        sql = f"""
            SELECT year, unitid, instnm, city, stabbr,
                   sector_name, iclevel_name, control_name,
                   carnegie, locale_name, cipcode, majornum,
                   awlevel, award_level_name, ctotalt
            FROM completions_view
            {where_sql}
            ORDER BY {order_by}
        """

    if limit:
        sql += f" LIMIT {int(limit)}"

    cur  = conn.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Display helpers ──────────────────────────────────────────────────────────
def print_table(rows, max_col_width=40):
    if not rows:
        print("No results.")
        return
    keys = list(rows[0].keys())
    widths = {k: min(max(len(k), max(len(str(r.get(k, "") or "")) for r in rows)), max_col_width)
              for k in keys}
    header = "  ".join(k.upper().ljust(widths[k]) for k in keys)
    sep    = "  ".join("-" * widths[k] for k in keys)
    print(header)
    print(sep)
    for row in rows:
        line = "  ".join(str(row.get(k, "") or "").ljust(widths[k])[:max_col_width] for k in keys)
        print(line)
    print(f"\n{len(rows):,} row(s)")


def export_csv(rows, path):
    if not rows:
        print("Nothing to export.")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"Exported {len(rows):,} rows to {path}")


# ── Reference lookups ────────────────────────────────────────────────────────
def show_award_levels():
    print("\nAward Level Codes:")
    for k, v in AWARD_LEVELS.items():
        print(f"  {k:>3}  {v}")

def show_sectors():
    print("\nSector Codes:")
    for k, v in SECTORS.items():
        print(f"  {k:>3}  {v}")

def show_controls():
    print("\nControl Codes:"); [print(f"  {k}  {v}") for k, v in CONTROLS.items()]

def show_iclevels():
    print("\nIC Level Codes:"); [print(f"  {k}  {v}") for k, v in ICLEVELS.items()]

def show_cip_sample():
    if not DB_PATH.exists():
        return
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT cipcode FROM completions ORDER BY cipcode LIMIT 30"
    ).fetchall()
    conn.close()
    print("\nSample CIP codes in database (first 30):")
    for r in rows:
        print(f"  {r[0]}")


# ── CLI ──────────────────────────────────────────────────────────────────────
def parse_int_list(s):
    return [int(x.strip()) for x in s.split(",")]

def parse_str_list(s):
    return [x.strip().upper() for x in s.split(",")]

def parse_year_range(s):
    if "-" in s:
        parts = s.split("-")
        return int(parts[0]), int(parts[1])
    return int(s), int(s)


def main():
    ap = argparse.ArgumentParser(
        description="Query IPEDS completions data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--cip",      help="CIP prefix(es), comma-separated. E.g. 51.38 or 11,52")
    ap.add_argument("--level",    help="Award level code(s). E.g. 5 or 5,7  (--levels to list)")
    ap.add_argument("--state",    help="State abbreviation(s). E.g. CA or CA,TX")
    ap.add_argument("--sector",   help="Sector code(s)  (--sectors to list)")
    ap.add_argument("--control",  help="Control code(s): 1=public, 2=nonprofit, 3=for-profit")
    ap.add_argument("--iclevel",  help="Level code: 1=4yr, 2=2yr, 3=<2yr")
    ap.add_argument("--years",    help="Year range. E.g. 2019-2023 or 2022")
    ap.add_argument("--group",    help="Group-by columns, comma-separated. E.g. year,stabbr")
    ap.add_argument("--all-majors", action="store_true", help="Include second majors (default: first only)")
    ap.add_argument("--include-closed", action="store_true", help="Include closed institutions")
    ap.add_argument("--limit",    type=int, help="Max rows to return")
    ap.add_argument("--export",   help="Export results to CSV file path")
    # Reference flags
    ap.add_argument("--levels",   action="store_true", help="List award level codes and exit")
    ap.add_argument("--sectors",  action="store_true", help="List sector codes and exit")
    ap.add_argument("--controls", action="store_true", help="List control codes and exit")
    ap.add_argument("--cip-sample", action="store_true", help="Show sample CIP codes in DB")

    args = ap.parse_args()

    # Reference info flags
    if args.levels:   show_award_levels(); return
    if args.sectors:  show_sectors(); return
    if args.controls: show_controls(); return
    if args.cip_sample: show_cip_sample(); return

    if not DB_PATH.exists():
        sys.exit(f"\nDatabase not found: {DB_PATH}\nRun setup_ipeds.py first.\n")

    # Parse args
    cip_arg     = [c.strip() for c in args.cip.split(",")] if args.cip else None
    level_arg   = parse_int_list(args.level)  if args.level   else None
    state_arg   = parse_str_list(args.state)  if args.state   else None
    sector_arg  = parse_int_list(args.sector) if args.sector  else None
    control_arg = parse_int_list(args.control)if args.control else None
    iclevel_arg = parse_int_list(args.iclevel)if args.iclevel else None
    group_arg   = [g.strip() for g in args.group.split(",")] if args.group else None

    year_start = year_end = None
    if args.years:
        year_start, year_end = parse_year_range(args.years)

    # Unwrap single-element lists
    def unwrap(x):
        return x[0] if x and len(x) == 1 else x

    rows = query(
        cip           = cip_arg,
        awlevel       = unwrap(level_arg),
        stabbr        = unwrap(state_arg),
        sector        = unwrap(sector_arg),
        control       = unwrap(control_arg),
        iclevel       = unwrap(iclevel_arg),
        year_start    = year_start,
        year_end      = year_end,
        exclude_closed= not args.include_closed,
        first_major_only = not args.all_majors,
        group_by      = group_arg,
        limit         = args.limit,
    )

    if args.export:
        export_csv(rows, args.export)
    else:
        print_table(rows)


if __name__ == "__main__":
    main()
