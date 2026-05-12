"""Produce a slimmed copy of ipeds.db for deployment on Render.

The full DB carries 11 years of redistributed completions
(completions_by_state + completions_by_metro = ~54M rows, ~4.6 GB).
The deployed rankings only need the 3 anchor years that drive trend
metrics: earliest (2014), post-COVID base (2021), and latest (2024).

Approach: instead of DELETE-then-VACUUM (slow on a 51M-row indexed
table — each row update touches the index), we copy ipeds.db then
rewrite the two redistributed tables in place using CREATE TABLE AS
SELECT, dropping/recreating their indexes. Then VACUUM compacts.

All other tables (raw completions for every year, institutions,
nc_sara_enrollment, etc.) are kept intact — they're either small or
power features unrelated to rankings.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import time

SOURCE = "ipeds.db"
TARGET = "ipeds_render.db"

# Years the rankings actually use:
#   earliest   = 2014 -> completions_long_trend base
#   pc_base    = 2021 -> completions_pc_trend base
#   latest     = 2024 -> current volume + both trend endpoints
KEEP_YEARS = (2014, 2021, 2024)

REDIST_TABLES = {
    "completions_by_state": {
        "key_col": "dest_state",
        "indexes": [
            ("idx_cbs_year_cip_aw",
             "CREATE INDEX idx_cbs_year_cip_aw "
             "ON completions_by_state(year, cipcode, awlevel)"),
            ("idx_cbs_state",
             "CREATE INDEX idx_cbs_state ON completions_by_state(dest_state)"),
        ],
    },
    "completions_by_metro": {
        "key_col": "dest_cbsa",
        "indexes": [
            ("idx_cbm_year_cip_aw",
             "CREATE INDEX idx_cbm_year_cip_aw "
             "ON completions_by_metro(year, cipcode, awlevel)"),
            ("idx_cbm_cbsa",
             "CREATE INDEX idx_cbm_cbsa ON completions_by_metro(dest_cbsa)"),
        ],
    },
}


def main() -> None:
    if not os.path.exists(SOURCE):
        raise FileNotFoundError(SOURCE)
    if os.path.exists(TARGET):
        os.remove(TARGET)

    print(f"Copying {SOURCE} -> {TARGET} "
          f"({os.path.getsize(SOURCE) / 1e9:.2f} GB)...", flush=True)
    t = time.time()
    shutil.copy2(SOURCE, TARGET)
    print(f"  copied in {time.time() - t:.0f}s", flush=True)

    conn = sqlite3.connect(TARGET)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode = MEMORY")
    cur.execute("PRAGMA synchronous = OFF")
    cur.execute("PRAGMA temp_store = MEMORY")
    cur.execute("PRAGMA cache_size = -2000000")  # ~2 GB page cache

    ph = ",".join("?" * len(KEEP_YEARS))
    for tbl, spec in REDIST_TABLES.items():
        key_col = spec["key_col"]
        before = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"\nRewriting {tbl} (currently {before:,} rows)...", flush=True)

        # Drop indexes first so the CTAS doesn't pay per-row index updates.
        for idx_name, _ in spec["indexes"]:
            cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
        cur.execute(f"ALTER TABLE {tbl} RENAME TO {tbl}_old")

        t = time.time()
        cur.execute(
            f"""
            CREATE TABLE {tbl} AS
            SELECT year, cipcode, awlevel, {key_col}, completions
            FROM {tbl}_old
            WHERE year IN ({ph})
            """,
            KEEP_YEARS,
        )
        cur.execute(f"DROP TABLE {tbl}_old")
        conn.commit()
        after = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  CTAS done in {time.time() - t:.0f}s: "
              f"{before:,} -> {after:,} rows", flush=True)

        t = time.time()
        for _, idx_sql in spec["indexes"]:
            cur.execute(idx_sql)
        conn.commit()
        print(f"  indexes rebuilt in {time.time() - t:.0f}s", flush=True)

    print("\nVACUUM...", flush=True)
    t = time.time()
    conn.execute("VACUUM")
    conn.close()
    print(f"  VACUUM done in {time.time() - t:.0f}s", flush=True)

    final = os.path.getsize(TARGET)
    src = os.path.getsize(SOURCE)
    print(f"\nFinal: {TARGET} = {final / 1e9:.2f} GB "
          f"(source: {src / 1e9:.2f} GB, saved {(src - final) / 1e9:.2f} GB)")


if __name__ == "__main__":
    main()
