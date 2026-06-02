"""Normalize metro (CBSA) codes that change across years.

Background
----------
IPEDS adopted the 2020 OMB CBSA re-delineation in its 2023 HD files, and BLS
OES adopted it in 2024. Metros were renumbered (e.g. Cleveland 17460 -> 17410,
Dayton 19380 -> 19430). The app keys every metro view off the per-year CBSA
code and the dropdown only offers the current code, so without normalization a
selected metro returns only the years whose code happens to match — older years
silently vanish.

Two domains, two safe strategies
--------------------------------
1. Institutions (completions): canonicalize each institution to its OWN most
   recent valid CBSA across all of its years. This is exact — no code-level
   guessing — so it is correct even when an institution genuinely relocated.
   Only institutions that ever carried a now-deprecated code are touched.

2. OES employment (no per-institution rows): fold an old area code into a new
   one ONLY when the OES data itself shows a clean year handoff (old code's
   years all precede the new code's years, no overlap). Candidate pairs come
   from the institution crosswalk; the handoff test independently rejects
   coincidental / relocation pairs (e.g. it correctly refuses Madera->Fresno,
   which overlap, and would refuse Sumter->Gallipolis).

The implied deprecated->canonical pairs are persisted to `cbsa_crosswalk` so the
OES step can run later in the build pipeline (after institutions have already
been canonicalized).

Idempotent: safe to run repeatedly. Run after setup_ipeds.py and load_oes_data.py
(or rely on those scripts calling these functions).

    python normalize_cbsa.py
"""

import sqlite3
from collections import defaultdict, Counter
from pathlib import Path

DB_PATH = Path(__file__).parent / "ipeds.db"


def _valid(code) -> bool:
    return code is not None and str(code).strip().isdigit() and int(code) > 0


def _institution_history(conn):
    """Return {unitid: [(year, cbsa), ...]} sorted by year, valid codes only."""
    rows = conn.execute(
        "SELECT unitid, year, cbsa FROM institutions "
        "WHERE cbsa IS NOT NULL ORDER BY unitid, year"
    ).fetchall()
    hist = defaultdict(list)
    for u, y, cb in rows:
        if _valid(cb):
            hist[u].append((y, cb))
    return hist


def canonicalize_institutions(conn, verbose=True):
    """Set every institution's cbsa/cbsanm to its most-recent valid value, for
    institutions that ever carried a now-deprecated code. Persists the implied
    deprecated->canonical pairs to the `cbsa_crosswalk` table.
    """
    latest_year = conn.execute("SELECT MAX(year) FROM institutions").fetchone()[0]
    current = {
        r[0] for r in conn.execute(
            "SELECT DISTINCT cbsa FROM institutions WHERE year = ?", (latest_year,)
        ) if _valid(r[0])
    }
    hist = _institution_history(conn)

    # Names: prefer the authoritative cbsa_names, else the institution's own.
    cbsa_names = dict(conn.execute("SELECT cbsa, cbsanm FROM cbsa_names").fetchall())

    crosswalk = Counter()          # (old_code -> canonical_code) : n_institutions
    updates = []                   # (canon_cbsa, canon_name, unitid)
    for u, seq in hist.items():
        codes = {cb for _, cb in seq}
        deprecated_here = codes - current
        if not deprecated_here:
            continue               # nothing stale — leave untouched
        canon_cbsa = seq[-1][1]    # most recent valid code
        # Canonical display name
        canon_name = cbsa_names.get(canon_cbsa)
        if not canon_name:
            row = conn.execute(
                "SELECT cbsanm FROM institutions WHERE unitid = ? AND cbsa = ? "
                "AND cbsanm IS NOT NULL AND cbsanm != '' ORDER BY year DESC LIMIT 1",
                (u, canon_cbsa),
            ).fetchone()
            canon_name = row[0] if row else ""
        updates.append((canon_cbsa, canon_name, u))
        for d in deprecated_here:
            if d != canon_cbsa:
                crosswalk[(d, canon_cbsa)] += 1

    conn.executemany(
        "UPDATE institutions SET cbsa = ?, cbsanm = ? WHERE unitid = ?", updates
    )

    # Per-unitid canonicalization leaves CLOSED institutions (last seen before
    # the re-delineation) stranded on the old code. For pairs with strong,
    # unambiguous evidence (>=3 institutions transitioned old->new, dominant
    # target), apply a code-level remap so those institutions' historical
    # completions also roll up under the current metro. Low-support pairs are
    # left alone — they are individual relocations, not metro re-delineations.
    agg = defaultdict(Counter)
    for (old, new), n in crosswalk.items():
        agg[old][new] += n
    CONFIDENT_MIN = 3
    confident = {}
    for old, targets in agg.items():
        new, n = targets.most_common(1)[0]
        if n >= CONFIDENT_MIN:
            confident[old] = new
    for old, new in confident.items():
        name = cbsa_names.get(new) or ""
        conn.execute(
            "UPDATE institutions SET cbsa = ?, cbsanm = COALESCE(NULLIF(?, ''), cbsanm) "
            "WHERE cbsa = ?", (new, name, old),
        )

    # Persist the crosswalk for the OES step / auditing.
    conn.execute("DROP TABLE IF EXISTS cbsa_crosswalk")
    conn.execute(
        "CREATE TABLE cbsa_crosswalk ("
        "old_cbsa TEXT, new_cbsa TEXT, n_institutions INTEGER, "
        "PRIMARY KEY (old_cbsa, new_cbsa))"
    )
    conn.executemany(
        "INSERT INTO cbsa_crosswalk VALUES (?, ?, ?)",
        [(d, t, n) for (d, t), n in crosswalk.items()],
    )
    conn.commit()
    if verbose:
        print(f"[institutions] canonicalized {len(updates)} institutions "
              f"touching deprecated CBSA codes; {len(crosswalk)} code pairs recorded.")
    return len(updates)


def normalize_oes(conn, verbose=True):
    """Fold deprecated OES metro area codes into their successor, but only when
    the OES data shows a clean year handoff (no overlap). Candidates come from
    `cbsa_crosswalk`.
    """
    try:
        pairs = conn.execute(
            "SELECT old_cbsa, new_cbsa FROM cbsa_crosswalk"
        ).fetchall()
    except sqlite3.OperationalError:
        if verbose:
            print("[oes] no cbsa_crosswalk table — run canonicalize_institutions first.")
        return 0

    def years(code):
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT year FROM oes_employment "
            "WHERE area_type = 4 AND area_code = ? ORDER BY year", ("00" + code,)
        )]

    folded = []
    for old, new in pairs:
        oy, ny = years(old), years(new)
        if not oy or not ny:
            continue                       # one side absent in OES — nothing to fold
        if max(oy) >= min(ny):
            continue                       # overlap -> distinct metros, reject
        # Clean handoff: relabel old -> new, adopting the new code's OES title.
        title = conn.execute(
            "SELECT area_title FROM oes_employment WHERE area_code = ? "
            "AND area_title IS NOT NULL ORDER BY year DESC LIMIT 1", ("00" + new,)
        ).fetchone()
        new_title = title[0] if title else None
        conn.execute(
            "UPDATE oes_employment SET area_code = ?, area_title = COALESCE(?, area_title) "
            "WHERE area_code = ?", ("00" + new, new_title, "00" + old),
        )
        folded.append((old, new, new_title))
    conn.commit()
    if verbose:
        for old, new, t in folded:
            print(f"[oes] folded 00{old} -> 00{new} ({t})")
        print(f"[oes] {len(folded)} metro area code(s) folded.")
    return len(folded)


def backfill_cbsa_names(conn, verbose=True):
    """Ensure every current institution CBSA code has a row in cbsa_names so the
    metro dropdown can label it. Pulls names from institutions where missing.
    """
    missing = conn.execute(
        "SELECT i.cbsa, MAX(i.cbsanm) FROM institutions i "
        "WHERE i.cbsa IS NOT NULL AND CAST(i.cbsa AS INTEGER) > 0 "
        "AND i.cbsa NOT IN (SELECT cbsa FROM cbsa_names) "
        "AND i.cbsanm IS NOT NULL AND i.cbsanm != '' "
        "GROUP BY i.cbsa"
    ).fetchall()
    conn.executemany(
        "INSERT OR IGNORE INTO cbsa_names (cbsa, cbsanm, cbsatype) VALUES (?, ?, 'metro')",
        missing,
    )
    conn.commit()
    if verbose:
        print(f"[cbsa_names] backfilled {len(missing)} missing metro name(s).")
    return len(missing)


def main():
    conn = sqlite3.connect(DB_PATH)
    print(f"Normalizing CBSA codes in {DB_PATH} ...")
    canonicalize_institutions(conn)
    normalize_oes(conn)
    backfill_cbsa_names(conn)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
