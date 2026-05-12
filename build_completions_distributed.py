"""Redistribute IPEDS completions geographically using NC-SARA + population.

Builds two aggregate tables that replace the current "100% to institution HQ"
attribution used in rankings.py:

    completions_by_state(year, cipcode, awlevel, dest_state, completions)
    completions_by_metro(year, cipcode, awlevel, dest_cbsa, completions)

Distribution rules (per user spec)
----------------------------------
Each institution is classified from IPEDS distance-ed flags:

    distnced = 1                       → "fully_online"
    distpgs  = 1 AND distnced != 1     → "online_program"
    otherwise                          → "brick"

Brick-and-mortar:
    100% of completions stay at the institution's home (state, CBSA).

Online program (distpgs=1, partial distance):
    80% in-state — 50% to home metro, 30% across other home-state CBSAs
                   weighted by population.
    20% out-of-state — distributed across receiving states per the
                       institution's 3-year NC-SARA window, then within each
                       state across CBSAs weighted by population.

Fully online (distnced=1):
    20% in-state — 10% home metro, 10% across other home-state CBSAs by pop.
    80% out-of-state — same NC-SARA × CBSA-population mechanic.

Year window
-----------
For completions year Y, the NC-SARA window is Fall (Y-2, Y-1, Y), clamped to
the [2019, 2024] data we have on hand. Institutions not in NC-SARA fall back
to a pooled distribution by (member_type, ipeds_level).

Non-SARA / PR / VI buckets in NC-SARA are dropped before normalization, which
implicitly redistributes them proportionally across known receiving states.

Edge cases
----------
- Institution with home_cbsa = '-2' (rural / no metro): the "home metro"
  share is rerouted to the rest-of-state distribution, so 100% of in-state
  goes across the state's CBSAs by population.
- Institution in a state with no other CBSAs: rest-of-state share goes to
  the home metro instead. (Only matters for a couple of single-CBSA states.)
"""
from __future__ import annotations

import sqlite3
import numpy as np
import pandas as pd
from scipy.sparse import coo_matrix, csr_matrix

DB_PATH = "ipeds.db"

# For institutions with a valid home CBSA ("urban"). Shares are
# (home_metro, rest_of_state, out_of_state, rural_bucket).
URBAN_SHARES = {
    "brick":          (1.0, 0.0,  0.0, 0.0),
    "online_program": (0.5, 0.3,  0.2, 0.0),
    "fully_online":   (0.1, 0.1,  0.8, 0.0),
}

# For institutions without a usable CBSA ("rural" — home_cbsa missing or '-2').
# Brick-and-mortar rurals stay 100% in their state with no CBSA assignment;
# distance-ed rurals follow the spec: out-of-state count = 1/3 of in-state,
# of the in-state share 75% sits in the institution's rural area and 25%
# spreads across the state's metros by population. Net shares:
#   25% out-of-state, 18.75% in-state metros, 56.25% in rural bucket.
RURAL_SHARES = {
    "brick":          (0.0, 0.0,    0.0,  1.0),
    "online_program": (0.0, 0.1875, 0.25, 0.5625),
    "fully_online":   (0.0, 0.1875, 0.25, 0.5625),
}

CONTROL_TO_TYPE = {
    1: "Public", 2: "Private Non-Profit", 3: "Private For-Profit",
}
ICLEVEL_TO_LEVEL = {1: "4 Year", 2: "2 Year", 3: "2 Year"}

NCSARA_FIRST_YEAR = 2019
NCSARA_LAST_YEAR = 2024

EXCLUDE_DEST = {"NON_SARA", "PR", "VI"}


def pick_nc_sara_window(comp_year: int) -> list[int]:
    yrs = [comp_year - 2, comp_year - 1, comp_year]
    yrs = [max(NCSARA_FIRST_YEAR, min(NCSARA_LAST_YEAR, y)) for y in yrs]
    return sorted(set(yrs))


def _primary_state(cbsanm) -> str | None:
    if cbsanm is None or not isinstance(cbsanm, str) or "," not in cbsanm:
        return None
    return cbsanm.split(",")[-1].strip().split("-")[0].strip()[:2]


def load_cbsa_geo(conn) -> pd.DataFrame:
    """Returns DataFrame[cbsa_code, state, population] covering all CBSAs we
    can map to a US state with a known population."""
    df = pd.read_sql(
        "SELECT cn.cbsa AS cbsa_code, cn.cbsanm, cp.population "
        "FROM cbsa_names cn JOIN cbsa_populations cp ON cp.cbsa_code = cn.cbsa",
        conn,
    )
    df["state"] = df["cbsanm"].map(_primary_state)
    df = df[df["state"].notna()
            & df["state"].str.match(r"^[A-Z]{2}$", na=False)
            & (df["population"] > 0)]
    return df[["cbsa_code", "state", "population"]].reset_index(drop=True)


def load_institutions(year: int, conn) -> pd.DataFrame:
    df = pd.read_sql(
        """
        SELECT i.unitid, i.stabbr AS home_state, i.cbsa AS home_cbsa,
               i.control, i.iclevel,
               COALESCE(d.distnced, 0) AS distnced,
               COALESCE(d.distpgs, 0) AS distpgs
        FROM institutions i
        LEFT JOIN distance_ed_status d
               ON d.unitid = i.unitid AND d.year = i.year
        WHERE i.year = ?
        """,
        conn, params=[year],
    )
    df["category"] = np.where(
        df["distnced"] == 1, "fully_online",
        np.where(df["distpgs"] == 1, "online_program", "brick"),
    )
    df["member_type"] = df["control"].map(CONTROL_TO_TYPE)
    df["ipeds_level"] = df["iclevel"].map(ICLEVEL_TO_LEVEL)
    valid_cbsa = df["home_cbsa"].astype(str).str.fullmatch(r"\d{5}")
    df.loc[~valid_cbsa, "home_cbsa"] = None
    return df


def load_xwalk(conn) -> pd.DataFrame:
    """First-known unitid for each OPEID (single canonical mapping)."""
    df = pd.read_sql(
        "SELECT opeid, unitid FROM unitid_opeid_crosswalk", conn
    )
    return df.drop_duplicates("opeid", keep="first")


def build_out_state_shares(comp_year: int, inst: pd.DataFrame,
                           xwalk: pd.DataFrame, conn) -> pd.DataFrame:
    """Returns DataFrame[unitid, dest_state, share_out] — out-of-state
    distribution shares (excluding home state, summing to 1.0 per unitid).
    Institutions in NC-SARA use their own data; others fall back to a pool
    by (member_type, ipeds_level)."""
    window = pick_nc_sara_window(comp_year)
    ph = ",".join("?" * len(window))
    sara = pd.read_sql(
        f"""
        SELECT n.opeid, n.member_type, n.ipeds_level,
               n.dest_state, SUM(n.enrollments) AS enr
        FROM nc_sara_enrollment n
        WHERE n.year IN ({ph})
          AND n.dest_state NOT IN ('NON_SARA','PR','VI')
        GROUP BY n.opeid, n.member_type, n.ipeds_level, n.dest_state
        """,
        conn, params=window,
    )

    # Direct NC-SARA → unitid via OPEID
    direct = sara.merge(xwalk, on="opeid", how="inner")
    direct = direct.merge(
        inst[["unitid", "home_state", "category"]], on="unitid", how="inner"
    )
    direct = direct[direct["category"].isin({"online_program", "fully_online"})]

    # Pooled fallback by (member_type, ipeds_level)
    pool = (
        sara.groupby(["member_type", "ipeds_level", "dest_state"],
                     as_index=False)["enr"].sum()
    )
    in_sara_units = set(direct["unitid"].unique())
    needs_fallback = inst[
        inst["category"].isin({"online_program", "fully_online"})
        & ~inst["unitid"].isin(in_sara_units)
    ]
    fallback = needs_fallback[
        ["unitid", "home_state", "member_type", "ipeds_level"]
    ].merge(pool, on=["member_type", "ipeds_level"], how="inner")

    combined = pd.concat(
        [direct[["unitid", "home_state", "dest_state", "enr"]],
         fallback[["unitid", "home_state", "dest_state", "enr"]]],
        ignore_index=True,
    )
    combined = combined[combined["dest_state"] != combined["home_state"]]
    tot = combined.groupby("unitid")["enr"].transform("sum")
    combined["share_out"] = combined["enr"] / tot.replace(0, np.nan)
    combined = combined[combined["share_out"].notna()]
    return combined[["unitid", "dest_state", "share_out"]]


def build_distribution(year: int, conn,
                       cbsa_geo: pd.DataFrame,
                       xwalk: pd.DataFrame
                       ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-institution weights for one IPEDS year. Returns two DataFrames:

      metro_dist:  (unitid, dest_state, dest_cbsa, weight)
      rural_dist:  (unitid, dest_state, weight)

    Weights across both frames sum to ~1.0 per unitid (we renormalize at the
    end to absorb edge cases like institutions whose home state has no
    CBSAs we recognize)."""
    inst = load_institutions(year, conn)
    inst["is_rural"] = inst["home_cbsa"].isna()

    # Materialize the four shares (home_metro, rest_of_state, out_of_state,
    # rural_bucket) as columns on `inst` via a vectorized lookup. The maps
    # are tiny (3 categories × 4 shares) so we build the share columns by
    # mapping (rural, category) to the right tuple in O(n_inst).
    share_keys = list(inst["category"])
    rural_flags = inst["is_rural"].values
    shares = np.empty((len(inst), 4), dtype=float)
    for i, (r, c) in enumerate(zip(rural_flags, share_keys)):
        shares[i] = (RURAL_SHARES if r else URBAN_SHARES)[c]
    inst[["s_metro", "s_rest", "s_out", "s_rural"]] = shares

    state_cbsa_pop = cbsa_geo[["cbsa_code", "state", "population"]]
    metro_rows: list[pd.DataFrame] = []

    # ── 1. Home metro (urban only — rural has s_metro = 0).
    home = inst[(inst["s_metro"] > 0) & ~inst["is_rural"]]
    if not home.empty:
        metro_rows.append(pd.DataFrame({
            "unitid": home["unitid"].values,
            "dest_state": home["home_state"].values,
            "dest_cbsa": home["home_cbsa"].values,
            "weight": home["s_metro"].values,
        }))

    # ── 2. Rest of home state — spread across in-state CBSAs by population.
    # Urban excludes the institution's own home CBSA; rural has no CBSA to
    # exclude so all in-state CBSAs are eligible.
    rest_units = inst[inst["s_rest"] > 0]
    if not rest_units.empty:
        rest = rest_units.merge(
            state_cbsa_pop, left_on="home_state",
            right_on="state", how="inner",
        )
        rest = rest[rest["cbsa_code"] != rest["home_cbsa"].fillna("")]
        denom = rest.groupby("unitid")["population"].transform("sum")
        rest["weight"] = (
            rest["s_rest"] * rest["population"] / denom.replace(0, np.nan)
        )
        rest = rest[rest["weight"].notna() & (rest["weight"] > 0)]
        if not rest.empty:
            metro_rows.append(
                rest.rename(columns={"state": "dest_state",
                                     "cbsa_code": "dest_cbsa"})
                [["unitid", "dest_state", "dest_cbsa", "weight"]]
            )
        # Orphans: rest_of_state share with nowhere to land (state has no
        # eligible CBSAs after excluding home). Urban orphans → home_metro;
        # rural orphans → rural_bucket. Tracked by adjusting shares for the
        # renormalization step below.
        landed = set(rest["unitid"].unique())
        orphans = rest_units[~rest_units["unitid"].isin(landed)]
        urban_orphans = orphans[~orphans["is_rural"]]
        rural_orphans = orphans[orphans["is_rural"]]
        if not urban_orphans.empty:
            metro_rows.append(pd.DataFrame({
                "unitid": urban_orphans["unitid"].values,
                "dest_state": urban_orphans["home_state"].values,
                "dest_cbsa": urban_orphans["home_cbsa"].values,
                "weight": urban_orphans["s_rest"].values,
            }))
        if not rural_orphans.empty:
            mask = inst["unitid"].isin(rural_orphans["unitid"])
            inst.loc[mask, "s_rural"] = (
                inst.loc[mask, "s_rural"] + inst.loc[mask, "s_rest"]
            )

    # ── 3. Out of state — NC-SARA dest-state shares × within-state CBSA pop.
    out_units = inst[inst["s_out"] > 0]
    if not out_units.empty:
        out_shares = build_out_state_shares(year, inst, xwalk, conn)
        out = out_units[["unitid", "s_out"]].merge(
            out_shares, on="unitid", how="inner"
        )
        out = out.merge(
            state_cbsa_pop, left_on="dest_state",
            right_on="state", how="inner",
        )
        denom = out.groupby(["unitid", "dest_state"])["population"].transform("sum")
        out["weight"] = (
            out["s_out"] * out["share_out"]
            * out["population"] / denom.replace(0, np.nan)
        )
        out = out[out["weight"].notna() & (out["weight"] > 0)]
        if not out.empty:
            metro_rows.append(
                out.rename(columns={"cbsa_code": "dest_cbsa"})
                [["unitid", "dest_state", "dest_cbsa", "weight"]]
            )
        # Institutions whose s_out > 0 but no NC-SARA distribution materialized
        # (e.g. unknown control or no member-type/level fallback match) keep
        # their lost share absorbed by the renormalize step below.

    metro_dist = (
        pd.concat(metro_rows, ignore_index=True)
        if metro_rows else
        pd.DataFrame(columns=["unitid", "dest_state", "dest_cbsa", "weight"])
    )

    # ── 4. Rural bucket — state-only, no CBSA assignment.
    rural_inst = inst[inst["s_rural"] > 0]
    rural_dist = pd.DataFrame({
        "unitid": rural_inst["unitid"].values,
        "dest_state": rural_inst["home_state"].values,
        "weight": rural_inst["s_rural"].values,
    }) if not rural_inst.empty else pd.DataFrame(
        columns=["unitid", "dest_state", "weight"]
    )

    # ── 5. Renormalize so weights sum to 1.0 per unitid. Compensates for
    # institutions whose home state has no recognized CBSAs or whose
    # out-of-state distribution failed to land.
    metro_tot = metro_dist.groupby("unitid")["weight"].sum()
    rural_tot = rural_dist.groupby("unitid")["weight"].sum() \
        if not rural_dist.empty else pd.Series(dtype=float)
    totals = (
        pd.concat([metro_tot.rename("m"), rural_tot.rename("r")], axis=1)
        .fillna(0).sum(axis=1)
    )
    scale = (1.0 / totals).replace([np.inf, -np.inf], np.nan)

    if not metro_dist.empty:
        metro_dist["weight"] = (
            metro_dist["weight"] * scale.reindex(metro_dist["unitid"]).values
        )
        metro_dist = metro_dist[
            metro_dist["weight"].notna() & (metro_dist["weight"] > 0)
        ]
    if not rural_dist.empty:
        rural_dist["weight"] = (
            rural_dist["weight"] * scale.reindex(rural_dist["unitid"]).values
        )
        rural_dist = rural_dist[
            rural_dist["weight"].notna() & (rural_dist["weight"] > 0)
        ]

    return (metro_dist[["unitid", "dest_state", "dest_cbsa", "weight"]],
            rural_dist[["unitid", "dest_state", "weight"]])


def _sparse_redistribute(comp: pd.DataFrame, dist: pd.DataFrame,
                         dest_col: str) -> pd.DataFrame:
    """Fan completions[unitid, cipcode, awlevel, ctotalt] through a
    distribution matrix dist[unitid, dest_col, weight] via sparse matrix
    multiplication. Returns DataFrame[cipcode, awlevel, dest_col, completions].
    """
    if comp.empty or dist.empty:
        return pd.DataFrame(columns=["cipcode", "awlevel", dest_col, "completions"])

    units = pd.Index(
        sorted(set(comp["unitid"]) | set(dist["unitid"])), name="unitid"
    )
    uidx = pd.Series(np.arange(len(units)), index=units)

    cipaw = comp[["cipcode", "awlevel"]].drop_duplicates().reset_index(drop=True)
    cipaw_key = list(zip(cipaw["cipcode"], cipaw["awlevel"]))
    cipaw_idx = {k: i for i, k in enumerate(cipaw_key)}

    dests = pd.Index(sorted(dist[dest_col].unique()), name=dest_col)
    didx = pd.Series(np.arange(len(dests)), index=dests)

    comp = comp.copy()
    comp["u"] = uidx.loc[comp["unitid"]].values
    comp["k"] = comp.set_index(["cipcode", "awlevel"]).index.map(cipaw_idx)
    C = coo_matrix(
        (comp["ctotalt"].astype(float).values,
         (comp["u"].values, comp["k"].values)),
        shape=(len(units), len(cipaw)),
    ).tocsr()

    dist = dist[dist["unitid"].isin(units)].copy()
    dist["u"] = uidx.loc[dist["unitid"]].values
    dist["d"] = didx.loc[dist[dest_col]].values
    W = coo_matrix(
        (dist["weight"].astype(float).values,
         (dist["u"].values, dist["d"].values)),
        shape=(len(units), len(dests)),
    ).tocsr()

    R = (C.T @ W).tocoo()
    # Vectorized index → (cipcode, awlevel). Iterating cipaw_key in Python
    # for 5M+ rows takes minutes; numpy fancy-indexing does it in ms.
    cip_arr = cipaw["cipcode"].to_numpy()
    aw_arr = cipaw["awlevel"].to_numpy()
    out = pd.DataFrame({
        "cipcode": cip_arr[R.row],
        "awlevel": aw_arr[R.row],
        dest_col: dests[R.col].to_numpy(),
        "completions": R.data,
    })
    out = out[out["completions"] > 0.0]
    return out[["cipcode", "awlevel", dest_col, "completions"]]


def distribute_year(year: int, conn, cbsa_geo, xwalk):
    """Returns (by_state_df, by_metro_df) for one IPEDS completions year."""
    metro_dist, rural_dist = build_distribution(year, conn, cbsa_geo, xwalk)

    comp = pd.read_sql(
        "SELECT unitid, cipcode, awlevel, ctotalt FROM completions "
        "WHERE year = ? AND ctotalt > 0",
        conn, params=[year],
    )

    by_metro = _sparse_redistribute(comp, metro_dist, "dest_cbsa")
    by_metro["year"] = year
    by_metro = by_metro[["year", "cipcode", "awlevel", "dest_cbsa", "completions"]]

    # State table: sum metro contribution by dest_state, then add the
    # rural-bucket contribution which is already keyed by state.
    cbsa_to_state = cbsa_geo.set_index("cbsa_code")["state"]
    metro_by_state = by_metro.copy()
    metro_by_state["dest_state"] = metro_by_state["dest_cbsa"].map(cbsa_to_state)
    metro_by_state = metro_by_state[metro_by_state["dest_state"].notna()]
    metro_by_state = metro_by_state.groupby(
        ["year", "cipcode", "awlevel", "dest_state"], as_index=False
    )["completions"].sum()

    rural_by_state = _sparse_redistribute(comp, rural_dist, "dest_state")
    if not rural_by_state.empty:
        rural_by_state["year"] = year
        by_state = pd.concat(
            [metro_by_state,
             rural_by_state[["year", "cipcode", "awlevel",
                             "dest_state", "completions"]]],
            ignore_index=True,
        )
        by_state = by_state.groupby(
            ["year", "cipcode", "awlevel", "dest_state"], as_index=False
        )["completions"].sum()
    else:
        by_state = metro_by_state

    return by_state, by_metro


def main(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    cbsa_geo = load_cbsa_geo(conn)
    xwalk = load_xwalk(conn)

    years = pd.read_sql(
        "SELECT DISTINCT year FROM completions ORDER BY year", conn
    )["year"].tolist()

    # Create tables WITHOUT a primary key — we add the (year, cipcode,
    # awlevel, dest_X) index after all rows are inserted, since maintaining
    # a PK index during a 30M+ row bulk load is the dominant runtime cost.
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS completions_by_state")
    cur.execute("DROP TABLE IF EXISTS completions_by_metro")
    cur.execute(
        """
        CREATE TABLE completions_by_state (
            year INTEGER NOT NULL,
            cipcode TEXT NOT NULL,
            awlevel INTEGER NOT NULL,
            dest_state TEXT NOT NULL,
            completions REAL NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE completions_by_metro (
            year INTEGER NOT NULL,
            cipcode TEXT NOT NULL,
            awlevel INTEGER NOT NULL,
            dest_cbsa TEXT NOT NULL,
            completions REAL NOT NULL
        )
        """
    )
    # Bulk-load pragmas for fast inserts.
    cur.execute("PRAGMA journal_mode = MEMORY")
    cur.execute("PRAGMA synchronous = OFF")
    cur.execute("PRAGMA temp_store = MEMORY")
    conn.commit()

    state_insert = (
        "INSERT INTO completions_by_state "
        "(year, cipcode, awlevel, dest_state, completions) VALUES (?,?,?,?,?)"
    )
    metro_insert = (
        "INSERT INTO completions_by_metro "
        "(year, cipcode, awlevel, dest_cbsa, completions) VALUES (?,?,?,?,?)"
    )

    for year in years:
        print(f"Distributing completions for {year}...", flush=True)
        by_state, by_metro = distribute_year(year, conn, cbsa_geo, xwalk)
        state_rows = list(
            by_state[["year", "cipcode", "awlevel",
                      "dest_state", "completions"]]
            .itertuples(index=False, name=None)
        )
        metro_rows = list(
            by_metro[["year", "cipcode", "awlevel",
                      "dest_cbsa", "completions"]]
            .itertuples(index=False, name=None)
        )
        cur.executemany(state_insert, state_rows)
        cur.executemany(metro_insert, metro_rows)
        conn.commit()
        print(f"  -> {len(by_state):,} state rows, "
              f"{len(by_metro):,} metro rows", flush=True)

    print("Building indexes...", flush=True)
    cur.execute(
        "CREATE INDEX idx_cbs_year_cip_aw "
        "ON completions_by_state(year, cipcode, awlevel)"
    )
    cur.execute("CREATE INDEX idx_cbs_state ON completions_by_state(dest_state)")
    cur.execute(
        "CREATE INDEX idx_cbm_year_cip_aw "
        "ON completions_by_metro(year, cipcode, awlevel)"
    )
    cur.execute("CREATE INDEX idx_cbm_cbsa ON completions_by_metro(dest_cbsa)")
    conn.commit()

    # Reconciliation totals: redistributed completions should match raw
    # totals year-by-year (sum across all destinations == sum of ctotalt).
    raw = pd.read_sql(
        "SELECT year, SUM(ctotalt) AS raw FROM completions GROUP BY year", conn
    )
    redis = pd.read_sql(
        "SELECT year, SUM(completions) AS redistributed "
        "FROM completions_by_state GROUP BY year", conn
    )
    rec = raw.merge(redis, on="year").assign(
        diff_pct=lambda d: (d["redistributed"] - d["raw"]) / d["raw"] * 100
    )
    print("\nReconciliation (raw vs redistributed totals):")
    print(rec.to_string(index=False))
    conn.close()


if __name__ == "__main__":
    main()
