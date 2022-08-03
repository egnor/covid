"""Functions to merge wastewater sampling metrics into a RegionAtlas"""

import logging
from warnings import warn

import matplotlib.cm

import covid.fetch_scan_wastewater
from covid.region_data import make_metric

STATECITY_FIPS = {
    ("CA", "Carson"): 6037,
    ("CA", "Richmond"): 6013,
    ("CA", "Davis"): 6113,
    ("CA", "Half Moon Bay"): 6081,
    ("CA", "Martinez"): 6013,
    ("CA", "Novato"): 6041,
    ("CA", "Oakland"): 6001,
    ("CA", "Ontario"): 6071,
    ("CA", "Paso Robles"): 6079,
    ("CA", "Petaluma"): 6097,
    ("CA", "San Francisco"): 6075,
    ("CA", "San Mateo"): 6081,
    ("CA", "Santa Cruz"): 6087,
    ("CO", "Parker"): 8035,
    ("FL", "Orlando"): 12095,
    ("GA", "College Park"): 13121,
    ("GA", "Roswell"): 5049,
    ("ID", "Coeur D Alene"): 16055,
    ("KY", "Louisville"): 21111,
    ("MI", "Ann Arbor"): 26161,
    ("MI", "Jackson"): 26075,
    ("TX", "Garland"): 48113,
    ("TX", "Sunnyvale"): 48113,
}


def add_metrics(session, atlas):
    logging.info("Loading and merging SCAN wastewater data...")
    df = covid.fetch_scan_wastewater.get_wastewater(session)

    dups = df.index.duplicated(keep=False)
    for city, state, site, timestamp in df.index[dups]:
        warn(
            "Duplicate SCAN wastewater data: "
            f"{city}, {state} ({site}) {timestamp.strftime('%Y-%m-%d')}"
        )

    df = df[~dups]
    index_cols = ["City", "State_Abbr", "Site_Name"]
    for plant_i, ((city, state, site), rows) in enumerate(
        df.groupby(level=index_cols, sort=False, as_index=False)
    ):
        rows.reset_index(index_cols, drop=True, inplace=True)
        fips = STATECITY_FIPS.get((state, city))
        if not fips:
            warn(f"Unknown SCAN wastewater city: {city}, {state}")
            continue

        region = atlas.by_fips.get(fips)
        if not region:
            warn(f"Missing SCAN wastewater FIPS: {fips} ({plant})")
            continue

        region.credits.update(covid.fetch_scan_wastewater.credits())
        ww_metrics = region.metrics.wastewater

        ww_metrics[f"COVID (all) Kcopies ({site})"] = make_metric(
            c=matplotlib.cm.tab20b.colors[(4 + plant_i * 4) % 20],
            em=1,
            ord=1.0,
            raw=rows.SC2_S_gc_g_dry_weight * 1e-3,
        )

        ww_metrics[f"COVID BA.4/5 Kcopies ({site})"] = make_metric(
            c=matplotlib.cm.tab20b.colors[(6 + plant_i * 4) % 20],
            em=0,
            ord=1.0,
            raw=rows.HV_69_70_Del_gc_g_dry_weight * 1e-3,
        )


if __name__ == "__main__":
    import argparse

    from covid import build_atlas
    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument("--print_data", action="store_true")

    args = parser.parse_args()
    session = cache_policy.new_session(args)
    atlas = build_atlas.get_atlas(session)
    add_metrics(session=session, atlas=atlas)
    print(atlas.world.debug_tree(with_data=args.print_data))
