"""Functions to merge wastewater sampling metrics into a RegionAtlas"""

import logging
from warnings import warn

import matplotlib.cm

import covid.fetch_scan_wastewater
from covid.region_data import make_metric

PLANT_FIPS = {
    "Ann Arbor, MI": 26161,
    "Coeur d'Alene, ID": 16055,
    "College Park, GA": 13121,
    "Contra Costa County, CA": 6013,
    "Davis, CA": 6113,
    "Garland, TX": 48113,
    "Half Moon Bay, CA": 6081,
    "Jackson, MI": 26075,
    "Los Angeles County, CA": 6037,
    "Louisville, KY": 21111,
    "Novato, CA": 6041,
    "Oakland, CA": 6001,
    "Ontario, CA": 6071,
    "Orange County, FL": 12095,
    "Parker, CO": 8035,
    "Paso Robles, CA": 6079,
    "Petaluma, CA": 6097,
    "Roswell, GA": 5049,
    "San Mateo, CA": 6081,
    "Santa Cruz County, CA": 6087,
    "Santa Cruz, CA": 6087,
    "Southeast San Francisco, CA": 6075,
    "Sunnyvale, TX": 48113,
    "University of California, Davis, CA": 6113,
    "West Contra Costa County, CA": 6013,
}


def add_metrics(session, atlas):
    logging.info("Loading and merging SCAN wastewater data...")
    df = covid.fetch_scan_wastewater.get_wastewater(session)

    dups = df.index.duplicated(keep=False)
    for plant, site, timestamp in df.index[dups]:
        warn(
            "Duplicate SCAN wastewater data: "
            f"{plant} ({site}) {timestamp.strftime('%Y-%m-%d')}"
        )

    df = df[~dups]
    for (plant, site), rows in df.groupby(
        level=["Plant", "Site_Name"], sort=False, as_index=False
    ):
        rows.reset_index(["Plant", "Site_Name"], drop=True, inplace=True)
        fips = PLANT_FIPS.get(plant.split("-")[0].strip())
        if not fips:
            warn(f"Unknown SCAN wastewater plant: {plant}")
            continue

        region = atlas.by_fips.get(fips)
        if not region:
            warn(f"Missing SCAN wastewater FIPS: {fips} ({plant})")
            continue

        region.credits.update(covid.fetch_scan_wastewater.credits())

        ww_metrics = region.metrics.wastewater
        ww_metrics[site + " (COVID Kcopies)"] = make_metric(
            c=matplotlib.cm.tab20b.colors[(12 + len(ww_metrics)) % 20],
            em=1,
            ord=1.0,
            raw=rows.SC2_S_gc_g_dry_weight * 1e-3,
            rollup=False,
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
