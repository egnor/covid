"""Functions to merge wastewater sampling metrics into a RegionAtlas"""

import logging
from warnings import warn

import matplotlib.cm

import covid.fetch_scan_wastewater
from covid.region_data import make_metric


# Bad FIPS values for SCAN (and other?) sites
FIX_FIPS = {
  "City of San Leandro Water Pollution Control Plant": 6001,  # Alameda (CA)
  "Davis": 6113,  # Yolo (CA)
  "Fairfield-Suisun Sewer District": 6095,  # Solano (CA)
  "Southeast San Francisco": 6075,  # San Francisco (CA)
  "UC Davis": 6113,  # Yolo (CA)
}


def add_metrics(session, atlas):
    logging.info("Loading and merging SCAN wastewater data...")
    df = covid.fetch_scan_wastewater.get_wastewater(session)

    dups = df.index.duplicated(keep=False)
    for site, fips, timestamp in df.index[dups]:
        warn(
            "Duplicate SCAN wastewater data: "
            f"({site}) {timestamp.strftime('%Y-%m-%d')}"
        )

    df = df[~dups]
    for plant_i, ((fips, site), rows) in enumerate(
        df.groupby(
            level=["County_FIPS", "Site_Name"],
            sort=False,
            dropna=False,
            as_index=False
        )
    ):
        rows.reset_index(["County_FIPS", "Site_Name"], drop=True, inplace=True)
        try:
            fips = int(FIX_FIPS.get(site, fips))
        except ValueError:
            warn(f"Bad FIPS ({fips}) for SCAN wastewater plant: {site}")
            continue

        if not fips:
            warn(f"No FIPS for SCAN wastewater plant: {site}")
            continue

        region = atlas.by_fips.get(fips)
        if not region:
            warn(f"Unknown SCAN wastewater FIPS: {repr(fips)} ({site})")
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
