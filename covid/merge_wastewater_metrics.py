"""Functions to merge wastewater sampling metrics into a RegionAtlas"""

import logging
import warnings

import matplotlib.cm
import pycountry

import covid.fetch_cdc_wastewater
from covid.region_data import make_metric


def add_metrics(session, atlas):
    logging.info("Loading US CDC wastewater data...")
    cdc_credits = covid.fetch_cdc_wastewater.credits()
    df = covid.fetch_cdc_wastewater.get_wastewater(session)

    logging.info("Merging US CDC wastewater data...")
    for fipses, v in df.groupby(level=["county_fips"]):
        for fips in fipses.split(","):
            region = atlas.by_fips.get(int(fips))
            if not region:
                row = v.iloc[0]
                warnings.warn(
                    f"Missing CDC wastewater FIPS: {fips} ({row.county_names})"
                )
                continue

            pop = region.totals.get("population", 0)
            if not (pop > 0):
                warnings.warn("No population: {region.path()} (pop={pop})")
                continue

            ww_metrics = region.metrics["wastewater"]
            for (tp_id, key), w in v.groupby(level=["wwtp_id", "key_plot_id"]):
                if w.ptc_15d.count() < 2:
                    continue

                w.reset_index(
                    ["county_fips", "wwtp_id", "key_plot_id"],
                    drop=True,
                    inplace=True
                )

                row = w.iloc[0]
                details = []
                site = row.sample_location.lower()
                if site == "before treatment plant":
                    details.append("upstream")
                elif site != "treatment plant":
                    details.append(site)
                if row.sample_location_specify >= 0:
                    details.append(f"site {row.sample_location_specify:.0f}")

                matrix = key.split("_")[-1].lower()
                if matrix == "primary sludge":
                    details.append("sludge")
                elif matrix != "raw wastewater":
                    details.append(key.split("_")[-1].lower())

                name = f"Plant {tp_id}"
                name += f" ({' '.join(details)})" if details else ""
                name += f": {row.population_served:,.0f}p"

                w.index = w.index + 0.5 * (w.date_end - w.index)
                ww_metrics[name] = make_metric(
                    c=matplotlib.cm.tab20b.colors[(12 + len(ww_metrics)) % 20],
                    em=1 if row.population_served > 0.25 * pop else 0,
                    ord=1.0,
                    cred=cdc_credits,
                    v=((1 + (w.ptc_15d / 100)) ** (1 / 15) - 1) * 100,
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
