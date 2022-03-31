"""Function to merge overall mortality metrics into a RegionAtlas"""

import logging
import warnings

import pycountry

import covid.fetch_economist_mortality
from covid.region_data import make_metric


def add_metrics(session, atlas):
    logging.info("Loading and merging The Economist's mortality model...")
    econ_credits = covid.fetch_economist_mortality.credits()
    econ_df = covid.fetch_economist_mortality.get_mortality(session)
    for iso3, v in econ_df.groupby(level="iso3c", as_index=False):
        v.reset_index("iso3c", drop=True, inplace=True)
        cc = pycountry.countries.get(alpha_3=iso3)
        if cc is None:
            warnings.warn(f"Unknown Economist mortality country code: {iso3}")
            continue

        region = atlas.by_iso2.get(cc.alpha_2)
        if region is None:
            warnings.warn(f"Missing Economist mortality country: {cc.alpha_2}")
            continue

        pop = region.totals.get("population", 0)
        if not (pop > 0):
            warnings.warn(f"No population: {region.path()} (pop={pop})")
            continue

        region.metrics["covid"]["est exc mort / day / 10Mp"] = make_metric(
            c="tab:orange",
            em=0,
            ord=1.4,
            cred=econ_credits,
            v=v.estimated_daily_excess_deaths * 1e7 / pop,
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
