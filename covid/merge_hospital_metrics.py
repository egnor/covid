"""Functions to merge hospital utilization metrics into a RegionAtlas"""

import logging
import warnings

import pycountry

import covid.fetch_ourworld_hospitalizations
from covid.region_data import make_metric


def add_metrics(session, atlas):
    logging.info("Loading and merging ourworldindata hospitalization data...")
    hosp_credits = covid.fetch_ourworld_hospitalizations.credits()
    covid.fetch_ourworld_hospitalizations.get_occupancy(session)
    adm_df = covid.fetch_ourworld_hospitalizations.get_admissions(session)
    for iso3, v in adm_df.groupby("iso_code", as_index=False):
        cc = pycountry.countries.get(alpha_3=iso3)
        if cc is None:
            warnings.warn(f"Unknown OWID hosp country code: {iso3}")
            continue

        region = atlas.by_iso2.get(cc.alpha_2)
        if region is None:
            warnings.warn(f"Missing OWID hosp country: {cc.alpha_2}")
            continue

        pop = region.totals.get("population", 0)
        if not (pop > 0):
            warnings.warn(f"No population: {region.path()} (pop={pop})")
            continue

        v.reset_index("iso_code", drop=True, inplace=True)

        region.covid_metrics["hospital admissions / 1Mp"] = make_metric(
            c="tab:orange",
            em=0,
            ord=1.1,
            cred=hosp_credits,
            v=v["new hospital admissions"] * (1e6 / pop),
        )

        region.covid_metrics["ICU admissions / 1Mp"] = make_metric(
            c="tab:pink",
            em=0,
            ord=1.2,
            cred=hosp_credits,
            v=v["new ICU admissions"] * (1e6 / pop),
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
