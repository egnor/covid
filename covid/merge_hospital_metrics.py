"""Functions to merge hospital utilization metrics into a RegionAtlas"""

import logging
import warnings

import pycountry

import covid.fetch_hhs_hospitalizations
import covid.fetch_ourworld_hospitalizations
from covid.region_data import make_metric


def add_metrics(session, atlas):
    logging.info("Loading and merging ourworldindata hospitalization data...")
    owid_credits = covid.fetch_ourworld_hospitalizations.credits()
    covid.fetch_ourworld_hospitalizations.get_occupancy(session)
    adm_df = covid.fetch_ourworld_hospitalizations.get_admissions(session)
    for iso3, v in adm_df.groupby(level="iso_code", as_index=False):
        v.reset_index("iso_code", drop=True, inplace=True)

        cc = pycountry.countries.get(alpha_3=iso3)
        if cc is None:
            warnings.warn(f"Unknown OWID admissions country code: {iso3}")
            continue

        region = atlas.by_iso2.get(cc.alpha_2)
        if region is None:
            warnings.warn(f"Missing OWID admissions country: {cc.alpha_2}")
            continue

        pop = region.totals.get("population", 0)
        if not (pop > 0):
            warnings.warn(f"No population: {region.path()} (pop={pop})")
            continue

        metrics = region.metrics["hospital"]
        metrics["COVID admits / day / 1Mp"] = make_metric(
            c="black",
            em=0,
            ord=1.3,
            cred=owid_credits,
            v=v["new hospital admissions"] * (1e6 / pop),
        )

        metrics["ICU COVID admits / day / 10Mp"] = make_metric(
            c="tab:purple",
            em=0,
            ord=1.7,
            cred=owid_credits,
            v=v["new ICU admissions"] * (1e7 / pop),
        )

    occ_df = covid.fetch_ourworld_hospitalizations.get_occupancy(session)
    for iso3, v in occ_df.groupby(level="iso_code", as_index=False):
        v.reset_index("iso_code", drop=True, inplace=True)

        cc = pycountry.countries.get(alpha_3=iso3)
        if cc is None:
            warnings.warn(f"Unknown OWID occupancy country code: {iso3}")
            continue

        region = atlas.by_iso2.get(cc.alpha_2)
        if region is None:
            warnings.warn(f"Missing OWID admissions country: {cc.alpha_2}")
            continue

        pop = region.totals.get("population", 0)
        if not (pop > 0):
            warnings.warn(f"No population: {region.path()} (pop={pop})")
            continue

        metrics = region.metrics["hospital"]
        metrics["COVID use / 100Kp"] = make_metric(
            c="tab:gray",
            em=1,
            ord=1.2,
            cred=owid_credits,
            v=v["hospital occupancy"] * (1e5 / pop),
        )

        metrics["ICU COVID use / 1Mp"] = make_metric(
            c="tab:pink",
            em=1,
            ord=1.6,
            cred=owid_credits,
            v=v["ICU occupancy"] * (1e6 / pop),
        )

    logging.info("Loading US HHS hospitalization data...")
    hhs_credits = covid.fetch_hhs_hospitalizations.credits()
    hhs_df = covid.fetch_hhs_hospitalizations.get_hospitalizations(session)

    logging.info("Merging US HHS hospitalization data...")
    for fips, per_fips in hhs_df.groupby(level="fips_code", as_index=False):
        region = atlas.by_fips.get(fips)
        if region is None:
            row = per_fips.iloc[0]
            warnings.warn(
                f"Missing HHS hospital FIPS: {fips}"
                f" ({row.city} {row.state} {row.zip:.0f})"
            )
            continue

        pop = region.totals.get("population", 0)
        if not (pop > 0):
            warnings.warn(f"No population: {region.path()} (pop={pop})")
            continue

        per_fips = per_fips.select_dtypes(float)
        per_fips.clip(lower=0, inplace=True)
        v = per_fips.groupby(level="collection_week").sum()

        metrics = region.metrics["hospital"]
        metrics["capacity / 100Kp"] = make_metric(
            c="black",
            em=-1,
            ord=1.0,
            cred=hhs_credits,
            v=v.inpatient_beds_7_day_avg * (1e5 / pop),
        )

        metrics["total use / 100Kp"] = make_metric(
            c="tab:gray",
            em=0,
            ord=1.1,
            cred=hhs_credits,
            v=v.inpatient_beds_used_7_day_avg * (1e5 / pop),
        )

        metrics["COVID use / 100Kp"] = make_metric(
            c="tab:gray",
            em=1,
            ord=1.2,
            cred=hhs_credits,
            v=v.inpatient_beds_used_covid_7_day_avg * (1e5 / pop),
        )

        metrics["COVID admits / day / 1Mp"] = make_metric(
            c="black",
            em=0,
            ord=1.3,
            cred=hhs_credits,
            v=(
                v.previous_day_admission_adult_covid_confirmed_7_day_sum
                + v.previous_day_admission_adult_covid_suspected_7_day_sum
                + v.previous_day_admission_pediatric_covid_confirmed_7_day_sum
                + v.previous_day_admission_pediatric_covid_suspected_7_day_sum
            )
            * (1e6 / pop / 7),
        )

        metrics["ICU capacity / 1Mp"] = make_metric(
            c="tab:purple",
            em=-1,
            ord=1.4,
            cred=hhs_credits,
            v=v.total_staffed_adult_icu_beds_7_day_avg * (1e6 / pop),
        )

        metrics["ICU total use / 1Mp"] = make_metric(
            c="tab:pink",
            em=0,
            ord=1.5,
            cred=hhs_credits,
            v=v.staffed_adult_icu_bed_occupancy_7_day_avg * (1e6 / pop),
        )

        metrics["ICU COVID use / 1Mp"] = make_metric(
            c="tab:pink",
            em=1,
            ord=1.6,
            cred=hhs_credits,
            v=v.staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_avg
            * (1e6 / pop),
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
