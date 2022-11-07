"""Functions to merge hospital utilization metrics into a RegionAtlas"""

import logging
from warnings import warn

import pycountry

import covid.fetch_hhs_hospitalizations
import covid.fetch_ourworld_hospitalizations
from covid.region_data import make_metric


def owid_region(atlas, owid_code):
    if owid_code == "OWID_ENG":
        iso3, sub = "GBR", "England"
    elif owid_code == "OWID_SCT":
        iso3, sub = "GBR", "Scotland"
    elif owid_code == "OWID_WLS":
        iso3, sub = "GBR", "Wales"
    elif owid_code == "OWID_NIR":
        iso3, sub = "GBR", "Northern Ireland"
    else:
        iso3, sub = owid_code, None

    cc = pycountry.countries.get(alpha_3=iso3)
    if cc is None:
        warn(f"Unknown OWID country code: {iso3}")
        return None

    region = atlas.by_iso2.get(cc.alpha_2)
    if region is None:
        warn(f"Missing OWID country: {cc.alpha_2}")
        return None

    if sub:
        region = region.subregions.get(sub)
        if region is None:
            warn(f"Missing OWID subregion: {region.debug_path()}/{sub}")
            return None

    pop = region.metrics.total["population"]
    if not (pop > 0):
        warn(f"No population: {region.debug_path()} (pop={pop})")
        return None

    return region



def add_metrics(session, atlas):
    logging.info("Loading and merging ourworldindata hospitalization data...")
    covid.fetch_ourworld_hospitalizations.get_occupancy(session)
    adm_df = covid.fetch_ourworld_hospitalizations.get_admissions(session)
    for iso3, v in adm_df.groupby(level="iso_code", as_index=False):
        v.reset_index("iso_code", drop=True, inplace=True)
        region = owid_region(atlas, iso3)
        if region is None:
            continue

        region.credits.update(covid.fetch_ourworld_hospitalizations.credits())

        pop = region.metrics.total["population"]
        metrics = region.metrics.hospital
        metrics["COVID admits / day / 1Mp"] = make_metric(
            c="black",
            em=0,
            ord=1.3,
            v=v["new hospital admissions"] * (1e6 / pop),
        )

        metrics["ICU COVID admits / day / 10Mp"] = make_metric(
            c="tab:purple",
            em=0,
            ord=1.7,
            v=v["new ICU admissions"] * (1e7 / pop),
        )

    occ_df = covid.fetch_ourworld_hospitalizations.get_occupancy(session)
    for iso3, v in occ_df.groupby(level="iso_code", as_index=False):
        v.reset_index("iso_code", drop=True, inplace=True)
        region = owid_region(atlas, iso3)
        if region is None:
            continue

        pop = region.metrics.total["population"]
        metrics = region.metrics.hospital
        metrics["COVID use / 100Kp"] = make_metric(
            c="tab:gray",
            em=1,
            ord=1.2,
            v=v["hospital occupancy"] * (1e5 / pop),
        )

        metrics["ICU COVID use / 1Mp"] = make_metric(
            c="tab:pink",
            em=1,
            ord=1.6,
            v=v["ICU occupancy"] * (1e6 / pop),
        )

    logging.info("Loading and merging US HHS hospitalization data...")
    hhs_df = covid.fetch_hhs_hospitalizations.get_hospitalizations(session)
    for fips, per_fips in hhs_df.groupby(level="fips_code", as_index=False):
        region = atlas.by_fips.get(fips)
        if region is None:
            row = per_fips.iloc[0]
            warn(
                f"Missing HHS hospital FIPS: {fips}"
                f" ({row.city} {row.state} {row.zip:.0f})"
            )
            continue

        pop = region.metrics.total["population"]
        if not (pop > 0):
            warn(f"No population: {region.debug_path()} (pop={pop})")
            continue

        region.credits.update(covid.fetch_hhs_hospitalizations.credits())

        per_fips = per_fips.select_dtypes(float)
        per_fips.clip(lower=0, inplace=True)
        v = per_fips.groupby(level="collection_week").sum()

        metrics = region.metrics.hospital
        metrics["capacity / 100Kp"] = make_metric(
            c="tab:gray",
            em=-1,
            ord=1.0,
            v=v.inpatient_beds_7_day_avg * (1e5 / pop),
        )

        metrics["total use / 100Kp"] = make_metric(
            c="tab:gray",
            em=0,
            ord=1.1,
            v=v.inpatient_beds_used_7_day_avg * (1e5 / pop),
        )

        metrics["COVID use / 100Kp"] = make_metric(
            c="tab:gray",
            em=1,
            ord=1.2,
            v=v.inpatient_beds_used_covid_7_day_avg * (1e5 / pop),
        )

        metrics["COVID admits / day / 1Mp"] = make_metric(
            c="black",
            em=0,
            ord=1.3,
            v=(
                v.previous_day_admission_adult_covid_confirmed_7_day_sum
                + v.previous_day_admission_adult_covid_suspected_7_day_sum
                + v.previous_day_admission_pediatric_covid_confirmed_7_day_sum
                + v.previous_day_admission_pediatric_covid_suspected_7_day_sum
            )
            * (1e6 / pop / 7),
        )

        metrics["ICU capacity / 1Mp"] = make_metric(
            c="tab:pink",
            em=-1,
            ord=1.4,
            v=v.total_staffed_adult_icu_beds_7_day_avg * (1e6 / pop),
        )

        metrics["ICU total use / 1Mp"] = make_metric(
            c="tab:pink",
            em=0,
            ord=1.5,
            v=v.staffed_adult_icu_bed_occupancy_7_day_avg * (1e6 / pop),
        )

        metrics["ICU COVID use / 1Mp"] = make_metric(
            c="tab:pink",
            em=1,
            ord=1.6,
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
