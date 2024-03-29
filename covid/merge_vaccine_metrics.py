"""Function to merge vaccination metrics into a RegionAtlas"""

import logging
from warnings import warn

import pycountry
import us

import covid.fetch_cdc_vaccinations
import covid.fetch_ourworld_vaccinations
from covid.region_data import make_metric


def add_metrics(session, atlas):
    logging.info("Loading CDC vaccination data...")
    cdc_data = covid.fetch_cdc_vaccinations.get_vaccinations(session=session)

    logging.info("Merging CDC vaccination data...")
    for fips, v in cdc_data.groupby("FIPS", as_index=False, sort=False):
        region = atlas.by_fips.get(fips)
        if region is None:
            warn(f"Missing CDC vax FIPS: {fips}")
            continue

        pop = region.metrics.total["population"]
        if not (pop > 0):
            warn(f"No population: {region.debug_path()} (pop={pop})")
            continue

        v.reset_index(level="FIPS", drop=True, inplace=True)
        v.Administered_Dose1_Recip.fillna(method="ffill", inplace=True)
        v.Series_Complete_Yes.fillna(method="ffill", inplace=True)
        v.Booster_Doses.fillna(method="ffill", inplace=True)

        if v.Series_Complete_Yes.isnull().all():
            continue  # No actual data

        vaxxed = v.Series_Complete_Yes.iloc[-1]
        if not (0 <= vaxxed <= pop * 1.1 + 10000):
            warn(f"Bad CDC vax: {region.debug_path()} ({vaxxed}/{pop}p)")
            continue

        region.credits.update(covid.fetch_cdc_vaccinations.credits())
        region.metrics.total["vaccinated"] = vaxxed

        vax_metrics = region.metrics.vaccine
        vax_metrics["people given any doses / 100p"] = make_metric(
            c="tab:olive",
            em=0,
            ord=1.2,
            v=v.Administered_Dose1_Recip * (100 / pop),
        )

        vax_metrics["people fully vaccinated / 100p"] = make_metric(
            c="tab:green",
            em=1,
            ord=1.3,
            v=v.Series_Complete_Yes * (100 / pop),
        )

        vax_metrics["booster doses given / 100p"]: make_metric(
            c="tab:purple",
            em=1,
            ord=1.4,
            v=v.Booster_Doses * (100 / pop),
        )

    logging.info("Loading and merging ourworldindata vaccination data...")

    # https://github.com/unitedstates/python-us/issues/65
    state_abbr_by_name = us.states.mapping("name", "abbr")

    owid_data = covid.fetch_ourworld_vaccinations.get_vaccinations(
        session=session
    )
    vcols = ["iso_code", "state"]
    owid_data.state.fillna("", inplace=True)  # Or groupby() drops them.
    owid_data.sort_values(by=vcols + ["date"], inplace=True)
    owid_data.set_index(keys="date", inplace=True)
    for (iso3, admin2), v in owid_data.groupby(vcols, as_index=False):
        if iso3 == "OWID_WRL":
            cc = None
        elif iso3 == "OWID_ENG":
            cc, admin2 = pycountry.countries.get(alpha_2="GB"), "England"
        elif iso3 == "OWID_SCT":
            cc, admin2 = pycountry.countries.get(alpha_2="GB"), "Scotland"
        elif iso3 == "OWID_NIR":
            cc = pycountry.countries.get(alpha_2="GB")
            admin2 = "Northern Ireland"
        elif iso3 == "OWID_WLS":
            cc, admin2 = pycountry.countries.get(alpha_2="GB"), "Wales"
        else:
            cc = pycountry.countries.get(alpha_3=iso3)
            if cc is None:
                warn(f"Unknown OWID vax country code: {iso3}")
                continue

        region = atlas.by_iso2.get(cc.alpha_2) if cc else atlas.world
        if region is None:
            warn(f"Missing OWID vax country: {cc.alpha_2}")
            continue

        if admin2:
            if cc.alpha_2 == "US":
                # Data includes "New York State", lookup() needs "New York"
                abbr = state_abbr_by_name.get(admin2.replace(" State", ""))
                if not abbr:
                    warn(f"Unknown OWID vax state: {admin2}")
                    continue

                fips = us.states.lookup(abbr).fips
                region = atlas.by_fips.get(int(fips))
                if region is None:
                    warn(f"Missing OWID vax FIPS: {fips}")
                    continue
            else:
                region = region.subregions.get(admin2)
                if region is None:
                    warn(f"Unknown OWID vax subregion: {admin2}")

        pop = region.metrics.total["population"]
        if not (pop > 0):
            warn(f"No population: {region.debug_path()} (pop={pop})")
            continue

        v.total_distributed.fillna(method="ffill", inplace=True)
        v.total_vaccinations.fillna(method="ffill", inplace=True)
        v.total_boosters.fillna(method="ffill", inplace=True)
        v.people_vaccinated.fillna(method="ffill", inplace=True)
        v.people_fully_vaccinated.fillna(method="ffill", inplace=True)

        vaxxed = v.people_fully_vaccinated.iloc[-1]
        if not (0 <= vaxxed <= pop * 1.1 + 10000):
            warn(f"Bad OWID vax: {region.debug_path()} ({vaxxed}/{pop}p)")
            continue

        region.credits.update(covid.fetch_ourworld_vaccinations.credits())
        region.metrics.total["vaccinated"] = vaxxed

        vax_metrics = region.metrics.vaccine
        vax_metrics["people given any doses / 100p"] = make_metric(
            c="tab:olive",
            em=0,
            ord=1.2,
            v=v.people_vaccinated * (100 / pop),
        )

        vax_metrics["people fully vaccinated / 100p"] = make_metric(
            c="tab:green",
            em=1,
            ord=1.3,
            v=v.people_fully_vaccinated * (100 / pop),
        )

        vax_metrics["total booster doses / 100p"] = make_metric(
            c="tab:purple",
            em=1,
            ord=1.4,
            v=v.total_boosters * (100 / pop),
        )

        vax_metrics["doses / day / 5Kp"] = make_metric(
            c="tab:cyan",
            em=0,
            ord=1.5,
            v=v.daily_vaccinations * (5000 / pop),
            raw=v.daily_vaccinations_raw * (5000 / pop),
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
