"""Functions that combine data sources into a combined representation."""

import argparse
import logging
import pickle
import re
from dataclasses import replace
from warnings import warn

import numpy
import pandas
import pandas.api.types

from covid import build_atlas
from covid import cache_policy
from covid import fetch_california_blueprint
from covid import fetch_google_mobility
from covid import fetch_state_policy
from covid import merge_covid_metrics
from covid import merge_hospital_metrics
from covid import merge_mortality_metrics
from covid import merge_vaccine_metrics
from covid import merge_variant_metrics
from covid import merge_wastewater_metrics
from covid.logging_policy import collecting_warnings
from covid.region_data import PolicyChange
from covid.region_data import make_metric

KNOWN_WARNINGS_REGEX = re.compile(
    r"|Bad CDC vax: World/US/Georgia/Chattahoochee .*"
    r"|Bad CDC vax: World/US/Arizona/Santa Cruz .*"
    r"|Bad deaths: World/AU .*"
    r"|Bad OWID vax: World/ET .*"
    r"|Cannot parse header or footer so it will be ignored"  # xlsx parser
    r"|Duplicate covariant \(World/RS\): .*"
    r"|Duplicate covariant \(World/US/Puerto Rico\): .*"
    r"|Duplicate SCAN wastewater data: CODIGA 2021-09-27"
    r"|Duplicate SCAN wastewater data: Gilroy .* 2022-01-12"
    r"|Duplicate SCAN wastewater data: Merced .* 2021-11-07"
    r"|Duplicate SCAN wastewater data: Oceanside .* 2021-03-14"
    r"|Duplicate SCAN wastewater data: Palo Alto .* 2021-02-14"
    r"|Duplicate SCAN wastewater data: Palo Alto .* 2021-04-27"
    r"|Duplicate SCAN wastewater data: Sacramento .* 2021-06-14"
    r"|Duplicate SCAN wastewater data: SantaClara_.* 2022-01-12"
    r"|Missing Biobot wastewater FIPS: 780[123]0 .*"
    r"|Missing CDC vax FIPS: (66|78)\d\d\d"
    r"|Missing Economist mortality country: (KP|NR|NU|PN|TK|TM|TV)"
    r"|Missing HHS hospital FIPS: (2|66|69|78)\d\d\d .*"
    r"|Missing OWID vax country: (GG|JE|NU|NR|PN|TK|TM|TV)"
    r"|No COVID metrics: World/(EH|MD|NG|NR|PL|RO|SK|TV).*"
    r"|No COVID metrics: World/GB/(Guernsey|Jersey|Pitcairn Islands)"
    r"|No COVID metrics: World/NZ/Niue"
    r"|No COVID metrics: World/US/Alaska/Yakutat plus Hoonah-Angoon"
    r"|Underpopulation: World/(DK|FR|NZ) .*"
    r"|Unknown CDC sero state: CR[0-9] .*"
    r"|Unknown Economist mortality country code: KSV"
    r"|Unknown OWID vax country code: OWID.*"
    r"|Unknown OWID vax state: Bureau of Prisons"
    r"|Unknown OWID vax state: Dept of Defense"
    r"|Unknown OWID vax state: Federated States of Micronesia"
    r"|Unknown OWID vax state: Indian Health Svc"
    r"|Unknown OWID vax state: Long Term Care"
    r"|Unknown OWID vax state: Marshall Islands"
    r"|Unknown OWID vax state: Republic of Palau"
    r"|Unknown OWID vax state: United States"
    r"|Unknown OWID vax state: Veterans Health"
)


def combined_atlas(session, only):
    """Returns an Atlas with filled data, checking warnings."""

    only = set(only)
    cache_path = cache_policy.cached_path(session, _atlas_cache_key(only))
    if cache_path.exists():
        logging.info(f"Loading cached atlas: {cache_path}")
        with cache_path.open(mode="rb") as cache_file:
            return pickle.load(cache_file)

    with collecting_warnings(allow_regex=KNOWN_WARNINGS_REGEX) as warnings:
        atlas = _combined_atlas(session, only)
        if warnings:
            raise ValueError(f"{len(warnings)} warnings found combining data")

    logging.info(f"Saving cached atlas: {cache_path}")
    with cache_policy.temp_to_rename(cache_path, mode="wb") as cache_file:
        pickle.dump(atlas, cache_file)
    return atlas


def _atlas_cache_key(only):
    return ":".join(["https://plague.wtf/atlas", *sorted(only)])


def _combined_atlas(session, only):
    """Assembles an atlas from data, allowing warnings."""

    atlas = build_atlas.get_atlas(session)
    merge_covid_metrics.add_metrics(session=session, atlas=atlas)

    if not only or "hospital" in only:
        merge_hospital_metrics.add_metrics(session=session, atlas=atlas)

    if not only or "mortality" in only:
        merge_mortality_metrics.add_metrics(session=session, atlas=atlas)

    if not only or "vaccine" in only:
        merge_vaccine_metrics.add_metrics(session=session, atlas=atlas)

    if not only or "variant" in only:
        merge_variant_metrics.add_metrics(session=session, atlas=atlas)

    if not only or "wastewater" in only:
        merge_wastewater_metrics.add_metrics(session=session, atlas=atlas)

    #
    # Add policy changes for US states from the state policy database.
    #

    if not only or "policy" in only:
        logging.info("Loading and merging state policy database...")
        state_policy = fetch_state_policy.get_events(session=session)
        for f, events in state_policy.groupby(level="state_fips", sort=False):
            region = atlas.by_fips.get(f)
            if region is None:
                warn(f"Unknown state policy FIPS: {f}")
                continue

            region.credits.update(fetch_state_policy.credits())

            for e in events.itertuples():
                region.metrics.policy.append(
                    PolicyChange(
                        date=e.Index[1],
                        score=e.score,
                        emoji=e.emoji,
                        text=e.policy,
                    )
                )

        logging.info("Loading and merging California blueprint data chart...")
        cal_counties = fetch_california_blueprint.get_counties(session=session)
        for county in cal_counties.values():
            region = atlas.by_fips.get(county.fips)
            if region is None:
                warn(f"FIPS {county.fips} (CA {county.name}) missing")
                continue

            region.credits.update(fetch_california_blueprint.credits())

            for date, tier in sorted(county.tier_history.items()):
                text = tier.color
                if tier.number < 10:
                    text = f"Entered {tier.color} tier ({tier.name})"
                region.metrics.policy.append(
                    PolicyChange(
                        date=date,
                        emoji=tier.emoji,
                        score=(-3 if tier.number <= 2 else +3),
                        text=text,
                    )
                )

        for r in atlas.by_jhu_id.values():
            r.metrics.policy.sort(
                key=lambda p: (p.date.date(), -abs(p.score), p.score)
            )

    #
    # Add mobility data where it's available.
    #

    if not only or "mobility" in only:
        gcols = [
            "country_region_code",
            "sub_region_1",
            "sub_region_2",
            "metro_area",
            "iso_3166_2_code",
            "census_fips_code",
        ]

        logging.info("Loading Google mobility data...")
        mobility_data = fetch_google_mobility.get_mobility(session=session)
        logging.info("Merging Google mobility data...")
        mobility_data.sort_values(by=gcols + ["date"], inplace=True)
        mobility_data.set_index(keys="date", inplace=True)
        for g, m in mobility_data.groupby(gcols, as_index=False, sort=False):
            if g[5]:
                region = atlas.by_fips.get(g[5])
            else:
                region = atlas.by_iso2.get(g[0])
                for n in g[1:4]:
                    if region and n:
                        region = region.subregions.get(n)

            if region is None:
                continue

            region.credits.update(fetch_google_mobility.credits())

            pcfb = "percent_change_from_baseline"  # common, long suffix
            region.metrics.mobility = {
                "residential": make_metric(
                    c="tab:brown",
                    em=1,
                    ord=1.0,
                    raw=100 + m[f"residential_{pcfb}"],
                ),
                "retail / recreation": make_metric(
                    c="tab:orange",
                    em=1,
                    ord=1.1,
                    raw=100 + m[f"retail_and_recreation_{pcfb}"],
                ),
                "workplaces": make_metric(
                    c="tab:red",
                    em=1,
                    ord=1.2,
                    raw=100 + m[f"workplaces_{pcfb}"],
                ),
                "grocery / pharmacy": make_metric(
                    c="tab:blue",
                    em=0,
                    ord=1.4,
                    raw=100 + m[f"grocery_and_pharmacy_{pcfb}"],
                ),
                "transit stations": make_metric(
                    "tab:purple",
                    em=0,
                    ord=1.5,
                    raw=100 + m[f"transit_stations_{pcfb}"],
                ),
            }

            # Raw daily mobility metrics are confusing, don't show them.
            for m in region.metrics.mobility.values():
                m.frame.drop(columns=["raw"], inplace=True)

    #
    # Combine metrics from subregions when not defined at the higher level.
    #

    def roll_up_metrics(r):
        totname_popvals, subs_pop = {}, 0
        for key, sub in list(r.subregions.items()):
            roll_up_metrics(sub)
            if not any(m.emphasis >= 0 for m in sub.metrics.covid.values()):
                warn(f"No COVID metrics: {sub.debug_path()}")
                del r.subregions[key]
                continue

            sub_pop = sub.metrics.total["population"]
            if not sub_pop:
                warn(f"No population: {sub.debug_path()}")
                del r.subregions[key]
                continue

            subs_pop += sub_pop
            for name, value in sub.metrics.total.items():
                totname_popvals.setdefault(name, []).append((sub_pop, value))

        pop = r.metrics.total.setdefault("population", subs_pop)
        if subs_pop > pop * 1.1:
            warn(
                f"Overpopulation: {r.debug_path()} has {pop}p, "
                f"{subs_pop}p in parts"
            )
        if subs_pop > 0 and subs_pop < pop * 0.9:
            warn(
                f"Underpopulation: {r.debug_path()} has {pop}p, "
                f"{subs_pop}p in parts"
            )

        for totname, popvals in totname_popvals.items():
            totname_pop = sum(p for p, v in popvals)
            if abs(totname_pop - pop) > pop * 0.1:
                continue  # Don't synthesize if population doesn't match.

            subs_total = sum(v for p, v in popvals)
            r.metrics.total[totname] = max(r.metrics.total[totname], subs_total)

        week = pandas.Timedelta(weeks=1)
        for cat in ["covid", "hospital", "map", "mobility", "vaccine"]:
            name_popvals = {}
            for sub in r.subregions.values():
                sub_pop = sub.metrics.total["population"]
                for name, metric in getattr(sub.metrics, cat).items():
                    name_popvals.setdefault(name, []).append((sub_pop, metric))

            cat_metrics = getattr(r.metrics, cat)
            for name, popvals in name_popvals.items():
                metric_pop = sum(p for p, v in popvals)
                if abs(metric_pop - pop) > pop * 0.1:
                    continue  # Don't synthesize if population doesn't match.

                ends = list(sorted(v.frame.index[-1] for p, v in popvals))
                end = ends[len(ends) // 2]  # Use the median end date.

                old_metric = cat_metrics.get(name)
                if old_metric and old_metric.frame.index[-1] > end - week:
                    continue  # Higher level has reasonably fresh data already.

                num = len(popvals)
                logging.debug(f"Rollup: {r.debug_path()}: {num}x {cat}[{name}]")

                # Use metric metadata from the most populated subregion
                popvals.sort(reverse=True, key=lambda pv: pv[0])
                first_pop, first_val = popvals[0]
                frame = first_pop * first_val.frame.loc[:end]

                # Merge population-weighted data from other subregions
                for next_pop, next_val in popvals[1:]:
                    next_frame = next_pop * next_val.frame.loc[:end]
                    frame = frame.add(next_frame, fill_value=0)

                cat_metrics[name] = replace(first_val, frame=frame / metric_pop)

        # Remove metrics which have no (or very little) valid data.
        for metrics in [
            r.metrics.covid,
            r.metrics.hospital,
            r.metrics.map,
            r.metrics.mobility,
            r.metrics.variant,
            r.metrics.vaccine,
            *r.metrics.wastewater.values(),
        ]:
            for name, m in list(metrics.items()):
                if m.frame.value.count() < 2:
                    del metrics[name]

        # Clean up some categories if we didn't get any "headline" data.
        for category in r.metrics.vaccine, r.metrics.mobility:
            if not any(m.emphasis > 0 for m in category.values()):
                category.clear()

        # Remove wastewater sites with no data.
        for name, ww in list(r.metrics.wastewater.items()):
            if not ww:
                del r.metrics.wastewater[name]

    logging.info("Rolling up metrics...")
    roll_up_metrics(atlas.world)

    # World mobility rollup is weird.
    atlas.world.metrics.mobility.clear()

    #
    # Interpolate synchronized weekly map metrics from time series metrics.
    #

    # Sync map metric weekly data points to this end date.
    latest = max(m.frame.index[-1] for m in atlas.world.metrics.covid.values())

    def add_map_metric(region, c_name, m_name, mul, col, i_col, d_col):
        m = region.metrics.covid.get(c_name)
        if m is not None:
            first = m.frame.index[0].astimezone(latest.tz)
            weeks = (latest - first) // pandas.Timedelta(days=7)
            dates = pandas.date_range(end=latest, periods=weeks, freq="7D")
            value = mul * numpy.interp(dates, m.frame.index, m.frame.value)
            if (~numpy.isnan(value)).any():
                region.metrics.map[m_name] = replace(
                    m,
                    frame=pandas.DataFrame({"value": value}, index=dates),
                    color=col,
                    increase_color=i_col,
                    decrease_color=d_col,
                )

    def make_map_metrics(region):
        for sub in region.subregions.values():
            make_map_metrics(sub)

        mul = region.metrics.total["population"] / 50  # 100K => 2K, 10M => 200K
        add_map_metric(
            region,
            "COVID positives / day / 100Kp",
            "pos x2K",
            mul,
            "#0000FF50",
            "#0000FFA0",
            "#00FF00A0",
        )

        add_map_metric(
            region,
            "COVID deaths / day / 10Mp",
            "deaths x200K",
            mul,
            "#FF000050",
            "#FF0000A0",
            None,
        )

    if not only or "maps" in only:
        make_map_metrics(atlas.world)

    return atlas


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument("--only", nargs="*", default=[])
    parser.add_argument("--print_data", action="store_true")

    args = parser.parse_args()
    session = cache_policy.new_session(args)
    atlas = combined_atlas(session=session, only=args.only)
    print(atlas.world.debug_tree(with_data=args.print_data))
