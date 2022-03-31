"""Functions that combine data sources into a combined representation."""

import argparse
import itertools
import logging
import pickle
import re
import warnings
from dataclasses import replace

import matplotlib.cm
import numpy
import pandas
import pandas.api.types
import pycountry
import us

from covid import build_atlas
from covid import cache_policy
from covid import fetch_california_blueprint
from covid import fetch_cdc_prevalence
from covid import fetch_covariants
from covid import fetch_google_mobility
from covid import fetch_state_policy
from covid import merge_covid_metrics
from covid import merge_hospital_metrics
from covid import merge_mortality_metrics
from covid import merge_vaccine_metrics
from covid import merge_wastewater_metrics
from covid.logging_policy import collecting_warnings
from covid.region_data import PolicyChange
from covid.region_data import make_metric

# Reusable command line arguments for data collection.
argument_parser = argparse.ArgumentParser(add_help=False)
arg_group = argument_parser.add_argument_group("data gathering")
arg_group.add_argument("--no_california_blueprint", action="store_true")
arg_group.add_argument("--no_cdc_prevalence", action="store_true")
arg_group.add_argument("--no_covariants", action="store_true")
arg_group.add_argument("--no_covid_metrics", action="store_true")
arg_group.add_argument("--no_google_mobility", action="store_true")
arg_group.add_argument("--no_hospital_metrics", action="store_true")
arg_group.add_argument("--no_maps", action="store_true")
arg_group.add_argument("--no_mortality_metrics", action="store_true")
arg_group.add_argument("--no_state_policy", action="store_true")
arg_group.add_argument("--no_vaccine_metrics", action="store_true")
arg_group.add_argument("--use_wastewater_metrics", action="store_true")


KNOWN_WARNINGS_REGEX = re.compile(
    r"|Bad CDC vax: World/US/Georgia/Chattahoochee .*"
    r"|Bad deaths: World/AU .*"
    r"|Bad OWID vax: World/ET .*"
    r"|Cannot parse header or footer so it will be ignored"  # xlsx parser
    r"|Duplicate covariant \(World/RS\): .*"
    r"|Missing CDC vax FIPS: (66|78)\d\d\d"
    r"|Missing Economist mortality country: (KP|NR|NU|PN|TK|TM|TV)"
    r"|Missing HHS hospital FIPS: (2|66|69|78)\d\d\d .*"
    r"|Missing OWID vax country: (GG|JE|NU|NR|PN|TK|TM|TV)"
    r"|No COVID metrics: World/(EH|NG|PL).*"
    r"|No COVID metrics: World/GB/(Guernsey|Jersey)"
    r"|No COVID metrics: World/US/Alaska/Yakutat plus Hoonah-Angoon"
    r"|Underpopulation: World/(DK|FR|NZ) .*"
    r"|Unknown CDC sero state: CR[0-9] .*"
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


def get_world(session, args):
    """Returns data organized into a tree rooted at a World region.
    Warnings are captured and printed, then raise a ValueError exception."""

    cache_path = cache_policy.cached_path(session, _world_cache_key(args))
    if cache_path.exists():
        logging.info(f"Loading cached world: {cache_path}")
        with cache_path.open(mode="rb") as cache_file:
            return pickle.load(cache_file)

    with collecting_warnings(allow_regex=KNOWN_WARNINGS_REGEX) as warnings:
        world = _compute_world(session, args)
        if warnings:
            raise ValueError(f"{len(warnings)} warnings found combining data")

    logging.info(f"Saving cached world: {cache_path}")
    with cache_policy.temp_to_rename(cache_path, mode="wb") as cache_file:
        pickle.dump(world, cache_file)
    return world


def _world_cache_key(args):
    # Only include args understood by this module.
    ks = list(sorted(vars(argument_parser.parse_args([])).keys()))
    return "https://plague.wtf/world" + "".join(
        f":{k}={getattr(args, k)}" for k in ks
    )


def _compute_world(session, args):
    """Assembles a World region from data, allowing warnings."""

    atlas = build_atlas.get_atlas(session)

    if not args.no_covid_metrics:
        merge_covid_metrics.add_metrics(session=session, atlas=atlas)

    if not args.no_mortality_metrics:
        merge_mortality_metrics.add_metrics(session=session, atlas=atlas)

    if not args.no_hospital_metrics:
        merge_hospital_metrics.add_metrics(session=session, atlas=atlas)

    if args.use_wastewater_metrics:
        merge_wastewater_metrics.add_metrics(session=session, atlas=atlas)

    if not args.no_vaccine_metrics:
        merge_vaccine_metrics.add_metrics(session=session, atlas=atlas)

    #
    # Add variant breakdown
    #

    if not args.no_covariants:
        logging.info("Loading and merging CoVariants data...")
        cov_credits = fetch_covariants.credits()
        covar = fetch_covariants.get_variants(session=session)

        totals = covar.groupby("variant")["found"].sum()
        vars = [v[0] for v in sorted(totals.items(), key=lambda v: v[1])]
        colors = dict(zip(vars, itertools.cycle(matplotlib.cm.tab20.colors)))

        region_cols = ["country", "region"]
        covar.sort_values(region_cols + ["date"], inplace=True)
        covar.set_index(keys="date", inplace=True)
        for r, rd in covar.groupby(region_cols, as_index=False, sort=False):
            if (r[0], r[1]) == ("United States", "USA"):
                continue  # Covered separately as ("USA", "").

            c_find = {
                "Curacao": "Curaçao",
                "South Korea": "Republic Of Korea",
                "Sint Maarten": "Sint Maarten (Dutch part)",
                "Democratic Republic of the Congo": "Congo, The Democratic Republic of the",
            }.get(r[0], r[0])
            try:
                countries = [pycountry.countries.lookup(c_find)]
            except LookupError:
                try:
                    countries = pycountry.countries.search_fuzzy(c_find)
                except LookupError:
                    warnings.warn(f'Unknown covariant country: "{c_find}"')
                    continue

            region = atlas.by_iso2.get(countries[0].alpha_2)
            if region is None:
                continue  # Valid country but not in skeleton

            r_find = {"Washington DC": "District of Columbia"}.get(r[1], r[1])
            if r_find:
                path, region = region.path(), region.subregions.get(r_find)
                if region is None:
                    warnings.warn(f"Unknown covariant region: {path}/{r_find}")
                    continue

            v_totals = v_others = []
            for v, vd in rd.groupby("variant", as_index=False):
                if not v:
                    v_others = vd.found
                    v_totals = vd.found
                    continue

                if v in region.metrics["variant"]:
                    warnings.warn(f"Duplicate covariant ({region.path()}): {v}")
                    continue

                if len(v_totals) != len(vd):
                    warnings.warn(
                        f"Bad covariant data ({region.path()}): "
                        f"len totals={len(v_totals)} len data={len(vd)}"
                    )
                    continue

                v_others = v_others - vd.found
                region.metrics["variant"][v] = make_metric(
                    c=colors[v],
                    em=1,
                    ord=0,
                    cred=cov_credits,
                    v=vd.found * 100.0 / v_totals,
                )

            other_variants = make_metric(
                c=(0.9, 0.9, 0.9),
                em=1,
                ord=0,
                cred=cov_credits,
                v=v_others * 100.0 / v_totals,
            )

            region.metrics["variant"] = {
                "original/other": other_variants,
                **region.metrics["variant"],
            }

    #
    # Add prevalence estimates
    #

    if not args.no_cdc_prevalence:
        logging.info("Loading and merging CDC prevalence estimates...")
        cdc_credits = fetch_cdc_prevalence.credits()
        cdc_data = fetch_cdc_prevalence.get_prevalence(session)

        rcols = ["Region Abbreviation", "Region"]
        for (abbr, name), v in cdc_data.groupby(rcols, as_index=False):
            if abbr.lower() == "all":
                region, name = atlas.by_iso2.get("US"), ""
                if region is None:
                    warnings.warn(f"Missing US for CDC")
                    continue
            else:
                s = us.states.lookup(abbr.split("-")[0])
                if not s:
                    warnings.warn(f"Unknown CDC sero state: {abbr} ({name})")
                    continue

                region = atlas.by_fips.get(int(s.fips))
                if region is None:
                    warnings.warn(f"Missing CDC sero FIPS: {s.fips} ({s.name})")
                    continue

            name = " ".join(name.replace("Region", "").split())
            if name.lower() == region.name.lower() or not name:
                name = ""
            else:
                for state in us.states.STATES_AND_TERRITORIES:
                    name = name.replace(state.name, state.abbr)
                name = name.replace("Southern", "S.").replace("Northern", "N.")
                name = name.replace("Western", "W.").replace("Eastern", "E.")
                name = name.replace("Southeastern", "SE.")
                name = name.replace("Northeastern", "NE.")
                name = name.replace("Southwestern", "SW.")
                name = name.replace("Northwestern", "NW.")
                name = f" ({name})"

            # plot midmonth (data spans the month)
            v.reset_index(rcols, drop=True, inplace=True)
            v.index = v.index + pandas.Timedelta(days=14)
            region_i = len(region.metrics["serology"]) // 2
            region.metrics["serology"][f"infected or vax{name}"] = make_metric(
                c=matplotlib.cm.tab20b.colors[region_i],
                em=1,
                ord=1.0,
                cred=cdc_credits,
                v=v["Rate %[Total Prevalence] Combined"],
            )

            region.metrics["serology"][f"infected{name}"] = make_metric(
                c=matplotlib.cm.tab20c.colors[region_i + 4],
                em=0,
                ord=1.1,
                cred=cdc_credits,
                v=v["Rate %[Total Prevalence] Infection"],
            )

    #
    # Add policy changes for US states from the state policy database.
    #

    if not args.no_state_policy:
        logging.info("Loading and merging state policy database...")
        policy_credits = fetch_state_policy.credits()
        state_policy = fetch_state_policy.get_events(session=session)
        for f, events in state_policy.groupby(level="state_fips", sort=False):
            region = atlas.by_fips.get(f)
            if region is None:
                warnings.warn(f"Unknown state policy FIPS: {f}")
                continue

            for e in events.itertuples():
                region.policy_changes.append(
                    PolicyChange(
                        date=e.Index[1],
                        score=e.score,
                        emoji=e.emoji,
                        text=e.policy,
                        credits=policy_credits,
                    )
                )

    if not args.no_california_blueprint:
        logging.info("Loading and merging California blueprint data chart...")
        cal_credits = fetch_california_blueprint.credits()
        cal_counties = fetch_california_blueprint.get_counties(session=session)
        for county in cal_counties.values():
            region = atlas.by_fips.get(county.fips)
            if region is None:
                warnings.warn(f"FIPS {county.fips} (CA {county.name}) missing")
                continue

            for date, tier in sorted(county.tier_history.items()):
                text = tier.color
                if tier.number < 10:
                    text = f"Entered {tier.color} tier ({tier.name})"
                region.policy_changes.append(
                    PolicyChange(
                        date=date,
                        emoji=tier.emoji,
                        score=(-3 if tier.number <= 2 else +3),
                        text=text,
                        credits=cal_credits,
                    )
                )

    for r in atlas.by_jhu_id.values():
        r.policy_changes.sort(
            key=lambda p: (p.date.date(), -abs(p.score), p.score)
        )

    #
    # Add mobility data where it's available.
    #

    if not args.no_google_mobility:
        gcols = [
            "country_region_code",
            "sub_region_1",
            "sub_region_2",
            "metro_area",
            "iso_3166_2_code",
            "census_fips_code",
        ]

        logging.info("Loading Google mobility data...")
        mobility_credits = fetch_google_mobility.credits()
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

            pcfb = "percent_change_from_baseline"  # common, long suffix
            region.metrics["mobility"] = {
                "residential": make_metric(
                    c="tab:brown",
                    em=1,
                    ord=1.0,
                    cred=mobility_credits,
                    raw=100 + m[f"residential_{pcfb}"],
                ),
                "retail / recreation": make_metric(
                    c="tab:orange",
                    em=1,
                    ord=1.1,
                    cred=mobility_credits,
                    raw=100 + m[f"retail_and_recreation_{pcfb}"],
                ),
                "workplaces": make_metric(
                    c="tab:red",
                    em=1,
                    ord=1.2,
                    cred=mobility_credits,
                    raw=100 + m[f"workplaces_{pcfb}"],
                ),
                "grocery / pharmacy": make_metric(
                    c="tab:blue",
                    em=0,
                    ord=1.4,
                    cred=mobility_credits,
                    raw=100 + m[f"grocery_and_pharmacy_{pcfb}"],
                ),
                "transit stations": make_metric(
                    "tab:purple",
                    em=0,
                    ord=1.5,
                    cred=mobility_credits,
                    raw=100 + m[f"transit_stations_{pcfb}"],
                ),
            }

            # Raw daily mobility metrics are confusing, don't show them.
            for m in region.metrics["mobility"].values():
                m.frame.drop(columns=["raw"], inplace=True)

    #
    # Combine metrics from subregions when not defined at the higher level.
    #

    def roll_up_metrics(r):
        catname_popvals, total_popvals, sub_pop_total = {}, {}, 0
        for key, sub in list(r.subregions.items()):
            roll_up_metrics(sub)
            if not sub.metrics["covid"]:
                warnings.warn(f"No COVID metrics: {sub.path()}")
                del r.subregions[key]
                continue

            sub_pop = sub.totals["population"]
            sub_pop_total += sub_pop

            for name, value in sub.totals.items():
                total_popvals.setdefault(name, []).append((sub_pop, value))

            for cat, metrics in sub.metrics.items():
                if cat in ("variant", "serology", "wastewater"):
                    continue  # TODO: Add a per-metric no-rollup flag?
                for name, value in metrics.items():
                    catname, popval = (cat, name), (sub_pop, value)
                    catname_popvals.setdefault(catname, []).append(popval)

        pop = r.totals["population"]
        if pop == 0:
            pop = r.totals["population"] = sub_pop_total
        if sub_pop_total > pop * 1.1:
            warnings.warn(
                f"Overpopulation: {r.path()} has {pop}p, "
                f"{sub_pop_total}p in parts"
            )
        if sub_pop_total > 0 and sub_pop_total < pop * 0.9:
            warnings.warn(
                f"Underpopulation: {r.path()} has {pop}p, "
                f"{sub_pop_total}p in parts"
            )

        for name, popvals in total_popvals.items():
            total_pop = sum(p for p, v in popvals)
            if abs(total_pop - pop) > pop * 0.1:
                continue  # Don't synthesize if population doesn't match.

            sub_total = sum(val for pop, val in popvals)
            r.totals[name] = max(r.totals.get(name, 0), sub_total)

        week = pandas.Timedelta(weeks=1)
        for (cat, name), popvals in catname_popvals.items():
            metric_pop = sum(p for p, v in popvals)
            if abs(metric_pop - pop) > pop * 0.1:
                continue  # Don't synthesize if population doesn't match.

            ends = list(sorted(v.frame.index[-1] for p, v in popvals))
            end = ends[len(ends) // 2]  # Use the median end date.

            old_metric = r.metrics[cat].get(name)
            if old_metric and old_metric.frame.index[-1] > end - week:
                continue  # Higher level has reasonably fresh data already

            popvals.sort(reverse=True, key=lambda pv: pv[0])  # Highest first.
            first_pop, first_val = popvals[0]  # Most populated entry.
            frame = first_pop * first_val.frame.loc[:end]
            for next_pop, next_val in popvals[1:]:
                next_frame = next_pop * next_val.frame.loc[:end]
                frame = frame.add(next_frame, fill_value=0)

            r.metrics[cat][name] = replace(
                first_val,
                frame=frame / metric_pop,
                credits=dict(c for p, v in popvals for c in v.credits.items()),
            )

        # Remove metrics which have no (or very little) valid data
        for cat in r.metrics.values():
            for name, m in list(cat.items()):
                if m.frame.value.count() < 2:
                    del cat[name]

        # Clean up some categories if we didn't get any "headline" data
        for cat in ("vaccine", "serology", "mobility"):
            if not any(m.emphasis > 0 for m in r.metrics[cat].values()):
                r.metrics[cat].clear()

    logging.info("Rolling up metrics...")
    roll_up_metrics(atlas.world)

    atlas.world.metrics.pop("mobility")  # World mobility rollup is weird.

    #
    # Interpolate synchronized weekly map metrics from time series metrics.
    #

    # Sync map metric weekly data points to this end date.
    latest = max(
        m.frame.index[-1] for m in atlas.world.metrics["covid"].values()
    )

    def add_map_metric(region, c_name, m_name, mul, col, i_col, d_col):
        m = region.metrics["covid"].get(c_name)
        if m is not None:
            first = m.frame.index[0].astimezone(latest.tz)
            weeks = (latest - first) // pandas.Timedelta(days=7)
            dates = pandas.date_range(end=latest, periods=weeks, freq="7D")
            value = mul * numpy.interp(dates, m.frame.index, m.frame.value)
            if (~numpy.isnan(value)).any():
                region.metrics["map"][m_name] = replace(
                    m,
                    frame=pandas.DataFrame({"value": value}, index=dates),
                    color=col,
                    increase_color=i_col,
                    decrease_color=d_col,
                )

    def make_map_metrics(region):
        for sub in region.subregions.values():
            make_map_metrics(sub)

        mul = region.totals["population"] / 50  # 100K => 2K, 10Mp => 200K
        add_map_metric(
            region,
            "COVID cases / day / 100Kp",
            "cases x2K",
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

    if not args.no_maps:
        make_map_metrics(atlas.world)

    return atlas.world


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        parents=[cache_policy.argument_parser, argument_parser]
    )
    parser.add_argument("--print_credits", action="store_true")
    parser.add_argument("--print_data", action="store_true")

    args = parser.parse_args()
    session = cache_policy.new_session(args)
    world = get_world(session=session, args=args)
    print(
        world.debug_tree(
            with_credits=args.print_credits, with_data=args.print_data
        )
    )
