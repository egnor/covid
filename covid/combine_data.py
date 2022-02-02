"""Functions that combine data sources into a combined representation."""

import argparse
import collections
import itertools
import os.path
import pickle
import re
import sys
import traceback
import warnings
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from warnings import warn

import matplotlib.cm
import numpy
import pandas
import pandas.api.types
import pycountry
import us

from covid import cache_policy
from covid import fetch_california_blueprint
from covid import fetch_cdc_prevalence
from covid import fetch_cdc_vaccinations
from covid import fetch_covariants
from covid import fetch_google_mobility
from covid import fetch_jhu_csse
from covid import fetch_ourworld_vaccinations
from covid import fetch_state_policy

# Reusable command line arguments for data collection.
argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group("data gathering")
argument_group.add_argument("--no_california_blueprint", action="store_true")
argument_group.add_argument("--no_cdc_prevalence", action="store_true")
argument_group.add_argument("--no_cdc_vaccinations", action="store_true")
argument_group.add_argument("--no_covariants", action="store_true")
argument_group.add_argument("--no_google_mobility", action="store_true")
argument_group.add_argument("--no_jhu_csse", action="store_true")
argument_group.add_argument("--no_ourworld_vaccinations", action="store_true")
argument_group.add_argument("--no_state_policy", action="store_true")


KNOWN_WARNINGS_REGEX = re.compile(
    r"|Bad deaths: World/AU .*"
    r"|Cannot parse header or footer so it will be ignored"  # xlsx parser
    r"|Duplicate covariant \(World/RS\): .*"
    r"|Missing CDC vax FIPS: (66|78).*"
    r"|Missing OWID country: (GG|JE|NU|NR|PN|TK|TM|TV)"
    r"|No COVID metrics: World/(EH|NG|PL).*"
    r"|No COVID metrics: World/US/Alaska/Yakutat plus Hoonah-Angoon"
    r"|Underpopulation: World/(DK|FR|NZ) .*"
    r"|Unknown CDC sero state: CR[0-9] .*"
    r"|Unknown OWID state: Bureau of Prisons"
    r"|Unknown OWID state: Dept of Defense"
    r"|Unknown OWID state: Federated States of Micronesia"
    r"|Unknown OWID state: Indian Health Svc"
    r"|Unknown OWID state: Long Term Care"
    r"|Unknown OWID state: Marshall Islands"
    r"|Unknown OWID state: Republic of Palau"
    r"|Unknown OWID state: United States"
    r"|Unknown OWID state: Veterans Health"
)


@dataclass(frozen=True)
class Metric:
    frame: pandas.DataFrame
    color: str
    emphasis: int = 0
    order: float = 0
    increase_color: Optional[str] = None
    decrease_color: Optional[str] = None
    credits: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PolicyChange:
    date: pandas.Timestamp
    score: int
    emoji: str
    text: str
    credits: Dict[str, str]


@dataclass(eq=False)
class Region:
    name: str
    short_name: str
    iso_code: Optional[str] = None
    fips_code: Optional[int] = None
    zip_code: Optional[int] = None
    place_id: Optional[str] = None
    lat_lon: Optional[Tuple[float, float]] = None
    totals: collections.Counter = field(default_factory=collections.Counter)
    parent: Optional["Region"] = field(default=None, repr=0)
    subregions: Dict[str, "Region"] = field(default_factory=dict, repr=0)
    map_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    covid_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    variant_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    vaccine_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    serology_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    mobility_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    policy_changes: List[PolicyChange] = field(default_factory=list, repr=0)

    def path(r):
        return f"{r.parent.path()}/{r.short_name}" if r.parent else r.name

    def matches_regex(r, rx):
        rx = rx if isinstance(rx, re.Pattern) else (rx and re.compile(rx, re.I))
        return bool(
            not rx
            or rx.fullmatch(r.name)
            or rx.fullmatch(r.path())
            or rx.fullmatch(r.path().replace(" ", "_"))
        )


def get_world(session, args, verbose=False):
    """Returns data organized into a tree rooted at a World region.
    Warnings are captured and printed, then raise a ValueError exception."""

    vprint = lambda *a, **k: print(*a, **k) if verbose else None
    cache_path = cache_policy.cached_path(session, _world_cache_key(args))
    if cache_path.exists():
        vprint(f"Loading cached world: {cache_path}")
        with cache_path.open(mode="rb") as cache_file:
            return pickle.load(cache_file)

    warning_count = 0

    def show_and_count(message, category, filename, lineno, file, line):
        # Allow known data glitches.
        text = str(message).strip()
        if KNOWN_WARNINGS_REGEX.fullmatch(text):
            vprint(f"=== {text}")
        else:
            nonlocal warning_count
            warning_count += 1
            where = f"{os.path.basename(filename)}:{lineno}"
            print(f"*** #{warning_count} ({where}) {text}")
            traceback.print_stack(file=sys.stdout)
            print()

    try:
        warnings.showwarning, saved = show_and_count, warnings.showwarning
        world = _compute_world(session, args, vprint)
    finally:
        warnings.showwarning = saved

    if warning_count:
        raise ValueError(f"{warning_count} warnings found combining data")

    vprint(f"Saving cached world: {cache_path}")
    with cache_policy.temp_to_rename(cache_path, mode="wb") as cache_file:
        pickle.dump(world, cache_file)
    return world


def _world_cache_key(args):
    # Only include args understood by this module.
    ks = list(sorted(vars(argument_parser.parse_args([])).keys()))
    return "https://plague.wtf/world" + "".join(
        f":{k}={getattr(args, k)}" for k in ks
    )


def _compute_world(session, args, vprint):
    """Assembles a World region from data, allowing warnings."""

    vprint("Loading place data...")
    world = _make_skeleton(session)

    # Index by various forms of ID for merging data in.
    region_by_id = {}
    region_by_iso = {}
    region_by_fips = {}

    def index_region_tree(r):
        for index_dict, key in [
            (region_by_iso, r.iso_code),
            (region_by_fips, r.fips_code),
            (region_by_id, r.place_id),
        ]:
            if key is not None:
                index_dict[key] = r
        for sub in r.subregions.values():
            index_region_tree(sub)

    index_region_tree(world)

    #
    # Add metrics from the JHU CSSE dataset
    #

    if not args.no_jhu_csse:
        vprint("Loading JHU CSSE dataset (COVID)...")
        jhu_credits = fetch_jhu_csse.credits()
        jhu_covid = fetch_jhu_csse.get_covid(session)

        vprint("Merging JHU CSSE dataset...")
        for id, df in jhu_covid.groupby(level="ID", sort=False):
            region = region_by_id.get(id)
            if not region:
                continue  # Pruned out of the skeleton

            if df.empty:
                warnings.warn(f"No COVID data: {region.path()}")
                continue

            cases, deaths = df.Confirmed.iloc[-1], df.Deaths.iloc[-1]
            pop = region.totals["population"]
            if not (0 <= cases <= pop):
                warnings.warn(f"Bad cases: {region.path()} ({cases}/{pop}p)")
                continue
            if not (0 <= deaths <= pop):
                warnings.warn(f"Bad deaths: {region.path()} ({deaths}/{pop}p)")
                continue

            df.reset_index(level="ID", drop=True, inplace=True)
            region.totals["positives"] = cases
            region.totals["deaths"] = deaths

            region.covid_metrics["daily positives / 100Kp"] = _trend_metric(
                c="tab:blue",
                em=1,
                ord=1.0,
                cred=jhu_credits,
                cum=df.Confirmed * 1e5 / pop,
            )

            region.covid_metrics["daily deaths / 10Mp"] = _trend_metric(
                c="tab:red",
                em=1,
                ord=1.1,
                cred=jhu_credits,
                cum=df.Deaths * 1e7 / pop,
            )

            region.vaccine_metrics["confirmed cases / 100p"] = _trend_metric(
                c="tab:blue",
                em=0,
                ord=1.6,
                cred=jhu_credits,
                v=df.Confirmed * 100 / pop,
            )

    #
    # Add variant breakdown
    #

    if not args.no_covariants:
        vprint("Loading and merging CoVariants data...")
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

            region = region_by_iso.get(countries[0].alpha_2)
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

                if v in region.variant_metrics:
                    warnings.warn(f"Duplicate covariant ({region.path()}): {v}")
                    continue

                if len(v_totals) != len(vd):
                    warnings.warn(
                        f"Bad covariant data ({region.path()}): "
                        f"len totals={len(v_totals)} len data={len(vd)}"
                    )
                    continue

                v_others = v_others - vd.found
                region.variant_metrics[v] = _trend_metric(
                    c=colors[v],
                    em=1,
                    ord=0,
                    cred=cov_credits,
                    v=vd.found * 100.0 / v_totals,
                )

            other_variants = _trend_metric(
                c=(0.9, 0.9, 0.9),
                em=1,
                ord=0,
                cred=cov_credits,
                v=v_others * 100.0 / v_totals,
            )

            region.variant_metrics = {
                "original/other": other_variants,
                **region.variant_metrics,
            }

    #
    # Add vaccination statistics
    #

    if not args.no_cdc_vaccinations:
        vprint("Loading and merging CDC vaccination data...")
        vax_credits = fetch_cdc_vaccinations.get_credits()
        vax_data = fetch_cdc_vaccinations.get_vaccinations(session=session)
        for fips, v in vax_data.groupby("FIPS", as_index=False, sort=False):
            v.reset_index(level="FIPS", drop=True, inplace=True)
            region = region_by_fips.get(fips)
            if region is None:
                warnings.warn(f"Missing CDC vax FIPS: {fips}")
                continue

            pop = region.totals.get("population", 0)
            if not (pop > 0):
                warn(f"No population: {region.path()} (pop={pop})")
                continue

            region.vaccine_metrics.update(
                {
                    "people given any doses / 100p": _trend_metric(
                        c="tab:olive",
                        em=0,
                        ord=1.2,
                        cred=vax_credits,
                        v=v.Administered_Dose1_Recip * (100 / pop),
                    ),
                    "people fully vaccinated / 100p": _trend_metric(
                        c="tab:green",
                        em=1,
                        ord=1.3,
                        cred=vax_credits,
                        v=v.Series_Complete_Yes * (100 / pop),
                    ),
                    "booster doses given / 100p": _trend_metric(
                        c="tab:purple",
                        em=1,
                        ord=1.4,
                        cred=vax_credits,
                        v=v.Booster_Doses * (100 / pop),
                    ),
                }
            )

    if not args.no_ourworld_vaccinations:
        vprint("Loading and merging ourworldindata vaccination data...")
        vax_credits = fetch_ourworld_vaccinations.credits()
        vax_data = fetch_ourworld_vaccinations.get_vaccinations(session=session)
        vcols = ["iso_code", "state"]
        vax_data.state.fillna("", inplace=True)  # Or groupby() drops them.
        vax_data.sort_values(by=vcols + ["date"], inplace=True)
        vax_data.set_index(keys="date", inplace=True)
        for (iso, s), v in vax_data.groupby(vcols, as_index=False, sort=False):
            if iso.startswith("OWID"):
                continue  # Special ourworldindata regions, not real countries

            country = pycountry.countries.get(alpha_3=iso)
            if country is None:
                warnings.warn(f"Unknown OWID country code: {iso}")
                continue

            region = region_by_iso.get(country.alpha_2)
            if region is None:
                warnings.warn(f"Missing OWID country: {country.alpha_2}")
                continue

            if s:
                # Data includes "New York State", lookup() needs "New York"
                state = us.states.lookup(s.replace(" State", ""))
                if not state:
                    warnings.warn(f"Unknown OWID state: {s}")
                    continue

                region = region_by_fips.get(int(state.fips))
                if region is None:
                    warnings.warn(
                        "Missing OWID FIPS: {state.fips} (state.name)"
                    )
                    continue

            pop = region.totals.get("population", 0)
            if not (pop > 0):
                warn(f"No population: {region.path()} (pop={pop})")
                continue

            v.total_distributed.fillna(method="ffill", inplace=True)
            v.total_vaccinations.fillna(method="ffill", inplace=True)
            v.total_boosters.fillna(method="ffill", inplace=True)
            v.people_vaccinated.fillna(method="ffill", inplace=True)
            v.people_fully_vaccinated.fillna(method="ffill", inplace=True)

            region.vaccine_metrics.update(
                {
                    "people given any doses / 100p": _trend_metric(
                        c="tab:olive",
                        em=0,
                        ord=1.2,
                        cred=vax_credits,
                        v=v.people_vaccinated * (100 / pop),
                    ),
                    "people fully vaccinated / 100p": _trend_metric(
                        c="tab:green",
                        em=1,
                        ord=1.3,
                        cred=vax_credits,
                        v=v.people_fully_vaccinated * (100 / pop),
                    ),
                    "booster doses given / 100p": _trend_metric(
                        c="tab:purple",
                        em=1,
                        ord=1.4,
                        cred=vax_credits,
                        v=v.total_boosters * (100 / pop),
                    ),
                    "daily dose rate / 5Kp": _trend_metric(
                        c="tab:cyan",
                        em=0,
                        ord=1.5,
                        cred=vax_credits,
                        v=v.daily_vaccinations * (5000 / pop),
                        raw=v.daily_vaccinations_raw * (5000 / pop),
                    ),
                }
            )

    #
    # Add prevalence estimates
    #

    if not args.no_cdc_prevalence:
        vprint("Loading and merging CDC prevalence estimates...")
        cdc_credits = fetch_cdc_prevalence.get_credits()
        cdc_data = fetch_cdc_prevalence.get_prevalence(session)

        rcols = ["Region Abbreviation", "Region"]
        for (abbr, name), v in cdc_data.groupby(rcols, as_index=False):
            if abbr.lower() == "all":
                region, name = region_by_iso.get("US"), ""
                if region is None:
                    warnings.warn(f"Missing US for CDC")
                    continue
            else:
                s = us.states.lookup(abbr.split("-")[0])
                if not s:
                    warnings.warn(f"Unknown CDC sero state: {abbr} ({name})")
                    continue

                region = region_by_fips.get(int(s.fips))
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
            region_i = len(region.serology_metrics) // 2
            region.serology_metrics.update(
                {
                    f"infected or vax{name}": _trend_metric(
                        c=matplotlib.cm.tab20b.colors[region_i],
                        em=1,
                        ord=1.0,
                        cred=cdc_credits,
                        v=v["Rate %[Total Prevalence] Combined"],
                    ),
                    f"infected{name}": _trend_metric(
                        c=matplotlib.cm.tab20c.colors[region_i + 4],
                        em=0,
                        ord=1.1,
                        cred=cdc_credits,
                        v=v["Rate %[Total Prevalence] Infection"],
                    ),
                }
            )

    #
    # Add policy changes for US states from the state policy database.
    #

    if not args.no_state_policy:
        vprint("Loading and merging state policy database...")
        policy_credits = fetch_state_policy.credits()
        state_policy = fetch_state_policy.get_events(session=session)
        for f, events in state_policy.groupby(level="state_fips", sort=False):
            region = region_by_fips.get(f)
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
        vprint("Loading and merging California blueprint data chart...")
        cal_credits = fetch_california_blueprint.credits()
        cal_counties = fetch_california_blueprint.get_counties(session=session)
        for county in cal_counties.values():
            region = region_by_fips.get(county.fips)
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

    def sort_policy_changes(r):
        def sort_key(p):
            return (p.date.date(), -abs(p.score), p.score)

        r.policy_changes.sort(key=sort_key)
        for sub in r.subregions.values():
            sort_policy_changes(sub)

    sort_policy_changes(world)

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

        vprint("Loading Google mobility data...")
        mobility_credits = fetch_google_mobility.credits()
        mobility_data = fetch_google_mobility.get_mobility(session=session)
        vprint("Merging Google mobility data...")
        mobility_data.sort_values(by=gcols + ["date"], inplace=True)
        mobility_data.set_index(keys="date", inplace=True)
        for g, m in mobility_data.groupby(gcols, as_index=False, sort=False):
            if g[5]:
                region = region_by_fips.get(g[5])
            else:
                region = region_by_iso.get(g[0])
                for n in g[1:4]:
                    if region and n:
                        region = region.subregions.get(n)

            if region is None:
                continue

            pcfb = "percent_change_from_baseline"  # common, long suffix
            mobility_metrics = {
                "residential": _trend_metric(
                    c="tab:brown",
                    em=1,
                    ord=1.0,
                    cred=mobility_credits,
                    raw=100 + m[f"residential_{pcfb}"],
                ),
                "retail / recreation": _trend_metric(
                    c="tab:orange",
                    em=1,
                    ord=1.1,
                    cred=mobility_credits,
                    raw=100 + m[f"retail_and_recreation_{pcfb}"],
                ),
                "workplaces": _trend_metric(
                    c="tab:red",
                    em=1,
                    ord=1.2,
                    cred=mobility_credits,
                    raw=100 + m[f"workplaces_{pcfb}"],
                ),
                "grocery / pharmacy": _trend_metric(
                    c="tab:blue",
                    em=0,
                    ord=1.4,
                    cred=mobility_credits,
                    raw=100 + m[f"grocery_and_pharmacy_{pcfb}"],
                ),
                "transit stations": _trend_metric(
                    "tab:purple",
                    em=0,
                    ord=1.5,
                    cred=mobility_credits,
                    raw=100 + m[f"transit_stations_{pcfb}"],
                ),
            }

            # Raw daily mobility metrics are confusing, don't show them.
            for m in mobility_metrics.values():
                m.frame.drop(columns=["raw"], inplace=True)

            region.mobility_metrics.update(mobility_metrics)

    #
    # Combine metrics from subregions when not defined at the higher level.
    #

    def roll_up_metrics(r):
        category_popvals, sub_pop_total = {}, 0
        for key, sub in list(r.subregions.items()):
            roll_up_metrics(sub)
            if not sub.covid_metrics:
                warnings.warn(f"No COVID metrics: {sub.path()}")
                del r.subregions[key]
                continue

            sub_pop = sub.totals["population"]
            sub_pop_total += sub_pop
            for category in (
                "totals",
                "covid_metrics",
                "vaccine_metrics",
                "mobility_metrics",
                # Don't roll up variant or serology metrics
            ):
                if category == "mobility_metrics" and not r.parent:
                    continue  # Mobility gets weird rolled up to the top level
                for name, value in getattr(sub, category).items():
                    fn, pv = (category, name), (sub_pop, value)
                    category_popvals.setdefault(fn, []).append(pv)

        pop = r.totals["population"]
        if pop == 0:
            pop = r.totals["population"] = sub_pop_total
        if sub_pop_total > pop * 1.1:
            warn(
                f"Overpopulation: {r.path()} has {pop}p, "
                f"{sub_pop_total}p in parts"
            )
        if sub_pop_total > 0 and sub_pop_total < pop * 0.9:
            warn(
                f"Underpopulation: {r.path()} has {pop}p, "
                f"{sub_pop_total}p in parts"
            )

        week = pandas.Timedelta(weeks=1)
        for (category, name), popvals in category_popvals.items():
            metric_pop = sum(p for p, v in popvals)
            if abs(metric_pop - pop) > pop * 0.1:
                continue  # Don't synthesize if population doesn't match.

            out_dict = getattr(r, category)
            old_value = out_dict.get(name)
            if out_dict is r.totals:
                sub_total = sum(v for p, v in popvals)
                out_dict[name] = max(old_value or 0, sub_total)
                continue

            ends = list(sorted(v.frame.index[-1] for p, v in popvals))
            end = ends[len(ends) // 2]  # Use the median end date.
            if old_value and old_value.frame.index[-1] > end - week:
                continue  # Higher level has reasonably fresh data already

            popvals.sort(reverse=True, key=lambda pv: pv[0])  # Highest first.
            first_pop, first_val = popvals[0]  # Most populated entry.
            frame = first_pop * first_val.frame.loc[:end]
            for next_pop, next_val in popvals[1:]:
                next_frame = next_pop * next_val.frame.loc[:end]
                frame = frame.add(next_frame, fill_value=0)

            out_dict[name] = replace(
                first_val,
                frame=frame / metric_pop,
                credits=dict(c for p, v in popvals for c in v.credits.items()),
            )

        # Clean up some categories if we didn't end up with important data
        for cat in [r.vaccine_metrics, r.serology_metrics, r.mobility_metrics]:
            if not any(m.emphasis > 0 for m in cat.values()):
                cat.clear()

    vprint("Rolling up metrics...")
    roll_up_metrics(world)

    #
    # Interpolate synchronized weekly map metrics from time series metrics.
    #

    # Sync map metric weekly data points to this end date.
    latest = max(m.frame.index[-1] for m in world.covid_metrics.values())

    def add_map_metric(region, c_name, m_name, mul, col, i_col, d_col):
        m = region.covid_metrics.get(c_name)
        if m is not None:
            first = m.frame.index[0].astimezone(latest.tz)
            weeks = (latest - first) // pandas.Timedelta(days=7)
            dates = pandas.date_range(end=latest, periods=weeks, freq="7D")
            value = mul * numpy.interp(dates, m.frame.index, m.frame.value)
            if (~numpy.isnan(value)).any():
                region.map_metrics[m_name] = replace(
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
            "daily positives / 100Kp",
            "positives x2K",
            mul,
            "#0000FF50",
            "#0000FFA0",
            "#00FF00A0",
        )
        add_map_metric(
            region,
            "daily deaths / 10Mp",
            "deaths x200K",
            mul,
            "#FF000050",
            "#FF0000A0",
            None,
        )

    make_map_metrics(world)
    return world


def _make_skeleton(session):
    """Returns a region tree for the world with no metrics populated."""

    def subregion(parent, key, name=None):
        key = str(key)
        region = parent.subregions.get(key)
        if not region:
            region = parent.subregions[key] = Region(
                name=name or key, short_name=key, parent=parent
            )
        return region

    world = Region(name="World", short_name="World")
    for p in fetch_jhu_csse.get_places(session).itertuples(name="Place"):
        if not (p.Population > 0):
            continue  # Analysis requires population data.

        try:
            # Put territories under the parent, even with their own ISO codes
            country_key = pycountry.countries.lookup(p.Country_Region).alpha_2
        except LookupError:
            country_key = p.iso2

        region = subregion(world, country_key, p.Country_Region)
        region.iso_code = country_key

        if p.Province_State:
            region = subregion(region, p.Province_State)
            if p.iso2 != country_key:
                region.iso_code = p.iso2  # Must be for a territory

        if p.FIPS in (36005, 36047, 36061, 36081, 36085):
            region = subregion(region, "NYC", "New York City")
        elif p.FIPS in (49003, 49005, 49033):
            region = subregion(region, "Bear River", "Bear River Area")
        elif p.FIPS in (49023, 49027, 49039, 49041, 49031, 49055):
            region = subregion(region, "Central Utah", "Central Utah Area")
        elif p.FIPS in (49007, 49015, 49019):
            region = subregion(region, "Southeast Utah", "Southeast Utah Area")
        elif p.FIPS in (49001, 49017, 49021, 49025, 49053):
            region = subregion(region, "Southwest Utah", "Southwest Utah Area")
        elif p.FIPS in (49009, 49013, 49047):
            region = subregion(region, "TriCounty", "TriCounty Area")
        elif p.FIPS in (49057, 49029):
            region = subregion(region, "Weber-Morgan", "Weber-Morgan Area")

        if p.Admin2:
            region = subregion(region, p.Admin2)

        if p.FIPS:
            region.fips_code = int(p.FIPS)

        region.place_id = p.Index
        region.totals["population"] = p.Population
        if p.Lat or p.Long_:
            region.lat_lon = (p.Lat, p.Long_)

    return world


def _trend_metric(
    c, em, ord, cred, v=None, raw=None, cum=None, mins=None, maxs=None
):
    """Returns a Metric with data massaged appropriately."""

    assert (v is not None) or (raw is not None) or (cum is not None)

    if cum is not None:
        raw = cum - cum.shift()  # Assume daily data.

    if (v is not None) and (raw is not None):
        assert v.index is raw.index
        df = pandas.DataFrame({"raw": raw, "value": v})
    elif v is not None:
        df = pandas.DataFrame({"value": v})
    elif raw is not None:
        (nonzero_is,) = (raw.values > 0).nonzero()  # Skip first nonzero.
        first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(raw)
        first_i = max(0, min(first_i, len(raw) - 14))
        smooth = raw.iloc[first_i:].clip(lower=0.0).rolling(7).mean()
        df = pandas.DataFrame({"raw": raw, "value": smooth})
    else:
        raise ValueError(f"No data for metric")

    if not pandas.api.types.is_datetime64_any_dtype(df.index.dtype):
        raise ValueError(f'Bad trend index dtype "{df.index.dtype}"')
    if df.index.duplicated().any():
        dups = df.index.duplicated(keep=False)
        raise ValueError(f"Dup trend dates: {df.index[dups]}")

    # Assume that mins/maxs are raw and need smoothing (true so far).
    if mins is not None:
        df["min"] = mins.loc[df.value.first_valid_index() :].rolling(7).mean()
    if maxs is not None:
        df["max"] = maxs.loc[df.value.first_valid_index() :].rolling(7).mean()

    return Metric(frame=df, color=c, emphasis=em, order=ord, credits=cred)


if __name__ == "__main__":
    import argparse
    import signal

    from covid import combine_data

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior.
    parser = argparse.ArgumentParser(
        parents=[cache_policy.argument_parser, argument_parser]
    )
    parser.add_argument("--print_credits", action="store_true")
    parser.add_argument("--print_data", action="store_true")
    parser.add_argument("--print_regex")

    args = parser.parse_args()
    session = cache_policy.new_session(args)
    world = combine_data.get_world(session=session, args=args, verbose=True)
    print_regex = args.print_regex and re.compile(args.print_regex, re.I)

    def print_tree(prefix, parents, key, r):
        if (not print_regex) or r.matches_regex(print_regex):
            line = (
                f'{prefix}{r.totals["population"] or -1:9.0f}p <'
                + ".h"[any("hosp" in k for k in r.covid_metrics.keys())]
                + ".m"[bool(r.map_metrics)]
                + ".c"[bool(r.covid_metrics)]
                + ".v"[bool(r.vaccine_metrics)]
                + ".s"[bool(r.serology_metrics)]
                + ".g"[bool(r.mobility_metrics)]
                + ".p"[bool(r.policy_changes)]
                + ">"
            )
            if key != r.short_name:
                line = f"{line} [{key}]"
            line = f"{line} {parents}{r.short_name}"
            if r.name not in (key, r.short_name):
                line = f"{line} ({r.name})"
            print(line)
            print(
                f"{prefix}    "
                + " ".join(f"{k}={v:.0f}" for k, v in sorted(r.totals.items()))
            )
            for cat, metrics in (
                ("map", r.map_metrics),
                ("cov", r.covid_metrics),
                ("var", r.variant_metrics),
                ("vax", r.vaccine_metrics),
                ("ser", r.serology_metrics),
                ("mob", r.mobility_metrics),
            ):
                for name, m in metrics.items():
                    print(
                        f"{prefix}    {len(m.frame):3d}d "
                        f"=>{m.frame.index.max().date()} "
                        f"last={m.frame.value.iloc[-1]:<5.1f} "
                        f"{cat}: {name}"
                    )
                    if args.print_credits:
                        print(f'{prefix}        {" ".join(m.credits.values())}')
                    if args.print_data:
                        print(m.frame)

            for c in r.policy_changes:
                print(
                    f"{prefix}           {c.date.date()} {c.score:+2d} "
                    f"{c.emoji} {c.text}"
                )

        for k, sub in r.subregions.items():
            print_tree(prefix + "  ", f"{parents}{r.short_name}/", k, sub)

    print_tree("", "", world.short_name, world)
