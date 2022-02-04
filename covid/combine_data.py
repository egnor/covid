"""Functions that combine data sources into a combined representation."""

import argparse
import itertools
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
from covid import fetch_cdc_vaccinations
from covid import fetch_covariants
from covid import fetch_google_mobility
from covid import fetch_ourworld_vaccinations
from covid import fetch_state_policy
from covid import merge_covid_metrics
from covid import merge_hospital_metrics
from covid.logging_policy import collecting_warnings
from covid.region_data import Metric
from covid.region_data import PolicyChange

# Reusable command line arguments for data collection.
argument_parser = argparse.ArgumentParser(add_help=False)
arg_group = argument_parser.add_argument_group("data gathering")
arg_group.add_argument("--no_california_blueprint", action="store_true")
arg_group.add_argument("--no_cdc_prevalence", action="store_true")
arg_group.add_argument("--no_cdc_vaccinations", action="store_true")
arg_group.add_argument("--no_covariants", action="store_true")
arg_group.add_argument("--no_covid_metrics", action="store_true")
arg_group.add_argument("--no_hospital_metrics", action="store_true")
arg_group.add_argument("--no_google_mobility", action="store_true")
arg_group.add_argument("--no_ourworld_vaccinations", action="store_true")
arg_group.add_argument("--no_state_policy", action="store_true")


KNOWN_WARNINGS_REGEX = re.compile(
    r"|Bad deaths: World/AU .*"
    r"|Cannot parse header or footer so it will be ignored"  # xlsx parser
    r"|Duplicate covariant \(World/RS\): .*"
    r"|Missing CDC vax FIPS: (66|78).*"
    r"|Missing OWID vax country: (GG|JE|NU|NR|PN|TK|TM|TV)"
    r"|No COVID metrics: World/(EH|NG|PL).*"
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


def get_world(session, args, verbose=False):
    """Returns data organized into a tree rooted at a World region.
    Warnings are captured and printed, then raise a ValueError exception."""

    vprint = lambda *a, **k: print(*a, **k) if verbose else None
    cache_path = cache_policy.cached_path(session, _world_cache_key(args))
    if cache_path.exists():
        vprint(f"Loading cached world: {cache_path}")
        with cache_path.open(mode="rb") as cache_file:
            return pickle.load(cache_file)

    with collecting_warnings(allow_regex=KNOWN_WARNINGS_REGEX) as warnings:
        world = _compute_world(session, args, vprint)
        if warnings:
            raise ValueError(f"{len(warnings)} warnings found combining data")

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
    atlas = build_atlas.get_atlas(session)

    if not args.no_covid_metrics:
        merge_covid_metrics.add_metrics(session=session, atlas=atlas)

    if not args.no_hospital_metrics:
        merge_hospital_metrics.add_metrics(session=session, atlas=atlas)

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
        vprint("Loading CDC vaccination data...")
        vax_credits = fetch_cdc_vaccinations.credits()
        vax_data = fetch_cdc_vaccinations.get_vaccinations(session=session)

        vprint("Merging CDC vaccination data...")
        for fips, v in vax_data.groupby("FIPS", as_index=False, sort=False):
            v.reset_index(level="FIPS", drop=True, inplace=True)
            region = atlas.by_fips.get(fips)
            if region is None:
                warnings.warn(f"Missing CDC vax FIPS: {fips}")
                continue

            pop = region.totals.get("population", 0)
            if not (pop > 0):
                warnings.warn(f"No population: {region.path()} (pop={pop})")
                continue

            vax_metrics = region.vaccine_metrics

            vax_metrics["people given any doses / 100p"] = _trend_metric(
                c="tab:olive",
                em=0,
                ord=1.2,
                cred=vax_credits,
                v=v.Administered_Dose1_Recip * (100 / pop),
            )

            vax_metrics["people fully vaccinated / 100p"] = _trend_metric(
                c="tab:green",
                em=1,
                ord=1.3,
                cred=vax_credits,
                v=v.Series_Complete_Yes * (100 / pop),
            )

            vax_metrics["booster doses given / 100p"]: _trend_metric(
                c="tab:purple",
                em=1,
                ord=1.4,
                cred=vax_credits,
                v=v.Booster_Doses * (100 / pop),
            )

    if not args.no_ourworld_vaccinations:
        vprint("Loading and merging ourworldindata vaccination data...")
        vax_credits = fetch_ourworld_vaccinations.credits()
        vax_data = fetch_ourworld_vaccinations.get_vaccinations(session=session)
        vcols = ["iso_code", "state"]
        vax_data.state.fillna("", inplace=True)  # Or groupby() drops them.
        vax_data.sort_values(by=vcols + ["date"], inplace=True)
        vax_data.set_index(keys="date", inplace=True)
        for (iso3, admin2), v in vax_data.groupby(vcols, as_index=False):
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
                    warnings.warn(f"Unknown OWID vax country code: {iso3}")
                    continue

            region = atlas.by_iso2.get(cc.alpha_2) if cc else atlas.world
            if region is None:
                warnings.warn(f"Missing OWID vax country: {cc.alpha_2}")
                continue

            if admin2:
                if cc.alpha_2 == "US":
                    # Data includes "New York State", lookup() needs "New York"
                    st = us.states.lookup(admin2.replace(" State", ""))
                    if not st:
                        warnings.warn(f"Unknown OWID vax state: {admin2}")
                        continue

                    region = atlas.by_fips.get(int(st.fips))
                    if region is None:
                        warnings.warn(f"Missing OWID vax FIPS: {st.fips}")
                        continue
                else:
                    region = region.subregions.get(admin2)
                    if region is None:
                        warnings.warn(f"Unknown OWID vax subregion: {admin2}")

            pop = region.totals.get("population", 0)
            if not (pop > 0):
                warnings.warn(f"No population: {region.path()} (pop={pop})")
                continue

            v.total_distributed.fillna(method="ffill", inplace=True)
            v.total_vaccinations.fillna(method="ffill", inplace=True)
            v.total_boosters.fillna(method="ffill", inplace=True)
            v.people_vaccinated.fillna(method="ffill", inplace=True)
            v.people_fully_vaccinated.fillna(method="ffill", inplace=True)

            vax_metrics = region.vaccine_metrics
            vax_metrics["people given any doses / 100p"] = _trend_metric(
                c="tab:olive",
                em=0,
                ord=1.2,
                cred=vax_credits,
                v=v.people_vaccinated * (100 / pop),
            )

            vax_metrics["people fully vaccinated / 100p"] = _trend_metric(
                c="tab:green",
                em=1,
                ord=1.3,
                cred=vax_credits,
                v=v.people_fully_vaccinated * (100 / pop),
            )

            vax_metrics["booster doses given / 100p"] = _trend_metric(
                c="tab:purple",
                em=1,
                ord=1.4,
                cred=vax_credits,
                v=v.total_boosters * (100 / pop),
            )

            vax_metrics["daily dose rate / 5Kp"] = _trend_metric(
                c="tab:cyan",
                em=0,
                ord=1.5,
                cred=vax_credits,
                v=v.daily_vaccinations * (5000 / pop),
                raw=v.daily_vaccinations_raw * (5000 / pop),
            )

    #
    # Add prevalence estimates
    #

    if not args.no_cdc_prevalence:
        vprint("Loading and merging CDC prevalence estimates...")
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
            region_i = len(region.serology_metrics) // 2
            region.serology_metrics[f"infected or vax{name}"] = _trend_metric(
                c=matplotlib.cm.tab20b.colors[region_i],
                em=1,
                ord=1.0,
                cred=cdc_credits,
                v=v["Rate %[Total Prevalence] Combined"],
            )

            region.serology_metrics[f"infected{name}"] = _trend_metric(
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
        vprint("Loading and merging state policy database...")
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
        vprint("Loading and merging California blueprint data chart...")
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

        vprint("Loading Google mobility data...")
        mobility_credits = fetch_google_mobility.credits()
        mobility_data = fetch_google_mobility.get_mobility(session=session)
        vprint("Merging Google mobility data...")
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
            warnings.warn(
                f"Overpopulation: {r.path()} has {pop}p, "
                f"{sub_pop_total}p in parts"
            )
        if sub_pop_total > 0 and sub_pop_total < pop * 0.9:
            warnings.warn(
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

        # Remove metrics which have no (or very little) valid data
        for cat in [
            r.map_metrics,
            r.covid_metrics,
            r.variant_metrics,
            r.vaccine_metrics,
            r.serology_metrics,
            r.mobility_metrics,
        ]:
            for name, m in list(cat.items()):
                if m.frame.value.count() < 2:
                    del cat[name]

        # Clean up some categories if we didn't get any "headline" data
        for cat in [r.vaccine_metrics, r.serology_metrics, r.mobility_metrics]:
            if not any(m.emphasis > 0 for m in cat.values()):
                cat.clear()

    vprint("Rolling up metrics...")
    roll_up_metrics(atlas.world)

    #
    # Interpolate synchronized weekly map metrics from time series metrics.
    #

    # Sync map metric weekly data points to this end date.
    latest = max(m.frame.index[-1] for m in atlas.world.covid_metrics.values())

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

    make_map_metrics(atlas.world)
    return atlas.world


def _trend_metric(c, em, ord, cred, v=None, raw=None, cum=None):
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

    return Metric(frame=df, color=c, emphasis=em, order=ord, credits=cred)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        parents=[cache_policy.argument_parser, argument_parser]
    )
    parser.add_argument("--print_credits", action="store_true")
    parser.add_argument("--print_data", action="store_true")

    args = parser.parse_args()
    session = cache_policy.new_session(args)
    world = get_world(session=session, args=args, verbose=True)
    print(
        world.debug_tree(
            with_credits=args.print_credits, with_data=args.print_data
        )
    )
