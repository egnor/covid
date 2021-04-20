"""Functions that combine data sources into a combined representation."""

import argparse
import collections
import os.path
import pickle
import re
import sys
import traceback
import warnings
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Tuple
from warnings import warn

import numpy
import pandas
import pandas.api.types
import pycountry
import us

from covid import cache_policy
from covid import fetch_california_blueprint
from covid import fetch_cdc_mortality
from covid import fetch_google_mobility
from covid import fetch_ourworld_vaccinations
from covid import fetch_state_policy
from covid import fetch_unified_dataset


# Reusable command line arguments for data collection.
argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group('data gathering')
argument_group.add_argument('--no_california_blueprint', action='store_true')
argument_group.add_argument('--no_cdc_mortality', action='store_true')
argument_group.add_argument('--no_google_mobility', action='store_true')
argument_group.add_argument('--no_ourworld_vaccinations', action='store_true')
argument_group.add_argument('--no_state_policy', action='store_true')
argument_group.add_argument('--no_unified_dataset', action='store_true')
argument_group.add_argument('--no_unified_hydromet', action='store_true')


KNOWN_WARNINGS_REGEX = re.compile(
    r'No COVID metrics: World/EH.*'
    r'|Underpopulation: World/DK .*'
    r'|Underpopulation: World/FR .*'
    r'|Underpopulation: World/PE/CAL .*'
    r'|Overpopulation: World/CL(/..)? .*'
    r'|Overpopulation: World/CO(/...)? .*'
    r'|Overpopulation: World/MX(/...)? .*'
    r'|Overpopulation: World/PE(/...)? .*'
    r'|Bad ourworldindata state: Bureau of Prisons'
    r'|Bad ourworldindata state: Dept of Defense'
    r'|Bad ourworldindata state: Federated States of Micronesia'
    r'|Bad ourworldindata state: Indian Health Svc'
    r'|Bad ourworldindata state: Long Term Care'
    r'|Bad ourworldindata state: Marshall Islands'
    r'|Bad ourworldindata state: Republic of Palau'
    r'|Bad ourworldindata state: United States'
    r'|Bad ourworldindata state: Veterans Health'
)


@dataclass(frozen=True)
class Metric:
    frame: pandas.DataFrame
    color: str
    emphasis: int = 0
    order: float = 0
    increase_color: Optional[str] = None
    decrease_color: Optional[str] = None
    peak: Optional[Tuple[pandas.Timestamp, float]] = None
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
    unified_id: Optional[str] = None
    lat_lon: Optional[Tuple[float, float]] = None
    totals: collections.Counter = field(default_factory=collections.Counter)
    parent: Optional['Region'] = field(default=None, repr=0)
    subregions: Dict[str, 'Region'] = field(default_factory=dict, repr=0)
    map_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    covid_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    vaccination_metrics: Dict[str, Metric] = field(
        default_factory=dict, repr=0)
    mobility_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    policy_changes: List[PolicyChange] = field(default_factory=list, repr=0)
    current_policy: Optional[PolicyChange] = None

    def path(r):
        return f'{r.parent.path()}/{r.short_name}' if r.parent else r.name

    def matches_regex(r, rx):
        rx = rx if isinstance(rx, re.Pattern) else (
            rx and re.compile(rx, re.I))
        return bool(not rx or rx.search(r.name) or rx.search(r.path()) or
                    rx.search(r.path().replace(' ', '_')))

    def lookup_path(r, p):
        p = p.strip('/ ').lower()
        n = (p[len(r.short_name):] if p.startswith(r.short_name.lower()) else
             p[len(r.name):] if p.startswith(r.name.lower()) else None)
        return r if (n == '') else None if (n is None) else next(
            (y for s in r.subregions.values()
             for y in [s.lookup_path(n)] if y), None)


def get_world(session, args, verbose=False):
    """Returns data organized into a tree rooted at a World region.
    Warnings are captured and printed, then raise a ValueError exception."""

    vprint = lambda *a, **k: print(*a, **k) if verbose else None
    cache_path = cache_policy.cached_path(session, _world_cache_key(args))
    if cache_path.exists():
        vprint(f'Loading cached world: {cache_path}')
        with cache_path.open(mode='rb') as cache_file:
            return pickle.load(cache_file)

    warning_count = 0

    def show_and_count(message, category, filename, lineno, file, line):
        # Allow known data glitches.
        if KNOWN_WARNINGS_REGEX.match(str(message)):
            print(f'=== {str(message).strip()}')
        else:
            nonlocal warning_count
            warning_count += 1
            where = f'{os.path.basename(filename)}:{lineno}'
            print(f'*** #{warning_count} ({where}) {str(message).strip()}')
            traceback.print_stack(file=sys.stdout)
            print()

    try:
        warnings.showwarning, saved = show_and_count, warnings.showwarning
        world = _compute_world(session, args, vprint)
    finally:
        warnings.showwarning = saved

    if warning_count:
        print()
        raise ValueError(f'{warning_count} warnings found combining data')

    vprint(f'Saving cached world: {cache_path}')
    with cache_policy.temp_to_rename(cache_path, mode='wb') as cache_file:
        pickle.dump(world, cache_file)
    return world


def _world_cache_key(args):
    # Only include args understood by this module.
    ks = list(sorted(vars(argument_parser.parse_args([])).keys()))
    return ('https://plague.wtf/world' +
            ''.join(f':{k}={getattr(args, k)}' for k in ks))


def _trend_metric(c, em, ord, cred, v, raw=None, mins=None, maxs=None):
    if not pandas.api.types.is_datetime64_any_dtype(v.index.dtype):
        raise ValueError(f'Bad trend index dtype "{v.index.dtype}"')
    if v.index.duplicated().any():
        dups = v.index.duplicated(keep=False)
        raise ValueError(f'Dup trend dates: {v.index[dups]}')
    nonzero_is, = (v.values > 0).nonzero()  # Skip first nonzero.
    first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(v)
    first_i = max(0, min(first_i, len(v) - 14))

    if raw is not None:
        df = pandas.DataFrame({'raw': raw, 'value': v})
    else:
        smooth = v.iloc[first_i:].rolling(7).mean()
        df = pandas.DataFrame({'raw': v, 'value': smooth})
    if mins is not None:
        assert mins.index is v.index
        df['min'] = mins.iloc[first_i:].rolling(7).mean()
    if maxs is not None:
        assert maxs.index is v.index
        df['max'] = maxs.iloc[first_i:].rolling(7).mean()

    peak_x = df.value.idxmax()
    peak = None if pandas.isna(peak_x) else (peak_x, df.value.loc[peak_x])
    return Metric(
        frame=df, color=c, emphasis=em, order=ord, credits=cred, peak=peak)


def _compute_world(session, args, vprint):
    """Assembles a World region from data, allowing warnings."""

    vprint('Loading place data...')
    world = _unified_skeleton(session)

    # Index by various forms of ID for merging data in.
    region_by_iso = {}
    region_by_fips = {}
    region_by_uid = {}

    def index_region_tree(r):
        for index_dict, key in [
            (region_by_iso, r.iso_code), (region_by_fips, r.fips_code),
                (region_by_uid, r.unified_id)]:
            if key is not None:
                index_dict[key] = r
        for sub in r.subregions.values():
            index_region_tree(sub)

    index_region_tree(world)

    #
    # Add metrics from the Unified COVID-19 Dataset.
    #

    if not args.no_unified_dataset:
        vprint('Loading unified dataset (COVID)...')
        unified_credits = fetch_unified_dataset.credits()
        unified_covid = fetch_unified_dataset.get_covid(session)

        if args.no_unified_hydromet:
            hydromet_by_uid = None
        else:
            vprint('Loading unified dataset (hydromet)...')
            unified_hydromet = fetch_unified_dataset.get_hydromet(session)
            hydromet_by_uid = unified_hydromet.groupby(level='ID', sort=False)

        vprint('Merging unified dataset...')
        for uid, df in unified_covid.groupby(level='ID', sort=False):
            region = region_by_uid.get(uid)
            if not region:
                continue  # Filtered out for one reason or another.

            pop = region.totals['population']
            df.reset_index(level='ID', drop=True, inplace=True)

            def best_data(type):
                for_type = df.xs(type)
                best_date, best_data = None, None
                for source, source_data in df.xs(type).groupby(level='Source'):
                    source_data = source_data.droplevel(level='Source')
                    source_latest = source_data.last_valid_index()
                    if best_date is None or source_latest > best_date:
                        best_date, best_data = source_latest, source_data
                return best_data

            # COVID metrics
            if 'Confirmed' in df.index:
                confirmed = best_data('Confirmed')
                region.totals['positives'] = confirmed.Cases.max()  # glitch
                region.covid_metrics['positives / 100Kp'] = _trend_metric(
                    c='tab:blue', em=1, ord=1.0, cred=unified_credits,
                    v=confirmed.Cases_New * 1e5 / pop)
            if 'Deaths' in df.index:
                deaths = best_data('Deaths')
                region.totals['deaths'] = deaths.Cases.max()  # glitch
                region.covid_metrics['deaths / 10Mp'] = _trend_metric(
                    c='tab:red', em=1, ord=1.1, cred=unified_credits,
                    v=deaths.Cases_New * 1e7 / pop)
            if 'Tests' in df.index:
                region.covid_metrics['tests / 10Kp'] = _trend_metric(
                    c='tab:green', em=0, ord=2.0, cred=unified_credits,
                    v=best_data('Tests').Cases_New * 1e4 / pop)
            if 'Hospitalized' in df.index:
                region.covid_metrics['hosp admit / 1Mp'] = _trend_metric(
                    c='tab:orange', em=0, ord=3.0, cred=unified_credits,
                    v=best_data('Hospitalized').Cases_New * 1e6 / pop)
            if 'Hospitalized_Now' in df.index:
                region.covid_metrics['hosp current / 100Kp'] = _trend_metric(
                    c='tab:pink', em=0, ord=3.1, cred=unified_credits,
                    v=best_data('Hospitalized_Now').Cases * 1e5 / pop)

            # Hydrometeorological data
            def to_f(c): return c * 1.8 + 32
            if hydromet_by_uid is not None and uid in hydromet_by_uid.groups:
                for_uid = hydromet_by_uid.get_group(uid)
                by_source = for_uid.groupby(level='HydrometSource')
                df = by_source.get_group(by_source['T'].count().idxmax())
                df.reset_index(level=('ID', 'HydrometSource'),
                               drop=True, inplace=True)
                region.mobility_metrics['temp °F'] = _trend_metric(
                    c='tab:gray', em=1, ord=2.0, cred=unified_credits,
                    v=to_f(df['T']), mins=to_f(df.Tmin), maxs=to_f(df.Tmax))

    #
    # Add baseline mortality data from CDC figures for US states.
    # TODO: Include seasonal variation and county level data?
    # TODO: Some sort of proper excess mortality plotting?
    #

    if not args.no_cdc_mortality:
        vprint('Loading and merging CDC mortality data...')
        cdc_credits = fetch_cdc_mortality.credits()
        cdc_mortality = fetch_cdc_mortality.get_states(session=session)
        y2020 = pandas.DatetimeIndex(['2020-01-01', '2020-12-31'], tz='UTC')
        for mort in cdc_mortality.itertuples(name='Mortality'):
            region = region_by_fips.get(mort.Index)
            if region is None:
                continue  # Filtered out for one reason or another.

            mort_10M = mort.Deaths / 365 * 1e7 / region.totals['population']
            region.covid_metrics.update({
                f'historical deaths / 10Mp ({mort_10M:.1f})': Metric(
                    color='black', emphasis=-1, order=4.0, credits=cdc_credits,
                    frame=pandas.DataFrame(
                        {'value': [mort_10M] * 2}, index=y2020))})

    #
    # Add policy changes for US states from the state policy database.
    #

    if not args.no_state_policy:
        vprint('Loading and merging state policy database...')
        policy_credits = fetch_state_policy.credits()
        state_policy = fetch_state_policy.get_events(session=session)
        for f, events in state_policy.groupby(level='state_fips', sort=False):
            region = region_by_fips.get(f)
            if region is None:
                continue

            for e in events.itertuples():
                region.policy_changes.append(PolicyChange(
                    date=e.Index[1], score=e.score, emoji=e.emoji,
                    text=e.policy, credits=policy_credits))

    if not args.no_california_blueprint:
        vprint('Loading and merging California blueprint data chart...')
        cal_credits = fetch_california_blueprint.credits()
        cal_counties = fetch_california_blueprint.get_counties(session=session)
        for county in cal_counties.values():
            region = region_by_fips.get(county.fips)
            if region is None:
                warnings.warn(f'FIPS {county.fips} (CA {county.name}) missing')
                continue

            prev = None
            for date, tier in sorted(county.tier_history.items()):
                region.current_policy = PolicyChange(
                    date=date, emoji=tier.emoji,
                    score=(-3 if tier.number <= 2 else +3),
                    text=f'Entered {tier.color} tier ({tier.name})',
                    credits=cal_credits)
                region.policy_changes.append(region.current_policy)
                prev = tier

    def sort_policy_changes(r):
        def sort_key(p): return (p.date.date(), -abs(p.score), p.score)
        r.policy_changes.sort(key=sort_key)
        for sub in r.subregions.values():
            sort_policy_changes(sub)

    sort_policy_changes(world)

    #
    # Add mobility data where it's available.
    #

    if not args.no_google_mobility:
        gcols = [
            'country_region_code', 'sub_region_1', 'sub_region_2',
            'metro_area', 'iso_3166_2_code', 'census_fips_code'
        ]

        vprint('Loading Google mobility data...')
        mobility_credits = fetch_google_mobility.credits()
        mobility_data = fetch_google_mobility.get_mobility(session=session)
        vprint('Merging Google mobility data...')
        mobility_data.sort_values(by=gcols + ['date'], inplace=True)
        mobility_data.set_index(keys='date', inplace=True)
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

            pcfb = 'percent_change_from_baseline'  # common, long suffix
            region.mobility_metrics.update({
                'residential': _trend_metric(
                    c='tab:brown', em=1, ord=1.0, cred=mobility_credits,
                    v=100 + m[f'residential_{pcfb}']),
                'retail / recreation': _trend_metric(
                    c='tab:orange', em=1, ord=1.1, cred=mobility_credits,
                    v=100 + m[f'retail_and_recreation_{pcfb}']),
                'workplaces': _trend_metric(
                    c='tab:red', em=1, ord=1.2, cred=mobility_credits,
                    v=100 + m[f'workplaces_{pcfb}']),
                'grocery / pharmacy': _trend_metric(
                    c='tab:blue', em=0, ord=1.4, cred=mobility_credits,
                    v=100 + m[f'grocery_and_pharmacy_{pcfb}']),
                'transit stations': _trend_metric(
                    'tab:purple', em=0, ord=1.5, cred=mobility_credits,
                    v=100 + m[f'transit_stations_{pcfb}']),
            })

    #
    # Add vaccination statistics.
    #

    if not args.no_ourworld_vaccinations:
        vprint('Loading ourworldindata vaccination data...')
        vax_credits = fetch_ourworld_vaccinations.credits()
        vax_data = fetch_ourworld_vaccinations.get_vaccinations(
            session=session)
        vprint('Merging ourworldindata vaccination data...')
        vcols = ['iso_code', 'state']
        vax_data.state.fillna('', inplace=True)  # Or groupby() drops them.
        vax_data.sort_values(by=vcols + ['date'], inplace=True)
        vax_data.set_index(keys='date', inplace=True)
        for (iso, s), v in vax_data.groupby(vcols, as_index=False, sort=False):
            if iso.startswith('OWID'):
                continue  # Special ourworldindata regions, not real countries

            country = pycountry.countries.get(alpha_3=iso)
            if country is None:
                warnings.warn(f'Bad ourworldindata country code: {iso}')
                continue

            region = region_by_iso.get(country.alpha_2)
            if region is None:
                continue

            if s:
                # Data includes "New York State", lookup() needs "New York"
                state = us.states.lookup(s.replace(' State', ''))
                if not state:
                    warnings.warn(f'Bad ourworldindata state: {s}')
                    continue
                region = region_by_fips.get(int(state.fips))
                if region is None:
                    warnings.warn(f'FIPS missing: {state.fips} (state.name)')
                    continue

            pop = region.totals.get('population', 0)
            if not pop:
                warn(f'No population for ourworldindata: {region.path}')
                continue

            region.vaccination_metrics.update({
                'people given any doses / 100p': _trend_metric(
                    c='tab:orange', em=1, ord=1.0, cred=vax_credits,
                    v=v.people_vaccinated * (100 / pop)),
                'people given all doses / 100p': _trend_metric(
                    c='tab:green', em=1, ord=1.1, cred=vax_credits,
                    v=v.people_fully_vaccinated * (100 / pop)),
                'doses given / 100p': _trend_metric(
                    c='tab:blue', em=0, ord=1.2, cred=vax_credits,
                    v=v.total_vaccinations * (100 / pop)),
                'doses allocated / 100p': _trend_metric(
                    c='tab:gray', em=0, ord=1.3, cred=vax_credits,
                    v=v.total_distributed * (100 / pop)),
                'daily doses given / 5Kp': _trend_metric(
                    c='tab:cyan', em=0, ord=1.3, cred=vax_credits,
                    v=v.daily_vaccinations * (5000 / pop),
                    raw=v.daily_vaccinations_raw * (5000 / pop)),
            })

    #
    # Combine metrics from subregions when not defined at the higher level.
    #

    def roll_up_metrics(r):
        fieldname_popvals, sub_pop_total = {}, 0
        for key, sub in list(r.subregions.items()):
            roll_up_metrics(sub)
            if not sub.covid_metrics:
                warn(f'No COVID metrics: {sub.path()}')
                del r.subregions[key]
                continue

            sub_pop = sub.totals['population']
            sub_pop_total += sub_pop
            for field in (
                'totals', 'covid_metrics', 'vaccination_metrics',
                    'mobility_metrics'):
                if not (field == 'mobility_metrics' and not r.parent):
                    for name, value in getattr(sub, field).items():
                        if name not in getattr(r, field):
                            fn, pv = (field, name), (sub_pop, value)
                            fieldname_popvals.setdefault(fn, []).append(pv)

        pop = r.totals['population']
        if pop == 0:
            pop = r.totals['population'] = sub_pop_total
        if sub_pop_total > pop * 1.1:
            warn(f'Overpopulation: {r.path()} has {pop}p, '
                 f'{sub_pop_total}p in parts')
        if sub_pop_total > 0 and sub_pop_total < pop * 0.9:
            warn(f'Underpopulation: {r.path()} has {pop}p, '
                 f'{sub_pop_total}p in parts')

        for (field, name), popvals in fieldname_popvals.items():
            metric_pop = sum(p for p, v in popvals)
            if abs(metric_pop - pop) > pop * 0.1:
                continue  # Don't synthesize if population doesn't match.

            if field == 'totals':
                r.totals[name] = sum(v for p, v in popvals)
                continue

            popvals.sort(reverse=True, key=lambda pv: pv[0])  # Highest first.
            credits = dict(c for p, v in popvals for c in v.credits.items())
            ends = list(sorted(v.frame.index[-1] for p, v in popvals))
            end = ends[len(ends) // 2]  # Use the median end date.

            first_pop, first_val = popvals[0]  # Most populated entry.
            frame = first_pop * first_val.frame.loc[:end]
            for next_pop, next_val in popvals[1:]:
                next_frame = next_pop * next_val.frame.loc[:end]
                frame = frame.add(next_frame, fill_value=0)
            getattr(r, field)[name] = replace(
                first_val, frame=frame / metric_pop, credits=credits)

    vprint('Rolling up metrics...')
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
            dates = pandas.date_range(end=latest, periods=weeks, freq='7D')
            value = mul * numpy.interp(dates, m.frame.index, m.frame.value)
            if (~numpy.isnan(value)).any():
                region.map_metrics[m_name] = replace(
                    m, frame=pandas.DataFrame({'value': value}, index=dates),
                    color=col, increase_color=i_col, decrease_color=d_col)

    def make_map_metrics(region):
        for sub in region.subregions.values():
            make_map_metrics(sub)

        mul = region.totals['population'] / 50  # 100K => 2K, 10Mp => 200K
        add_map_metric(region, 'positives / 100Kp', 'positives x2K', mul,
                       '#0000FF50', '#0000FFA0', '#00FF00A0')
        add_map_metric(region, 'deaths / 10Mp', 'deaths x200K', mul,
                       '#FF000050', '#FF0000A0', None)

    make_map_metrics(world)
    return world


def _unified_skeleton(session):
    """Returns a region tree for the world with no metrics populated."""

    def subregion(parent, key, name=None, short_name=None):
        key = str(key)
        region = parent.subregions.get(key)
        if not region:
            region = parent.subregions[key] = Region(
                name=name or key, short_name=short_name or key, parent=parent)
        return region

    world = Region(name='World', short_name='World')
    for id, p in fetch_unified_dataset.get_places(session).items():
        if not (p.Population > 0):
            continue  # Analysis requires population data.

        region = subregion(world, p.ISO1_2C, p.Admin0)
        if p.Admin1:
            region = subregion(region, p.Admin1, p.Admin1, p.ISO2)
        else:
            region.iso_code = p.ISO1_2C

        if p.ID[:7] in ('US36666', 'US36005', 'US36047',
                        'US36061', 'US36081', 'US36085'):
            region = subregion(region, 'NYC', 'New York City')
        elif p.ID[:7] in ('US49003', 'US49005', 'US49033'):
            region = subregion(region, 'Bear River', 'Bear River Area')
        elif p.ID[:7] in ('US49023', 'US49027', 'US49039',
                          'US49041', 'US49031', 'US49055'):
            region = subregion(region, 'Central Utah', 'Central Utah Area')
        elif p.ID[:7] in ('US49007', 'US49015', 'US49019'):
            region = subregion(region, 'Southeast Utah', 'Southeast Utah Area')
        elif p.ID[:7] in ('US49001', 'US49017', 'US49021',
                          'US49025', 'US49053'):
            region = subregion(region, 'Southwest Utah', 'Southwest Utah Area')
        elif p.ID[:7] in ('US49009', 'US49013', 'US49047'):
            region = subregion(region, 'TriCounty', 'TriCounty Area')
        elif p.ID[:7] in ('US49057', 'US49029'):
            region = subregion(region, 'Weber-Morgan', 'Weber-Morgan Area')

        if p.Admin2 and p.ID != 'US36666':
            region = subregion(region, p.Admin2)

        if p.ZCTA[:4] == '0000':  # NYC borough pseudo-ZIP
            region.name = region.short_name = p.Admin3  # Use borough name.
        elif p.Admin3:
            region = subregion(region, p.ZCTA or p.Admin3, p.Admin3)

        if p.ZCTA:
            region.zip_code = int(p.ZCTA)
        elif p.FIPS and p.FIPS.isdigit():
            region.fips_code = int(p.FIPS)
        elif not p.Admin2:
            region.iso_code = p.ISO2_UID

        region.unified_id = p.ID
        region.totals['population'] = p.Population
        if p.Latitude or p.Longitude:
            region.lat_lon = (p.Latitude, p.Longitude)

    world.totals['population'] = sum(
        s.totals['population'] for s in world.subregions.values())
    return world


if __name__ == '__main__':
    import argparse
    import signal
    from covid import combine_data

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior.
    parser = argparse.ArgumentParser(
        parents=[cache_policy.argument_parser, argument_parser])
    parser.add_argument('--print_regex')
    parser.add_argument('--print_credits', action='store_true')
    parser.add_argument('--print_data', action='store_true')

    args = parser.parse_args()
    session = cache_policy.new_session(args)
    world = combine_data.get_world(session=session, args=args, verbose=True)
    region_regex = args.print_regex and re.compile(args.print_regex, re.I)

    def print_tree(prefix, parents, key, r):
        if r.matches_regex(region_regex):
            line = (
                f'{prefix}{r.totals["population"] or -1:9.0f}p <' +
                '.h'[any('hosp' in k for k in r.covid_metrics.keys())] +
                '.m'[bool(r.map_metrics)] +
                '.c'[bool(r.covid_metrics)] +
                '.v'[bool(r.vaccination_metrics)] +
                '.g'[bool(r.mobility_metrics)] +
                '.p'[bool(r.policy_changes)] + '>')
            if key != r.short_name:
                line = f'{line} [{key}]'
            line = f'{line} {parents}{r.short_name}'
            if r.name not in (key, r.short_name):
                line = f'{line} ({r.name})'
            print(line)
            print(f'{prefix}    ' + ' '.join(
                f'{k}={v}' for k, v in sorted(r.totals.items())))
            for cat, metrics in (
                    ('map', r.map_metrics),
                    ('cov', r.covid_metrics),
                    ('vax', r.vaccination_metrics),
                    ('mob', r.mobility_metrics)):
                for name, m in metrics.items():
                    max = (' ' * 21 if not m.peak else
                           f' peak={m.peak[1]:<3.0f} @{m.peak[0].date()}')
                    print(f'{prefix}    {len(m.frame):3d}d '
                          f'=>{m.frame.index.max().date()}{max} {cat}: {name}')
                    if args.print_credits:
                        print(f'{prefix}        {" ".join(m.credits.values())}')
                    if args.print_data:
                        print(m.frame)

            for c in r.policy_changes:
                print(f'{prefix}      {c.date.date()} {c.score:+2d} '
                      f'{c.emoji} {c.text}')

        for k, sub in r.subregions.items():
            print_tree(prefix + '  ', f'{parents}{r.short_name}/', k, sub)

    print_tree('', '', world.short_name, world)
