"""Functions that combine data sources into a unified representation."""

import argparse
import collections
import functools
import re
from dataclasses import dataclass, field
from re import compile, Pattern
from typing import Dict, List, Optional, Tuple

import numpy
import pandas
import pycountry
import us

from covid import fetch_california_blueprint
from covid import fetch_cdc_mortality
from covid import fetch_covid_tracking
from covid import fetch_google_mobility
from covid import fetch_jhu_covid19
from covid import fetch_state_policy
from covid import fetch_unified_dataset


# Reusable command line arguments for data collection.
argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group('data gathering')
argument_group.add_argument('--data_regex')
argument_group.add_argument('--no_california_blueprint', action='store_true')
argument_group.add_argument('--no_cdc_mortality', action='store_true')
argument_group.add_argument('--no_google_mobility', action='store_true')
argument_group.add_argument('--no_state_policy', action='store_true')
argument_group.add_argument('--no_unified_dataset', action='store_true')
argument_group.add_argument('--use_covid_tracking', action='store_true')
argument_group.add_argument('--use_jhu_covid19', action='store_true')


Metric = collections.namedtuple(
    'Metric', ['color', 'emphasis', 'peak', 'frame', 'credits'])

PolicyChange = collections.namedtuple(
    'PolicyChange', ['date', 'score', 'emoji', 'text', 'credits'])


@dataclass(eq=False)
class Region:
    name: str
    short_name: str
    iso_code: Optional[str] = None
    fips_code: Optional[int] = None
    zip_code: Optional[int] = None
    jhu_uid: Optional[int] = None
    unified_id: Optional[str] = None
    lat_lon: Optional[Tuple[float, float]] = None
    totals: collections.Counter = field(default_factory=collections.Counter)
    parent: Optional['Region'] = field(default=None, repr=0)
    subregions: Dict[str, 'Region'] = field(default_factory=dict, repr=0)
    covid_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    map_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    mobility_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    policy_changes: List[PolicyChange] = field(default_factory=list, repr=0)

    def path(r):
        return f'{r.parent.path()}/{r.short_name}' if r.parent else r.name

    def matches_regex(r, rx):
        rx = rx if isinstance(rx, Pattern) else (rx and re.compile(rx, re.I))
        return bool(not rx or rx.search(r.name) or rx.search(r.path()) or
                    rx.search(r.path().replace(' ', '_')))


def get_world(session, args, verbose=False):
    """Returns data organized into a tree rooted at a World region."""

    vprint = lambda *a, **k: print(*a, **k) if verbose else None
    vprint('Loading place data...')
    if args.use_jhu_covid19 and args.no_unified_dataset:
        world = _jhu_skeleton(session)
    else:
        world = _unified_skeleton(session)

    # Index by various forms of ID for merging data in.
    region_by_iso = {}
    region_by_fips = {}
    region_by_jhu = {}
    region_by_uid = {}

    def index_region_tree(r):
        for index_dict, key in [
            (region_by_iso, r.iso_code), (region_by_fips, r.fips_code),
                (region_by_jhu, r.jhu_uid), (region_by_uid, r.unified_id)]:
            if key is not None:
                index_dict[key] = r
        for sub in r.subregions.values():
            index_region_tree(sub)

    index_region_tree(world)

    #
    # Add metrics from the Unified COVID-19 Dataset.
    #

    def trend_metric(color, emphasis, credits, values):
        nonzero_is, = (values.values > 0).nonzero()  # Skip first nonzero.
        first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(values)
        first_i = max(0, min(first_i, len(values) - 14))
        smooth = values[first_i:].rolling(7).mean()
        peak_x = smooth.idxmax()
        peak = None if pandas.isna(peak_x) else (peak_x, smooth.loc[peak_x])
        frame = pandas.DataFrame({'raw': values, 'value': smooth})
        return Metric(
            color=color, emphasis=emphasis, peak=peak, frame=frame,
            credits=credits)

    if not args.no_unified_dataset:
        vprint('Loading Unified COVID-19 Dataset...')
        unified_credits = fetch_unified_dataset.credits()
        unified_data = (
            fetch_unified_dataset.get_data(session)
            .xs(level='Age', key='Total').xs(level='Sex', key='Total'))

        vprint('Merging Unified COVID-19 Dataset...')
        for uid, df in unified_data.groupby(level='ID', sort=False):
            region = region_by_uid.get(uid)
            if not region:
                continue  # Filtered out for one reason or another.

            df.reset_index(level='ID', drop=True, inplace=True)

            def best_source(type):
                # TODO: Pick the best source, not just the first one!
                for_type = df.xs(type)
                return for_type.xs(for_type.index[0][0])

            pop = region.totals['population']
            if 'Confirmed' in df.index:
                confirmed = best_source('Confirmed')
                region.totals['positives'] = confirmed.Cases.iloc[-1]
                region.covid_metrics['positives / 100Kp'] = trend_metric(
                    'tab:blue', 1, unified_credits,
                    confirmed.Cases_New * 1e5 / pop)
            if 'Deaths' in df.index:
                deaths = best_source('Deaths')
                region.totals['deaths'] = deaths.Cases.iloc[-1]
                region.covid_metrics['deaths / 1Mp'] = trend_metric(
                    'tab:red', 1, unified_credits,
                    deaths.Cases_New * 1e6 / pop)
            if 'Tests' in df.index:
                region.covid_metrics['tests / 10Kp'] = trend_metric(
                    'tab:green', 0, unified_credits,
                    best_source('Tests').Cases_New * 1e4 / pop)
            if 'Hospitalized' in df.index:
                region.covid_metrics['hosp admit / 250Kp'] = trend_metric(
                    'tab:orange', 0, unified_credits,
                    best_source('Hospitalized').Cases_New * 25e4 / pop)
            if 'Hospitalized_Now' in df.index:
                region.covid_metrics['hosp current / 25Kp'] = trend_metric(
                    'tab:pink', 0, unified_credits,
                    best_source('Hospitalized_Now').Cases * 25e3 / pop)

    #
    # Add COVID metrics from JHU.
    #

    if args.use_jhu_covid19:
        vprint('Loading JHU COVID data...')
        jhu_credits = fetch_jhu_covid19.credits()
        jhu_data = fetch_jhu_covid19.get_data(session)
        vprint('Merging JHU COVID data...')
        for jid, df in jhu_data.groupby(level='UID', sort=False):
            region = region_by_jhu.get(jid)
            if not region:
                continue  # Filtered out for one reason or another.

            # All rows in the group have the same UID; index only by date.
            df.reset_index(level='UID', drop=True, inplace=True)

            # Convert total cases and deaths into daily cases and deaths.
            region.covid_metrics.update({
                'positives / 100Kp': trend_metric(
                    'tab:blue', 1, jhu_credits,
                    (df.total_cases.iloc[1:] - df.total_cases.values[:-1]) *
                    1e5 / region.totals['population']),
                'deaths / 1Mp': trend_metric(
                    'tab:red', 1, jhu_credits,
                    (df.total_deaths.iloc[1:] - df.total_deaths.values[:-1]) *
                    1e6 / region.totals['population']),
            })

            region.totals['deaths'] = df.total_deaths.iloc[-1]
            region.totals['positives'] = df.total_cases.iloc[-1]

        # Drop subtrees in the region tree with no JHU COVID metrics.
        def prune_region_tree(region):
            region.subregions = {
                k: sub for k, sub in region.subregions.items()
                if prune_region_tree(sub)
            }
            return (region.subregions or region.covid_metrics)

        prune_region_tree(world)

    #
    # Mix in covidtracking data to get hospital data for US states.
    # (Use its cases/deaths data where available, for matching metrics.)
    #

    if args.use_covid_tracking:
        vprint('Loading and merging covidtracking.com data...')
        covid_credits = fetch_covid_tracking.credits()
        covid_tracking = fetch_covid_tracking.get_states(session=session)
        for fips, covid in covid_tracking.groupby(level='fips', sort=False):
            region = region_by_fips.get(fips)
            if region is None:
                continue  # Filtered out for one reason or another.

            # All rows in the group have the same fips; index only by date.
            covid.reset_index(level='fips', drop=True, inplace=True)

            # Take all covidtracking data where available, for consistency.
            pop = region.totals['population']
            region.covid_metrics.update({
                'tests / 10Kp': trend_metric(
                    'tab:green', 0, covid_credits,
                    covid.totalTestResultsIncrease * 1e4 / pop),
                'positives / 100Kp': trend_metric(
                    'tab:blue', 1, covid_credits,
                    covid.positiveIncrease * 1e5 / pop),
                'hosp admit / 250Kp': trend_metric(
                    'tab:orange', 0, covid_credits,
                    covid.hospitalizedIncrease * 25e4 / pop),
                'hosp current / 25Kp': trend_metric(
                    'tab:pink', 0, covid_credits,
                    covid.hospitalizedCurrently * 25e3 / pop),
                'deaths / 1Mp': trend_metric(
                    'tab:red', 1, covid_credits,
                    covid.deathIncrease * 1e6 / pop),
            })

            region.totals['deaths'] = covid.death.iloc[-1]
            region.totals['positives'] = covid.positive.iloc[-1]

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

            mort_1M = mort.Deaths / 365 * 1e6 / region.totals['population']
            region.covid_metrics.update({
                'historical deaths / 1Mp': Metric(
                    color='black', emphasis=-1, peak=None, credits=cdc_credits,
                    frame=pandas.DataFrame(
                        {'value': [mort_1M] * 2}, index=y2020))})

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
        cols = list(enumerate(['FIPS'] + list(cal_counties.columns)))
        tier_col = next(i for i, c in cols if 'Updated Overall Tier' in c)
        prev_col = next(i for i, c in cols if 'Previous Overall Tier' in c)
        date_col = next(i for i, c in cols if 'First Date in Current' in c)
        for row in cal_counties.itertuples(index=True, name=None):
            region = region_by_fips.get(row[0])
            if region is None:
                continue

            tier, prev, date = (row[c] for c in (tier_col, prev_col, date_col))
            td = fetch_california_blueprint.TIER_DESCRIPTION[tier]
            pd = fetch_california_blueprint.TIER_DESCRIPTION[prev]
            text = f'Entered {td.color} tier ({td.name})'
            if td.color != pd.color:
                text += f'; was {pd.emoji} {pd.color} ({pd.name})'

            region.policy_changes.append(PolicyChange(
                date=date, score=3 * numpy.sign(tier - prev - 0.1),
                emoji=td.emoji, credits=cal_credits, text=text))

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
        mobility_data.set_index('date', inplace=True)
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
                'residential': trend_metric(
                    'tab:gray', 1, mobility_credits,
                    100 + m[f'residential_{pcfb}']),
                'retail / recreation': trend_metric(
                    'tab:orange', 1, mobility_credits,
                    100 + m[f'retail_and_recreation_{pcfb}']),
                'workplaces': trend_metric(
                    'tab:red', 1, mobility_credits,
                    100 + m[f'workplaces_{pcfb}']),
                'parks': trend_metric(
                    'tab:green', 0, mobility_credits,
                    100 + m[f'parks_{pcfb}']),
                'grocery / pharmacy': trend_metric(
                    'tab:blue', 0, mobility_credits,
                    100 + m[f'grocery_and_pharmacy_{pcfb}']),
                'transit stations': trend_metric(
                    'tab:purple', 0, mobility_credits,
                    100 + m[f'transit_stations_{pcfb}']),
            })

    #
    # Combine metrics from subregions when not defined at the higher level.
    #

    def roll_up_metrics(r):
        name_popmetrics = {}
        name_poptotals = {}
        for sub in r.subregions.values():
            roll_up_metrics(sub)
            pop = sub.totals['population']
            for name, total in sub.totals.items():
                if name not in r.totals:
                    name_poptotals.setdefault(name, []).append((pop, total))
            for name, metric in sub.covid_metrics.items():
                if name not in r.covid_metrics:
                    name_popmetrics.setdefault(name, []).append((pop, metric))

        # Combine totals & metrics if population sums ~match.
        pop = r.totals['population']
        for name, poptotals in name_poptotals.items():
            if abs(sum(p for p, t in poptotals or []) - pop) < pop * 0.1:
                r.totals[name] = sum(t for p, t in poptotals)

        for name, popmetrics in name_popmetrics.items():
            if abs(sum(p for p, m in popmetrics or []) - pop) < pop * 0.1:
                popmetrics.sort(reverse=True)
                r.covid_metrics[name] = popmetrics[0][1]._replace(
                    credits=dict(
                        c for p, m in popmetrics for c in m.credits.items()),
                    frame=functools.reduce(
                        lambda a, b: a.add(b, fill_value=0.0),
                        (p * m.frame for p, m in popmetrics)) / pop)

    vprint('Rolling up metrics...')
    roll_up_metrics(world)

    #
    # Interpolate synchronized weekly map metrics from time series metrics.
    #

    # Sync map metric weekly data points to this end date.
    latest = max((m.frame.index[-1] for m in world.covid_metrics.values()),
                 default=None)

    def map_metric(color, m, mul):
        first = m.frame.index[0].astimezone(latest.tz)
        weeks = (latest - first) // pandas.Timedelta(days=7)
        dates = pandas.date_range(end=latest, periods=weeks, freq='7D')
        value = mul * numpy.interp(dates, m.frame.index, m.frame.value)
        return Metric(
            color=color, emphasis=None, peak=None, credits=m.credits,
            frame=pandas.DataFrame({'value': value}, index=dates))

    def make_map_metrics(region):
        for sub in region.subregions.values():
            make_map_metrics(sub)

        mul = region.totals['population'] / 50  # 100K => 2K, 1Mp => 200K
        pos = (region.covid_metrics or {}).get('positives / 100Kp')
        if pos is not None:
            region.map_metrics['positives x2K'] = map_metric(
                '#0000FF50', pos, mul)

        death = (region.covid_metrics or {}).get('deaths / 1Mp')
        if death is not None:
            region.map_metrics['deaths x200K'] = map_metric(
                '#FF000050', death, mul)

    make_map_metrics(world)

    def trim_tree(r, rx):
        sub = r.subregions
        region.subregions = {k: s for k, s in sub.items() if trim_tree(s, rx)}
        return (r.subregions or r.matches_regex(rx))

    if args.data_regex:
        vprint(f'Filtering by /{args.data_regex}/...')
        trim_tree(world, re.compile(args.data_regex, re.I))

    return world


def _unified_skeleton(session):
    """Returns a region tree for the world with no metrics populated."""

    def subregion(parent, key, name=None, short_name=None):
        region = parent.subregions.get(key)
        if not region:
            region = parent.subregions[key] = Region(
                name=name or str(key), short_name=short_name or str(key),
                parent=parent)
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
        if p.Admin2:
            region = subregion(region, p.Admin2)
        else:
            region.iso_code = p.ISO2_UID
        if p.Admin3:
            region = subregion(region, p.Admin3)

        if p.ZCTA:
            region.zip_code = int(p.ZCTA)
        elif p.FIPS and p.FIPS.isdigit():
            region.fips_code = int(p.FIPS)

        region.unified_id = p.ID
        region.totals['population'] = p.Population
        if p.Latitude or p.Longitude:
            region.lat_lon = (p.Latitude, p.Longitude)

    # Compute population from subregions if it's not set at the higher level.
    def roll_up_population(region):
        spop = sum(roll_up_population(r) for r in region.subregions.values())
        rpop = region.totals['population']
        rpop = spop if (pandas.isna(rpop) or not (rpop > 0)) else rpop
        if not (rpop > 0):
            raise ValueError(f'No population for "{region.name}"')
        region.totals['population'] = rpop
        return rpop

    roll_up_population(world)
    return world


def _jhu_skeleton(session):
    """Returns a region tree for the world with no metrics populated."""

    def subregion(parent, key, name=None, short_name=None):
        region = parent.subregions.get(key)
        if not region:
            region = parent.subregions[key] = Region(
                name=name or str(key), short_name=short_name or str(key),
                parent=parent)
        return region

    # Do not generate a region for these US county FIPS codes.
    skip_fips = set((
        25007, 25019,                              # MA: "Dukes and Nantucket"
        36005, 36047, 36081, 36085,                # NY: "New York City"
        49003, 49005, 49033,                       # UT: "Bear River"
        49023, 49027, 49039, 49041, 49031, 49055,  # UT: "Central Utah"
        49007, 49015, 49019,                       # UT: "Southeast Utah"
        49001, 49017, 49021, 49025, 49053,         # UT: "Southwest Utah"
        49009, 49013, 49047,                       # UT: "TriCounty"
        49057, 49029))                             # UT: "Weber-Morgan"

    world = Region(name='World', short_name='World')
    for jid, place in fetch_jhu_covid19.get_places(session).items():
        if not (place.Population > 0):
            continue  # We require population data.

        if place.Country_Region == 'US':
            # US specific logic for FIPS, etc.
            region = subregion(world, 'US', us.unitedstatesofamerica.name)
            region.iso_code = 'US'  # Place all territories under US toplevel.
            if place.Province_State:
                s = us.states.lookup(place.Province_State, field='name')
                if s:
                    state_fips = int(s.fips)
                    region = subregion(region, state_fips, s.name, s.abbr)
                    region.fips_code = state_fips
                else:
                    region = subregion(region, place.Province_State)
                if place.iso2 != 'US':
                    region.iso2 = place.iso2
            if place.Admin2:
                if not place.Province_State:
                    raise ValueError(f'Admin2 but no State in {place}')
                key = place.FIPS or place.Admin2
                if key == 36061:
                    key = None  # JHU fudges 36061 (Manhattan) for all NYC.
                elif key == 'Kansas City':
                    continue    # KC data is also allocated to counties.
                elif key in skip_fips:
                    continue    # These regions are tracked elsewhere.
                region = subregion(region, key, place.Admin2, place.Admin2)
                region.fips_code = key if isinstance(key, int) else None

        else:
            # Generic non-US logic.
            country = pycountry.countries.get(name=place.Country_Region)
            if not country:
                country = pycountry.countries.get(alpha_2=place.iso2)
            if country:
                region = subregion(
                    world, country.alpha_2, country.name, country.alpha_2)
                region.iso_code = country.alpha_2
            elif place.Country_Region:
                # Uncoded "countries" are usually cruise ships and such?
                region = subregion(world, place.Country_Region)
            else:
                raise ValueError(f'No country in {place}')
            if place.Province_State:
                region = subregion(region, place.Province_State)
                if country and place.iso2 != country.alpha_2:
                    region.iso_code = place.iso2
            if place.Admin2:
                region = subregion(region, place.Admin2)

        region.jhu_uid = jid
        region.totals['population'] = place.Population
        region.lat_lon = (place.Lat, place.Long_)

    # Compute population from subregions if it's not set at the higher level.
    def roll_up_population(region):
        spop = sum(roll_up_population(r) for r in region.subregions.values())
        rpop = region.totals['population']
        rpop = spop if (pandas.isna(rpop) or not (rpop > 0)) else rpop
        if not (rpop > 0):
            raise ValueError(f'No population for "{region.name}"')
        region.totals['population'] = rpop
        return rpop

    roll_up_population(world)
    return world


if __name__ == '__main__':
    import argparse
    import itertools
    from covid import cache_policy

    parser = argparse.ArgumentParser(
        parents=[cache_policy.argument_parser, argument_parser])
    parser.add_argument('--print_regex')
    parser.add_argument('--print_data', action='store_true')
    args = parser.parse_args()
    world = get_world(
        session=cache_policy.new_session(args),
        args=args, verbose=True)

    print_regex = args.print_regex and re.compile(args.print_regex, re.I)

    def print_tree(prefix, parents, key, r):
        if r.matches_regex(print_regex):
            line = (
                f'{prefix}{r.totals["population"] or -1:9.0f}p <' +
                '.h'[any('hosp' in k for k in r.covid_metrics.keys())] +
                '.c'[bool(r.covid_metrics)] +
                '.m'[bool(r.map_metrics)] +
                '.g'[bool(r.mobility_metrics)] +
                '.p'[bool(r.policy_changes)] + '>')
            if key != r.short_name:
                line = f'{line} [{key}]'
            line = f'{line} {parents}{r.short_name}'
            if r.name not in (key, r.short_name):
                line = f'{line} ({r.name})'
            print(line)
            for cat, metrics in (
                    ('cov', r.covid_metrics),
                    ('map', r.map_metrics),
                    ('mob', r.mobility_metrics)):
                for name, m in metrics.items():
                    max = (' ' * 20 if not m.peak else
                           f' peak={m.peak[1]:<2.0f} @{m.peak[0].date()}')
                    print(f'{prefix}    {len(m.frame):3d}d '
                          f'=>{m.frame.index.max().date()}{max} {cat}: {name}')
                    if args.print_data:
                        print(m.frame)

            for c in r.policy_changes:
                print(f'{prefix}      {c.date.date()} {c.score:+2d} '
                      f'{c.emoji} {c.text}')

        for k, sub in r.subregions.items():
            print_tree(prefix + '  ', f'{parents}{r.short_name}/', k, sub)

    print_tree('', '', world.short_name, world)
