"""Functions that combine data sources into a unified representation."""

import argparse
import collections
import functools
import re
from dataclasses import dataclass, field
from re import compile, Pattern
from typing import Optional, Tuple

import numpy
import pandas
import pycountry
import us

from covid import fetch_cdc_mortality
from covid import fetch_covid_tracking
from covid import fetch_google_mobility
from covid import fetch_jhu_covid19
from covid import fetch_state_policy


# Reusable command line arguments for data collection.
argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group('data gathering')
argument_group.add_argument('--data_filter')
argument_group.add_argument('--no_cdc_mortality', action='store_true')
argument_group.add_argument('--no_covid_tracking', action='store_true')
argument_group.add_argument('--no_google_mobility', action='store_true')
argument_group.add_argument('--no_jhu_covid19', action='store_true')
argument_group.add_argument('--no_state_policy', action='store_true')


Metric = collections.namedtuple(
    'Metric', ['color', 'emphasis', 'peak', 'frame'])

DailyEvents = collections.namedtuple(
    'DailyEvents', ['date', 'score', 'emojis', 'frame'])


@dataclass(eq=False)
class Region:
    name: str
    short_name: str
    jhu_uid: Optional[int] = None
    iso_code: Optional[str] = None
    fips_code: Optional[int] = None
    population: Optional[int] = None
    lat_lon: Optional[Tuple[float, float]] = None
    parent: Optional['Region'] = field(default=None, repr=False)
    subregions: dict = field(default_factory=dict, repr=False)
    credits: dict = field(default_factory=dict, repr=False)
    baseline_metrics: dict = field(default_factory=dict, repr=False)
    covid_metrics: dict = field(default_factory=dict, repr=False)
    map_metrics: dict = field(default_factory=dict, repr=False)
    mobility_metrics: dict = field(default_factory=dict, repr=False)
    daily_events: list = field(default_factory=list, repr=False)

    def path(r):
        return f'{r.parent.path()}/{r.short_name}' if r.parent else r.name

    def matches_regex(r, rx):
        rx = rx if isinstance(rx, Pattern) else (rx and re.compile(rx, re.I))
        return bool(not rx or rx.search(r.name) or rx.search(r.path()) or
                    rx.search(r.path().replace(' ', '_')))


def get_world(session, args, verbose=False):
    """Returns data organized into a tree rooted at a World region."""

    vprint = lambda *a, **k: print(*a, **k) if verbose else None

    vprint('Loading JHU place data...')
    world = _get_skeleton(session)

    # Index by various forms of ID for merging data in.
    region_by_iso = {}
    region_by_fips = {}
    region_by_uid = {}

    def index_region_tree(region):
        if region.iso_code:
            region_by_iso[region.iso_code] = region
        if region.fips_code:
            region_by_fips[region.fips_code] = region
        if region.jhu_uid:
            region_by_uid[region.jhu_uid] = region
        for sub in region.subregions.values():
            index_region_tree(sub)

    index_region_tree(world)

    #
    # Add COVID metrics from JHU.
    #

    def trend_metric(color, emphasis, values):
        nonzero_is, = (values.values > 0).nonzero()  # Skip first nonzero.
        first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(values)
        first_i = max(0, min(first_i, len(values) - 14))
        smooth = values[first_i:].rolling(7).mean()
        peak_x = smooth.idxmax()
        peak = None if pandas.isna(peak_x) else (peak_x, smooth.loc[peak_x])
        frame = pandas.DataFrame({'raw': values, 'value': smooth})
        return Metric(color=color, emphasis=emphasis, peak=peak, frame=frame)

    if not args.no_jhu_covid19:
        # Populate the tree with JHU metrics.
        vprint('Loading JHU COVID data...')
        jhu_data = fetch_jhu_covid19.get_data(session)
        vprint('Merging JHU COVID data...')
        for uid, d in jhu_data.groupby(level='UID', sort=False):
            region = region_by_uid.get(uid)
            if not region:
                continue  # Filtered out for one reason or another.

            # Convert total cases and deaths into daily cases and deaths.
            d.reset_index(level='UID', drop=True, inplace=True)
            cases = d.total_cases.iloc[1:] - d.total_cases.values[:-1]
            deaths = d.total_deaths.iloc[1:] - d.total_deaths.values[:-1]

            region.covid_metrics.update({
                'positives / 100Kp': trend_metric(
                    'tab:blue', 1, cases * 1e5 / region.population),
                'deaths / 1Mp': trend_metric(
                    'tab:red', 1, deaths * 1e6 / region.population),
            })

        # Drop subtrees in the place tree with no JHU COVID metrics.
        def prune_region_tree(region):
            region.subregions = {
                k: sub for k, sub in region.subregions.items()
                if prune_region_tree(sub)
            }
            return (region.subregions or region.covid_metrics)

        prune_region_tree(world)

    #
    # Add baseline mortality data from CDC figures for US states.
    # TODO: Include seasonal variation and county level data?
    # TODO: Some sort of proper excess mortality plotting?
    #

    threshold_metric = (lambda color, value: Metric(
        color=color, emphasis=-1, peak=None,
        frame=pandas.DataFrame(
            {'value': [value] * 2},
            index=pandas.DatetimeIndex(['2020-01-01', '2020-12-31']))))

    if not args.no_cdc_mortality:
        vprint('Loading and merging CDC mortality data...')
        cdc_mortality = fetch_cdc_mortality.get_states(session=session)
        for mort in cdc_mortality.itertuples(name='Mortality'):
            region = region_by_fips.get(mort.Index)
            if region is None:
                continue

            region.credits.update(fetch_cdc_mortality.credits())
            region.baseline_metrics.update({
                'historical deaths / 1Mp': threshold_metric(
                    'black', mort.Deaths / 365 * 1e6 / region.population),
            })

    #
    # Mix in covidtracking data to get hospital data for US states.
    # (Use its cases/deaths data where available, for matching metrics.)
    #

    if not args.no_covid_tracking:
        vprint('Loading and merging covidtracking.com data...')
        covid_tracking = fetch_covid_tracking.get_states(session=session)
        for fips, covid in covid_tracking.groupby(level='fips', sort=False):
            region = region_by_fips.get(fips)
            if region is None:
                continue

            # Take all covidtracking data where available, for consistency.
            covid.reset_index(level='fips', drop=True, inplace=True)
            region.credits.update(fetch_covid_tracking.credits())
            region.covid_metrics.update({
                'tests / 10Kp': trend_metric(
                    'tab:green', 0,
                    covid.totalTestResultsIncrease * 1e4 / region.population),
                'positives / 100Kp': trend_metric(
                    'tab:blue', 1,
                    covid.positiveIncrease * 1e5 / region.population),
                'hosp admit / 250Kp': trend_metric(
                    'tab:orange', 0,
                    covid.hospitalizedIncrease * 25e4 / region.population),
                'hosp current / 25Kp': trend_metric(
                    'tab:pink', 0,
                    covid.hospitalizedCurrently * 25e3 / region.population),
                'deaths / 1Mp': trend_metric(
                    'tab:red', 1,
                    covid.deathIncrease * 1e6 / region.population),
            })

    #
    # Add policy changes for US states from the state policy database.
    #

    if not args.no_state_policy:
        vprint('Loading and merging state policy database...')
        state_policy = fetch_state_policy.get_events(session=session)
        state_policy['abs_score'] = state_policy.score.abs()
        for f, events in state_policy.groupby(level='state_fips', sort=False):
            region = region_by_fips.get(f)
            if region is None:
                continue

            region.credits.update(fetch_state_policy.credits())
            for date, es in events.groupby(level='date'):
                df = es.sort_values(['abs_score', 'policy'], ascending=[0, 1])
                smin, smax = df.score.min(), df.score.max()
                score = 0 if smin == -smax else smin if smin < -smax else smax
                emojis = list(dict.fromkeys(
                    e.emoji for e in df.itertuples() if abs(e.score) >= 2))
                region.daily_events.append(DailyEvents(
                    date=date, score=score, emojis=emojis, frame=df))

    #
    # Add mobility data where it's available.
    #

    if not args.no_google_mobility:
        gcols = [
            'country_region_code', 'sub_region_1', 'sub_region_2',
            'metro_area', 'iso_3166_2_code', 'census_fips_code'
        ]

        vprint('Loading Google mobility data...')
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
            region.credits.update(fetch_google_mobility.credits())
            region.mobility_metrics.update({
                'residential': trend_metric(
                    'tab:gray', 1, 100 + m[f'residential_{pcfb}']),
                'retail / recreation': trend_metric(
                    'tab:orange', 1, 100 + m[f'retail_and_recreation_{pcfb}']),
                'workplaces': trend_metric(
                    'tab:red', 1, 100 + m[f'workplaces_{pcfb}']),
                'parks': trend_metric(
                    'tab:green', 0, 100 + m[f'parks_{pcfb}']),
                'grocery / pharmacy': trend_metric(
                    'tab:blue', 0, 100 + m[f'grocery_and_pharmacy_{pcfb}']),
                'transit stations': trend_metric(
                    'tab:purple', 0, 100 + m[f'transit_stations_{pcfb}']),
            })

    #
    # Combine metrics from subregions when not defined at the higher level.
    #

    map_metric = (lambda index, input_df: Metric(
        color=None, emphasis=None, peak=None,
        frame=pandas.DataFrame(
            {'value': numpy.interp(index, input_df.index, input_df.value)},
            index=index)))

    def roll_up_metrics(region):
        # Use None to mark metrics already defined at the higher level.
        name_popmetrics = {name: None for name in region.covid_metrics.keys()}
        name_credits = {}
        for sub in region.subregions.values():
            roll_up_metrics(sub)
            for name, metric in sub.covid_metrics.items():
                popmetrics = name_popmetrics.setdefault(name, [])
                if popmetrics is not None:
                    popmetrics.append((sub.population, metric))
                    name_credits.setdefault(name, {}).update(sub.credits)

        # Combine metrics if they're defined for >90% of the population.
        for name, popmetrics in name_popmetrics.items():
            if sum(p for p, m in popmetrics or []) >= region.population * 0.9:
                region.credits.update(name_credits.get(name, {}))
                popmetrics.sort(reverse=True)
                popsum = functools.reduce(
                    lambda a, b: a.add(b, fill_value=0.0),
                    (m.frame * p for p, m in popmetrics))
                region.covid_metrics[name] = popmetrics[0][1]._replace(
                    frame=popsum / region.population)

        # Make synchronized weekly map metrics from time series metrics.
        if region.covid_metrics:
            pos_df = region.covid_metrics['positives / 100Kp'].frame
            death_df = region.covid_metrics['deaths / 1Mp'].frame
            weekly = pandas.date_range(
                start=pos_df.index[0].astimezone(None),
                end=pos_df.index[-1].astimezone(None),
                freq='W', normalize=True)
            region.map_metrics.update({
                'positives / 100Kp': map_metric(weekly, pos_df),
                'deaths / 1Mp': map_metric(weekly, death_df),
            })

    roll_up_metrics(world)

    def trim_tree(r, rx):
        sub = r.subregions
        region.subregions = {k: s for k, s in sub.items() if trim_tree(s, rx)}
        return (r.subregions or r.matches_regex(rx))

    if args.data_filter:
        vprint(f'Filtering by /{args.data_filter}/...')
        trim_tree(world, re.compile(args.data_filter, re.I))

    return world


def _get_skeleton(session):
    """Returns a region tree for the world with no metrics populated."""

    jhu_credits = fetch_jhu_covid19.credits()
    world = Region(name='World', short_name='World', credits={**jhu_credits})

    def subregion(parent, key, name=None, short_name=None):
        region = parent.subregions.get(key)
        if not region:
            region = parent.subregions[key] = Region(
                name=name or str(key), short_name=short_name or str(key),
                parent=parent, credits={**jhu_credits})
        return region

    jhu_places = fetch_jhu_covid19.get_places(session)
    for uid, place in jhu_places.items():
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
                fips = place.FIPS
                if place.Admin2 == 'New York City':
                    fips = None  # JHU fudges 36061 (Manhattan) for all NYC.
                elif place.Admin2 in ('Bronx', 'Kings', 'Queens', 'Richmond'):
                    continue  # JHU zeroes out data for other boroughs.
                region = subregion(
                    region, fips or place.Admin2, place.Admin2, place.Admin2)
                region.fips_code = fips

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

        region.jhu_uid = uid
        region.population = place.Population
        region.lat_lon = (place.Lat, place.Long_)

    # Compute population from subregions if it's not set at the higher level.
    def roll_up_population(region):
        pop = sum(roll_up_population(r) for r in region.subregions.values())
        if pandas.isna(region.population) or not (region.population > 0):
            region.population = pop
        if not (region.population > 0):
            raise ValueError(f'No population for "{region.name}"')
        return region.population

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
                f'{prefix}{r.population or -1:9.0f}p <' +
                '.b'[bool(r.baseline_metrics)] +
                '.h'[any('hosp' in k for k in r.covid_metrics.keys())] +
                '.c'[bool(r.covid_metrics)] +
                '.m'[bool(r.map_metrics)] +
                '.g'[bool(r.mobility_metrics)] +
                '.p'[bool(r.daily_events)] + '>')
            if key != r.short_name:
                line = f'{line} [{key}]'
            line = f'{line} {parents}{r.short_name}'
            if r.name not in (key, r.short_name):
                line = f'{line} ({r.name})'
            print(line)
            for cat, metrics in (
                    ('bas', r.baseline_metrics),
                    ('cov', r.covid_metrics),
                    ('map', r.map_metrics),
                    ('mob', r.mobility_metrics)):
                for name, m in metrics.items():
                    max = (' ' * 20 if not m.peak else
                           f' peak={m.peak[1]:<2.0f} @{m.peak[0].date()}')
                    print(f'{prefix}           {len(m.frame):3d}d '
                          f'=>{m.frame.index.max().date()}{max} {cat}: {name}')
                    if args.print_data:
                        print(m.frame)

        for k, sub in r.subregions.items():
            print_tree(prefix + '  ', f'{parents}{r.short_name}/', k, sub)

    print_tree('', '', world.short_name, world)
