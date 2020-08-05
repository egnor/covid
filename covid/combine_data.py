# Functions that combine data sources into a unified representation.
# (Can also be run as a standalone program for testing.)

import collections
import functools
import re
from dataclasses import dataclass, field
from typing import Optional

import numpy
import pandas
import pycountry
import us

from covid import fetch_cdc_mortality
from covid import fetch_census_population
from covid import fetch_covid_tracking
from covid import fetch_google_mobility
from covid import fetch_state_policy
from covid import fetch_jhu_covid19


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
    parent: Optional['Region'] = field(default=None)
    subregions: dict = field(default_factory=dict, repr=False)
    credits: dict = field(default_factory=dict, repr=False)
    baseline_metrics: dict = field(default_factory=dict, repr=False)
    covid_metrics: dict = field(default_factory=dict, repr=False)
    mobility_metrics: dict = field(default_factory=dict, repr=False)
    daily_events: list = field(default_factory=list, repr=False)


def get_world(session, filter_regex=None, verbose=False):
    """Returns data organized into a tree rooted at a World region."""

    vprint = lambda *a, **k: print(*a, **k) if verbose else None

    vprint('Loading JHU place data...')
    world = _get_skeleton(session, filter_regex)

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

    # Populate the tree with JHU metrics.
    vprint('Loading JHU COVID data...')
    jhu_data = fetch_jhu_covid19.get_data(session)
    vprint('Merging JHU COVID data...')
    for uid, data in jhu_data.groupby(level='UID', sort=False):
        region = region_by_uid.get(uid)
        if not region:
            continue  # Filtered out for one reason or another.

        # Convert total cases and deaths into daily cases and deaths.
        data.reset_index(level='UID', drop=True, inplace=True)
        cases = data.total_cases.iloc[1:] - data.total_cases.values[:-1]
        deaths = data.total_deaths.iloc[1:] - data.total_deaths.values[:-1]

        region.covid_metrics.update({
            'positives / 100Kp': _trend_metric(
                'tab:blue', 1, cases * 1e5 / region.population),
            'deaths / 1Mp': _trend_metric(
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

    vprint('Loading and merging CDC mortality data...')
    cdc_mortality = fetch_cdc_mortality.get_states(session=session)
    for mortality in cdc_mortality.itertuples(name='Mortality'):
        region = region_by_fips.get(mortality.Index)
        if region is None:
            continue

        region.credits.update(fetch_cdc_mortality.credits())
        region.baseline_metrics.update({
            'historical deaths / 1Mp': _threshold_metric(
                'black', mortality.Deaths / 365 * 1e6 / region.population),
        })

    #
    # Mix in covidtracking data to get hospital data for US states.
    # (Use its cases/deaths data where available, for matching metrics.)
    #

    vprint('Loading and merging covidtracking.com data...')
    covid_tracking = fetch_covid_tracking.get_states(session=session)
    for fips, covid in covid_tracking.groupby(level='fips', sort=False):
        region = region_by_fips.get(fips)
        if region is None:
            continue

        # Prefer covidtracking data to JHU data, for consistency.
        covid.reset_index(level='fips', drop=True, inplace=True)
        region.credits.update(fetch_covid_tracking.credits())
        region.covid_metrics.update({
            'tests / 10Kp': _trend_metric(
                'tab:green', 0,
                covid.totalTestResultsIncrease * 1e4 / region.population),
            'positives / 100Kp': _trend_metric(
                'tab:blue', 1,
                covid.positiveIncrease * 1e5 / region.population),
            'hosp admit / 250Kp': _trend_metric(
                'tab:orange', 0,
                covid.hospitalizedIncrease * 25e4 / region.population),
            'hosp current / 25Kp': _trend_metric(
                'tab:pink', 0,
                covid.hospitalizedCurrently * 25e3 / region.population),
            'deaths / 1Mp': _trend_metric(
                'tab:red', 1,
                covid.deathIncrease * 1e6 / region.population),
        })

    #
    # Add policy changes for US states from the state policy database.
    #

    vprint('Loading and merging state policy database...')
    state_policy = fetch_state_policy.get_events(session=session)
    state_policy['abs_score'] = state_policy.score.abs()
    for fips, events in state_policy.groupby(level='state_fips', sort=False):
        region = region_by_fips.get(fips)
        if region is None:
            continue

        region.credits.update(fetch_state_policy.credits())
        for date, es in events.groupby(level='date'):
            frame = es.sort_values(['abs_score', 'policy'], ascending=[0, 1])
            smin, smax = frame.score.min(), frame.score.max()
            score = 0 if smin == -smax else smin if smin < -smax else smax
            emojis = list(dict.fromkeys(
                e.emoji for e in frame.itertuples() if abs(e.score) >= 2))
            region.daily_events.append(DailyEvents(
                date=date, score=score, emojis=emojis, frame=frame))

    #
    # Add mobility data where it's available.
    #

    gcols = [
        'country_region_code', 'sub_region_1', 'sub_region_2',
        'metro_area', 'iso_3166_2_code', 'census_fips_code'
    ]

    vprint('Loading Google mobility data...')
    mobility_data = fetch_google_mobility.get_mobility(session=session)
    vprint('Merging Google mobility data...')
    mobility_data.sort_values(by=gcols + ['date'], inplace=True)
    mobility_data.set_index('date', inplace=True)
    for geo, mob in mobility_data.groupby(gcols, as_index=False, sort=False):
        if geo[5]:
            region = region_by_fips.get(geo[5])
        else:
            region = region_by_iso.get(geo[0])
            for n in geo[1:4]:
                region = region.subregions.get(n) if (region and n) else region

        if region is None:
            continue

        pcfb = 'percent_change_from_baseline'  # common, long suffix
        region.credits.update(fetch_google_mobility.credits())
        region.mobility_metrics.update({
            'retail / recreation': _trend_metric(
                'tab:orange', 1, mob[f'retail_and_recreation_{pcfb}']),
            'grocery / pharmacy': _trend_metric(
                'tab:blue', 1, mob[f'grocery_and_pharmacy_{pcfb}']),
            'parks': _trend_metric(
                'tab:green', 1, mob[f'parks_{pcfb}']),
            'transit stations': _trend_metric(
                'tab:purple', 1, mob[f'transit_stations_{pcfb}']),
            'workplaces': _trend_metric(
                'tab:red', 1, mob[f'workplaces_{pcfb}']),
            'residential':_trend_metric(
                'tab:gray', 1, mob[f'residential_{pcfb}']),
        })

    #
    # Combine metrics from subregions when not defined at the higher level.
    #

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

        # Only combine metrics if they're defined for >90% of the population.
        for name, popmetrics in name_popmetrics.items():
            if sum(p for p, m in popmetrics or []) >= region.population * 0.9:
                region.credits.update(name_credits.get(name, {}))
                popmetrics.sort(reverse=True)
                popsum = functools.reduce(
                    lambda a, b: a.add(b, fill_value=0.0),
                    (m.frame * p for p, m in popmetrics))
                region.covid_metrics[name] = popmetrics[0][1]._replace(
                    frame=popsum / region.population)

    roll_up_metrics(world)
    return world


def _get_skeleton(session, filter_regex):
    """Returns a region tree for the world with no metrics populated."""

    jhu_credits = fetch_jhu_covid19.credits()
    world = Region(name='World', short_name='World', credits={**jhu_credits})
    filter_regex = filter_regex and re.compile(filter_regex, re.I)

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
            region.iso_code = 'US'
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
                region = subregion(
                    region, place.FIPS or place.Admin2,
                    place.Admin2, place.Admin2)
                region.fips_code = place.FIPS

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

    def filter_region_tree(parents, region):
        region.subregions = {
            k: sub for k, sub in region.subregions.items()
            if filter_region_tree(f'{parents}/{sub.short_name}', sub)
        }
        return (region.subregions or filter_regex.search(parents) or
                filter_regex.search(region.name))

    if filter_regex and not filter_region_tree('world', world):
        return world  # All filtered out, return only a stub world region.

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


def _trend_metric(color, emphasis, values):
    nonzero_is, = (values.values > 0).nonzero()  # Skip first nonzero value.
    first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(values)
    first_i = max(0, min(first_i, len(values) - 14))
    smooth = values[first_i:].rolling(7).mean()
    peak_x = smooth.idxmax()
    peak = None if pandas.isna(peak_x) else (peak_x, smooth.loc[peak_x])
    return Metric(
        color=color, emphasis=emphasis, peak=peak,
        frame=pandas.DataFrame(dict(raw=values, value=smooth)))


def _threshold_metric(color, value):
    return Metric(
        color=color, emphasis=-1, peak=None,
        frame=pandas.DataFrame(dict(
            value=[value] * 2,
            date=[pandas.to_datetime('2020-01-01'),
                  pandas.to_datetime('2020-12-31')])).set_index('date'))


if __name__ == '__main__':
    import argparse
    import itertools
    from covid import cache_policy

    parser=argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument('--filter_regex')
    args=parser.parse_args()
    world=get_world(
        session=cache_policy.new_session(args),
        filter_regex=args.filter_regex,
        verbose=True)

    def print_tree(prefix, parents, key, r):
        line=(
            f'{prefix}{r.population:9.0f}p <' +
            '.b'[bool(r.baseline_metrics)] +
            '.c'[bool(r.covid_metrics)] +
            '.h'[any('hosp' in k for k in r.covid_metrics.keys())] +
            '.m'[bool(r.mobility_metrics)] +
            '.p'[bool(r.daily_events)] + '>')
        if key != r.short_name:
            line=f'{line} [{key}]'
        line=f'{line} {parents}{r.short_name}'
        if r.name not in (key, r.short_name):
            line=f'{line} ({r.name})'
        print(line)
        for n, m in itertools.chain(
                r.baseline_metrics.items(), r.covid_metrics.items(),
                r.mobility_metrics.items()):
            max = ('' if not m.peak else
                   f' max={m.peak[1]:<2.0f} @{m.peak[0].date()}')
            print(f'{prefix}           {len(m.frame):3d}d '
                  f'=>{m.frame.index.max().date()}{max} {n}')

        for k, sub in r.subregions.items():
            print_tree(prefix + '  ', f'{parents}{r.short_name}/', k, sub)

    print_tree('', '', world.short_name, world)
