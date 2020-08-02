# Functions that combine data sources into a unified representation.
# (Can also be run as a standalone program for testing.)

import collections
import dataclasses
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
    'Metric', ['color', 'emphasis', 'frame'])

DailyEvents = collections.namedtuple(
    'DailyEvents', ['date', 'score', 'emojis', 'events'])


@dataclass(eq=False)
class Region:
    name: str
    short_name: str
    population: Optional[int] = None
    parent: Optional['Region'] = field(default=None)
    subregions: dict = field(default_factory=dict, repr=False)
    attribution: dict = field(default_factory=dict, repr=False)
    baseline_metrics: dict = field(default_factory=dict, repr=False)
    covid_metrics: dict = field(default_factory=dict, repr=False)
    mobility_metrics: dict = field(default_factory=dict, repr=False)
    daily_events: list = field(default_factory=list, repr=False)


def _trend_frame(values):
    nonzero_is, = (values.values > 0).nonzero()  # Skip first nonzero value.
    first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(values)
    values = values[first_i:]
    return pandas.DataFrame(dict(raw=values, value=values.rolling(7).mean()))


def _threshold_frame(value):
    return pandas.DataFrame(dict(
        value=[value] * 2,
        date=[pandas.to_datetime('2020-01-01'),
              pandas.to_datetime('2020-12-31')])).set_index('date')


def get_world(session):
    world = Region(name='World', short_name='World')

    #
    # Start with JHU data to lay out structure of subregions.
    #

    def subregion(parent, key, name=None, short_name=None):
        return (parent.subregions.get(key) or
                parent.subregions.setdefault(key, Region(
                    name=name or str(key),
                    short_name=short_name or str(key),
                    parent=parent)))

    alpha2_region = {}  # Includes codes like 'GG' and 'PR' for territories.
    jhu_places = fetch_jhu_covid19.get_places(session)
    jhu_data = fetch_jhu_covid19.get_data(session)
    for uid, data in jhu_data.groupby(level='UID', sort=False):
        place = jhu_places[uid]
        if not (place.Population > 0):
            continue  # We require population data.

        if place.Country_Region == 'US':
            # US specific logic for FIPS, etc.
            region = subregion(world, 'US', us.unitedstatesofamerica.name)
            if place.Province_State:
                s = us.states.lookup(place.Province_State, field='name')
                if s:
                    region = subregion(region, int(s.fips), s.name, s.abbr)
                else:
                    region = subregion(region, place.Province_State)
                if place.iso2 != 'US':
                    alpha2_region[place.iso2] = region
            if place.Admin2:
                if not place.Province_State:
                    raise ValueError(f'Admin2 but no State in {place}')
                region = subregion(
                    region, place.FIPS or place.Admin2,
                    place.Admin2, place.Admin2)

        else:
            # Generic non-US logic.
            country = pycountry.countries.get(name=place.Country_Region)
            if not country:
                country = pycountry.countries.get(alpha_2=place.iso2)
            if country:
                region = subregion(
                    world, country.alpha_2, country.name, country.alpha_2)
            elif place.Country_Region:
                # Uncoded "countries" are usually cruise ships and such.
                region = subregion(world, place.Country_Region)
            else:
                raise ValueError(f'No country in {place}')
            if place.Province_State:
                region = subregion(region, place.Province_State)
                if country and place.iso2 != country.alpha_2:
                    alpha2_region[place.iso2] = region
            if place.Admin2:
                region = subregion(region, place.Admin2)

        # Convert total cases and deaths into daily cases and deaths.
        cases = data.total_cases.iloc[1:] - data.total_cases.values[:-1]
        deaths = data.total_deaths.iloc[1:] - data.total_deaths.values[:-1]

        data.reset_index(level='UID', drop=True, inplace=True)
        region.population = place.Population
        region.attribution.update(fetch_jhu_covid19.attribution())
        region.covid_metrics.update({
            'positives / 100Kp': Metric('tab:blue', 1,
                                        _trend_frame(cases * 1e5 / region.population)),
            'deaths / 1Mp': Metric('tab:red', 1,
                                   _trend_frame(deaths * 1e6 / region.population)),
        })

    # Also include "regular" alpha2 country codes in the shortcut map.
    alpha2_region.update(world.subregions)

    # Compute population from subregions if not set at higher level.
    # (Metrics get rolled up later, but pop is needed to compute metrics.)
    def roll_up_population(region):
        pop = sum(roll_up_population(r) for r in region.subregions.values())
        if pandas.isna(region.population) or not (region.population > 0):
            region.population = pop
        if not (region.population > 0):
            raise ValueError(f'No population for "{region.name}"')
        return region.population

    roll_up_population(world)

    #
    # Add baseline mortality data from CDC figures for US states.
    # TODO: Include seasonal variation and county level data?
    # TODO: Some sort of proper excess mortality plotting?
    #

    usa = world.subregions['US']
    cdc_mortality = fetch_cdc_mortality.get_states(session=session)
    for mortality in cdc_mortality.itertuples(name='Mortality'):
        region = usa.subregions.get(mortality.Index)
        if region is None:
            raise ValueError(f'Bad CDC mortality: {mortality}')

        region.attribution.update(fetch_cdc_mortality.attribution())
        region.baseline_metrics.update({
            'historical deaths / 1Mp': Metric('black', -1, _threshold_frame(
                mortality.Deaths / 365 * 1e6 / region.population)),
        })

    #
    # Mix in covidtracking data to get hospital data for US states.
    # (Use its cases/deaths data where available, for matching metrics.)
    #

    covid_tracking = fetch_covid_tracking.get_states(session=session)
    for fips, covid in covid_tracking.groupby(level='fips', sort=False):
        region = usa.subregions.get(fips)
        if region is None:
            raise ValueError(f'Bad covidtracking FIPS: {fips}')

        # Prefer covidtracking data to JHU data, for consistency.
        covid.reset_index(level='fips', drop=True, inplace=True)
        region.attribution.update(fetch_covid_tracking.attribution())
        region.covid_metrics.update({
            'tests / 10Kp': Metric('tab:green', 0, _trend_frame(
                covid.totalTestResultsIncrease * 1e4 / region.population)),
            'positives / 100Kp': Metric('tab:blue', 1, _trend_frame(
                covid.positiveIncrease * 1e5 / region.population)),
            'hosp admit / 250Kp': Metric('tab:orange', 0, _trend_frame(
                covid.hospitalizedIncrease * 25e4 / region.population)),
            'hosp current / 25Kp': Metric('tab:pink', 0, _trend_frame(
                covid.hospitalizedCurrently * 25e3 / region.population)),
            'deaths / 1Mp': Metric('tab:red', 1, _trend_frame(
                covid.deathIncrease * 1e6 / region.population)),
        })

    #
    # Add policy changes for US states from the state policy database.
    #

    state_policy = fetch_state_policy.get_events(session=session)
    state_policy['abs_score'] = state_policy.score.abs()
    for fips, events in state_policy.groupby(level='state_fips', sort=False):
        region = usa.subregions.get(fips)
        if region is None:
            raise ValueError(f'Bad state policy FIPS: {fips}')

        region.attribution.update(fetch_state_policy.attribution())
        for date, es in events.groupby(level='date'):
            events = es.sort_values(['abs_score', 'policy'], ascending=[0, 1])
            smin, smax = events.score.min(), events.score.max()
            score = 0 if smin == -smax else smin if smin < -smax else smax
            emojis = list(dict.fromkeys(
                e.emoji for e in events.itertuples() if abs(e.score) >= 2))
            region.daily_events.append(DailyEvents(
                date=date, score=score, emojis=emojis, events=events))

    #
    # Add mobility data where it's available.
    #

    gcols = [
        'country_region_code', 'sub_region_1', 'sub_region_2',
        'metro_area', 'iso_3166_2_code', 'census_fips_code'
    ]

    mobility_data = fetch_google_mobility.get_mobility(session=session)
    mobility_data.sort_values(by=gcols + ['date'], inplace=True)
    mobility_data.set_index('date', inplace=True)
    for geo, mob in mobility_data.groupby(gcols, as_index=False, sort=False):
        region = alpha2_region.get(geo[0])
        if region is None:
            raise ValueError(f'Bad Google mobility country: {geo}')

        fips = geo[5]
        if fips >= 100:  # County FIPS
            region = region.subregions.get(fips // 1000)
            region = region and region.subregions.get(fips)
            if region is None:
                raise ValueError(f'Bad Google mobility county FIPS: {geo}')
        elif fips > 0:   # State FIPS
            region = region.subregions.get(fips)
            if region is None:
                raise ValueError(f'Bad Google mobility state FIPS: {geo}')
        else:
            for name in geo[1:4]:
                if name and region:  # TODO - fix Puerto Rico??
                    region = region.subregions.get(name)
            if region is None:
                continue

        pcfb = 'percent_change_from_baseline'  # common, long suffix
        region.attribution.update(fetch_google_mobility.attribution())
        region.mobility_metrics.update({
            'retail / recreation': Metric('tab:orange', 1, _trend_frame(
                mob[f'retail_and_recreation_{pcfb}'])),
            'grocery / pharmacy': Metric('tab:blue', 1, _trend_frame(
                mob[f'grocery_and_pharmacy_{pcfb}'])),
            'parks': Metric('tab:green', 1, _trend_frame(
                mob[f'parks_{pcfb}'])),
            'transit stations': Metric('tab:purple', 1, _trend_frame(
                mob[f'transit_stations_{pcfb}'])),
            'workplaces': Metric('tab:red', 1, _trend_frame(
                mob[f'workplaces_{pcfb}'])),
            'residential': Metric('tab:gray', 1, _trend_frame(
                mob[f'residential_{pcfb}'])),
        })

    # TODO: Roll up metrics into higher level regions as needed.

    return world


def get_states(session, select_states):
    select_fips = [int(us.states.lookup(n).fips) for n in select_states or []]

    covid_states = fetch_covid_tracking.get_states(session=session)
    census_states = fetch_census_population.get_states(session=session)
    mortality_states = fetch_cdc_mortality.get_states(session=session)
    policy_events = fetch_state_policy.get_events(session=session)
    mobility_data = fetch_google_mobility.get_mobility(session=session)

    attribution = {
        **fetch_covid_tracking.attribution(),
        **fetch_census_population.attribution(),
        **fetch_cdc_mortality.attribution(),
        **fetch_state_policy.attribution(),
        **fetch_google_mobility.attribution()
    }

    policy_events['abs_score'] = policy_events.score.abs()
    events_by_state = policy_events.groupby(level='state_fips', sort=False)

    regions = []
    for fips, covid in covid_states.groupby(level='fips', sort=False):
        if select_fips and fips not in select_fips:
            continue

        try:
            census = census_states.loc[fips]
            mortality = mortality_states.loc[fips]
        except KeyError:
            continue

        pop = census.POP
        baseline_metrics = {
            'historical deaths / 1Mp': Metric('black', -1,
                                              _threshold_frame(mortality.Deaths / 365 * 1e6 / pop)),
        }

        covid.reset_index(level='fips', drop=True, inplace=True)
        covid_metrics = {
            'tests / 10Kp': Metric('tab:green', 0, _trend_frame(
                covid.totalTestResultsIncrease * 1e4 / pop)),
            'positives / 100Kp': Metric('tab:blue', 1, _trend_frame(
                covid.positiveIncrease * 1e5 / pop)),
            'hosp admit / 250Kp': Metric('tab:orange', 0, _trend_frame(
                covid.hospitalizedIncrease * 25e4 / pop)),
            'hosp current / 25Kp': Metric('tab:pink', 0, _trend_frame(
                covid.hospitalizedCurrently * 25e3 / pop)),
            'deaths / 1Mp': Metric('tab:red', 1, _trend_frame(
                covid.deathIncrease * 1e6 / pop)),
        }

        mob = mobility_data[mobility_data.census_fips_code.eq(fips)]
        mob = mob.sort_values(by='date')
        mob.set_index('date', inplace=True)
        pcfb = 'percent_change_from_baseline'  # common, long suffix
        mobility_metrics = {
            'retail / recreation': Metric('tab:orange', 1, _trend_frame(
                mob[f'retail_and_recreation_{pcfb}'])),
            'grocery / pharmacy': Metric('tab:blue', 1, _trend_frame(
                mob[f'grocery_and_pharmacy_{pcfb}'])),
            'parks': Metric('tab:green', 1, _trend_frame(
                mob[f'parks_{pcfb}'])),
            'transit stations': Metric('tab:purple', 1, _trend_frame(
                mob[f'transit_stations_{pcfb}'])),
            'workplaces': Metric('tab:red', 1, _trend_frame(
                mob[f'workplaces_{pcfb}'])),
            'residential': Metric('tab:gray', 1, _trend_frame(
                mob[f'residential_{pcfb}'])),
        }

        daily_events = []
        state_events = events_by_state.get_group(fips)
        for date, es in state_events.groupby(level='date'):
            events = es.sort_values(['abs_score', 'policy'], ascending=[0, 1])
            smin, smax = events.score.min(), events.score.max()
            score = 0 if smin == -smax else smin if smin < -smax else smax
            emojis = list(dict.fromkeys(
                e.emoji for e in events.itertuples() if abs(e.score) >= 2))
            daily_events.append(DailyEvents(
                date=date, score=score, emojis=emojis, events=events))

        state = us.states.lookup(f'{fips:02d}')
        regions.append(Region(
            name=state.name, short_name=state.abbr,
            subregions=[], population=census.POP,
            attribution=attribution,
            baseline_metrics=baseline_metrics,
            covid_metrics=covid_metrics,
            mobility_metrics=mobility_metrics,
            daily_events=daily_events))

    return regions


if __name__ == '__main__':
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    world = get_world(session=cache_policy.new_session(parser.parse_args()))

    def print_region(r, key, indent):
        line = (
            f'{" " * indent}{r.population:9.0f}p [' +
            ' b'[bool(r.baseline_metrics)] +
            ' c'[bool(r.covid_metrics)] +
            ' h'[any('hosp' in k for k in r.covid_metrics.keys())] +
            ' m'[bool(r.mobility_metrics)] +
            ' p'[bool(r.daily_events)] +
            f'] {key}')
        if r.short_name != key:
            line = f'{line}: {r.short_name}'
            if r.name not in (key, r.short_name):
                line = f'{line} ({r.name})'
        elif r.name != key:
            line = f'{line}: {r.name}'
        print(line)
        for k, v in r.subregions.items():
            print_region(v, k, indent + 2)

    print_region(world, 'World', 0)
