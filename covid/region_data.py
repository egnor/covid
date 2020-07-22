# Data types and related utilities for aggregated regional stats.

import collections

import numpy
import pandas
import us

from . import fetch_cdc_mortality
from . import fetch_census_population
from . import fetch_covid_tracking
from . import fetch_google_mobility
from . import fetch_state_policy


Metric = collections.namedtuple(
    'MetricData', 'name color importance frame')

DailyEvents = collections.namedtuple(
    'PolicyData', 'date score emojis events')

Region = collections.namedtuple(
    'RegionData',
    'id name population date attribution '
    'covid_metrics mobility_metrics daily_events '
)


def _name_per_capita(name, capita):
    def number(n):
        return numpy.format_float_positional(n, precision=3, trim='-')
    return (
        f'{name} / {number(capita / 1000000)}Mp' if capita >= 1000000 else
        f'{name} / {number(capita / 1000)}Kp' if capita >= 10000 else
        f'{name} / {number(capita)}p')


def _trend_per_capita(name, color, imp, date, raw, capita, pop):
    nonzero_is, = (raw.values > 0).nonzero()
    first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(raw)
    date = date[first_i:]
    per_cap = raw[first_i:] * capita / pop

    f = pandas.DataFrame(dict(
        date=date, raw=per_cap, value=per_cap.rolling(7).mean()))
    f.set_index('date', inplace=True)
    return Metric(_name_per_capita(name, capita), color, imp, f)


def _threshold_per_capita(name, color, imp, value, capita, pop):
    f = pandas.DataFrame(dict(
        value=[value * capita / pop] * 2,
        date=[pandas.to_datetime('2020-01-01'),
              pandas.to_datetime('2020-12-31')]))
    f.set_index('date', inplace=True)
    return Metric(_name_per_capita(name, capita), color, imp, f)


def get_regions(session, select_states):
    select_fips = [us.states.lookup(n).fips for n in select_states or []]

    covid_states = fetch_covid_tracking.get_states(session=session)
    census_states = fetch_census_population.get_states(session=session)
    mortality_states = fetch_cdc_mortality.get_states(session=session)
    policy_events = fetch_state_policy.get_events(session=session)
    mobility_data = fetch_google_mobility.get_mobility(session=session)

    attribution = {}
    attribution.update(fetch_covid_tracking.attribution())
    attribution.update(fetch_census_population.attribution())
    attribution.update(fetch_cdc_mortality.attribution())
    attribution.update(fetch_state_policy.attribution())
    attribution.update(fetch_google_mobility.attribution())

    update_date = covid_states.date.max()
    policy_events['abs_score'] = policy_events.score.abs()
    events_by_state = policy_events.groupby('state_fips')

    regions = []
    for fips, covid in covid_states.groupby(by='fips'):
        if select_fips and fips not in select_fips:
            continue

        try:
            census = census_states.loc[fips]
            mortality = mortality_states.loc[fips]
        except KeyError:
            continue

        covid = covid.sort_values(by='date')
        covid_metrics = [
            _trend_per_capita(
                name='tests', color='tab:green', imp=0, date=covid.date,
                raw=covid.totalTestResultsIncrease, capita=1e4, pop=census.POP),
            _trend_per_capita(
                name='positives', color='tab:blue', imp=1, date=covid.date,
                raw=covid.positiveIncrease, capita=1e5, pop=census.POP),
            _trend_per_capita(
                name='hosp admit', color='tab:orange', imp=0, date=covid.date,
                raw=covid.hospitalizedIncrease, capita=25e4, pop=census.POP),
            _trend_per_capita(
                name='hosp current', color='tab:pink', imp=0, date=covid.date,
                raw=covid.hospitalizedCurrently, capita=25e3, pop=census.POP),
            _trend_per_capita(
                name='deaths', color='tab:red', imp=1, date=covid.date,
                raw=covid.deathIncrease, capita=1e6, pop=census.POP),
            _threshold_per_capita(
                name='historical deaths', color='black', imp=-1,
                raw=mortality.Deaths / 365, capita=1e6, pop=census.POP),
        ]

        mob = mobility_data[mobility_data.census_fips_code == fips]
        mob = mobility.sort_values(by='date')
        mobility_metrics = [
            Metric(name='retail / recreation', color='tab:orange',
                   importance=1, frame=pandas.DataFrame(dict(
                       date=mobility.date,
                       value=mob.retail_and_recreation_percent_change_from_baseline))),
            Metric(name='grocery / pharmacy', color='tab:blue',
                   importance=1, frame=pandas.DataFrame(dict(
                       date=mobility.date,
                       value=mob.grocery_and_pharmacy_percent_change_from_baseline))),
            Metric(name='parks', color='tab:green',
                   importance=1, frame=pandas.DataFrame(dict(
                       date=mobility.date,
                       value=mob.parks_percent_change_from_baseline))),
            Metric(name='transit stations', color='tab:pink',
                   importance=1, frame=pandas.DataFrame(dict(
                       date=mobility.date,
                       value=mob.transit_stations_percent_change_from_baseline))),
            Metric(name='workplaces', color='tab:red',
                   importance=1, frame=pandas.DataFrame(dict(
                       date=mobility.date,
                       value=mob.workplaces_percent_change_from_baseline))),
            Metric(name='residential', color='tab:brown',
                   importance=1, frame=pandas.DataFrame(dict(
                       date=mobility.date,
                       value=mob.residential_percent_change_from_baseline))),
        ]

        daily_events = []
        state_events = events_by_state.get_group(fips)
        for date, es in state_events.groupby('date'):
            events = es.sort_values(['abs_score', 'policy'], ascending=[0, 1])
            smin, smax = events.score.min(), events.score.max()
            score = 0 if smin == -smax else smin if smin < -smax else smax
            emojis = list(dict.fromkeys(
                e.emoji for e in events.itertuples() if abs(e.score) >= 2))
            daily_events.append(DailyEvents(
                date=date, score=score, emojis=emojis, events=events))

        state = us.states.lookup(fips)
        regions.append(Region(
            id=state.abbr.lower(), name=state.name, date=update_date,
            attribution=attribution, population=census.POP,
            covid_metrics=covid_metrics,
            mobility_metrics=mobility_metrics,
            daily_events=daily_events))

    return regions
