#!/usr/bin/env python3

import argparse
import collections
import matplotlib.dates
import matplotlib.figure
import matplotlib.lines
import matplotlib.pyplot
import matplotlib.ticker
import numpy
import pandas
import signal
import sys
import us

import cache_policy
import fetch_cdc_mortality
import fetch_census_population
import fetch_covid_tracking


MetricData = collections.namedtuple('MetricData', 'name dates raw mean7')

RegionData = collections.namedtuple(
    'RegionData', 'name population mortality metrics')


signal.signal(signal.SIGINT, signal.SIG_DFL)
parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
parser.add_argument('--state', nargs='*')
parser.add_argument('--output_file', default='trends.pdf')

args = parser.parse_args()
select_fips = [us.states.lookup(n).fips for n in args.state or []]
session = cache_policy.new_session(args)

print('Reading data...')
covid_states = fetch_covid_tracking.get_states(session=session)
census_states = fetch_census_population.get_states(session=session)
mortality_states = fetch_cdc_mortality.get_states(session=session)

print('Merging data...')
regions = []
for fips, covid in covid_states.groupby(by='fips'):
    if select_fips and fips not in select_fips:
        continue

    try:
        census = census_states.loc[fips]
        mortality = mortality_states.loc[fips]
    except KeyError:
        continue

    def metric(name, color, date, raw, capita):
        nonzero_ilocs, = (raw.values > 0).nonzero()
        first_iloc = nonzero_ilocs[0] + 1 if len(nonzero_ilocs) else len(raw)

        date = date.iloc[first_iloc:]
        raw = raw.iloc[first_iloc:] * capita / census.POP
        frame = pandas.DataFrame(
            data=dict(date=date, raw=raw, mean7=raw.rolling(7).mean()))
        frame.set_index('date', inplace=True)

        def format(n):
            return numpy.format_float_positional(n, precision=3, trim='-')
        frame.color = color
        frame.name = (
            f'{name} / {format(capita / 1000000)}Mp' if capita >= 1000000 else
            f'{name} / {format(capita / 1000)}Kp' if capita >= 10000 else
            f'{name} / {format(capita)}p')
        return frame

    cov = covid.sort_values(by='date')
    d = cov.date
    metrics = [
        metric('tests', 'tab:green', d, cov.totalTestResultsIncrease, 1e4),
        metric('cases', 'tab:blue', d, cov.positiveIncrease, 1e5),
        metric('hosp admit', 'tab:orange', d, cov.hospitalizedIncrease, 25e4),
        metric('hosp current', 'tab:pink', d, cov.hospitalizedCurrently, 25e3),
        metric('deaths', 'tab:red', d, cov.deathIncrease, 1e6),
    ]

    regions.append(RegionData(
        name=census.NAME, population=census.POP, mortality=mortality.Deaths,
        metrics=metrics))

if not regions:
    print('*** No data to plot!', file=sys.stderr)
    sys.exit(1)

print('Plotting data...')

ymaxes = [
    max(10, 1.2 * max(m.mean7.max() for m in r.metrics))
    for r in regions]

heights = [0.08 * ym + 2 for ym in ymaxes]
figure, all_axes = matplotlib.pyplot.subplots(
    nrows=len(regions), ncols=1, sharex=True, squeeze=False,
    gridspec_kw=dict(height_ratios=heights),
    figsize=(8, sum(heights)))

start_date = pandas.to_datetime('2020-03-01')

end_date = max(
    m.index.max()
    for r in regions
    for m in r.metrics
    if m.index.notna().any()) + pandas.to_timedelta(1, unit='days')

for region, axes, ymax in zip(regions, all_axes, ymaxes):
    print(f'Plotting {region.name}...')
    axes, = axes

    for i, m in enumerate(region.metrics):
        if m.raw.notna().any():
            axes.plot(m.index, m.raw, color=m.color, alpha=0.5, lw=1)
            axes.plot(m.index, m.mean7, color=m.color, label=m.name, lw=2)

    axes.axhline(
        region.mortality * 1e6 / region.population / 365,
        label='baseline deaths / 1Mp', color='black', linestyle='--', alpha=0.5)

    axes.set_title(
        region.name, position=(0.5, 0.5), ha='center', va='center',
        fontsize=40, fontweight='bold', alpha=0.25)
    axes.legend(loc='upper left')
    axes.grid(color='g', alpha=0.2)
    axes.set_xlim(start_date, end_date)
    axes.set_ylim(0, ymax)

    month_locator = matplotlib.dates.MonthLocator()
    month_formatter = matplotlib.dates.ConciseDateFormatter(month_locator)
    week_locator = matplotlib.dates.WeekdayLocator(matplotlib.dates.SU)
    axes.xaxis.set_major_locator(month_locator)
    axes.xaxis.set_major_formatter(month_formatter)
    axes.xaxis.set_minor_locator(week_locator)
    axes.xaxis.set_tick_params(which='major', labelbottom=True)
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(10))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(1))

print('Writing plot...')
figure.tight_layout(pad=2)
figure.savefig(args.output_file, bbox_inches='tight')
print(f'Wrote plot: {args.output_file}')
