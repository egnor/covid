#!/usr/bin/env python3

import argparse
import collections
import matplotlib.dates
import matplotlib.figure
import matplotlib.lines
import matplotlib.pyplot
import matplotlib.ticker
import numpy
import os
import pandas
import signal
import sys
import urllib.parse
import us
import yattag

import cache_policy
import fetch_cdc_mortality
import fetch_census_population
import fetch_covid_tracking

MetricData = collections.namedtuple('MetricData', 'name color frame')

RegionData = collections.namedtuple('RegionData', 'name population metrics')

PlotData = collections.namedtuple('PlotData', 'region image_url thumb_url')


signal.signal(signal.SIGINT, signal.SIG_DFL)
parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
parser.add_argument('--state', nargs='*')
parser.add_argument('--output_dir', default='trends_out')

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

    def name_with_capita(name, capita):
        def number(n):
            return numpy.format_float_positional(n, precision=3, trim='-')
        return (
            f'{name} / {number(capita / 1000000)}Mp' if capita >= 1000000 else
            f'{name} / {number(capita / 1000)}Kp' if capita >= 10000 else
            f'{name} / {number(capita)}p')

    def trend(name, color, date, raw, capita):
        nonzero_ilocs, = (raw.values > 0).nonzero()
        first_iloc = nonzero_ilocs[0] + 1 if len(nonzero_ilocs) else len(raw)
        date = date[first_iloc:]
        per_cap = raw[first_iloc:] * capita / census.POP

        frame = pandas.DataFrame(dict(
            date=date, raw=per_cap, value=per_cap.rolling(7).mean()))
        frame.set_index('date', inplace=True)
        return MetricData(name_with_capita(name, capita), color, frame)

    def threshold(name, color, v, capita):
        frame = pandas.DataFrame(dict(value=[v * capita / census.POP]))
        return MetricData(name_with_capita(name, capita), color, frame)

    cov = covid.sort_values(by='date')
    d = cov.date
    metrics = [
        trend('tests', 'tab:green', d, cov.totalTestResultsIncrease, 1e4),
        trend('cases', 'tab:blue', d, cov.positiveIncrease, 1e5),
        trend('hosp admit', 'tab:orange', d, cov.hospitalizedIncrease, 25e4),
        trend('hosp current', 'tab:pink', d, cov.hospitalizedCurrently, 25e3),
        trend('deaths', 'tab:red', d, cov.deathIncrease, 1e6),
        threshold('baseline deaths', 'black', mortality.Deaths / 365, 1e6),
    ]

    regions.append(RegionData(
        name=census.NAME, population=census.POP, metrics=metrics))

if not regions:
    print('*** No data to plot!', file=sys.stderr)
    sys.exit(1)

print('Making plots...')
os.makedirs(args.output_dir, exist_ok=True)

min_date = pandas.to_datetime('2020-03-01')
max_date = pandas.to_timedelta(1, unit='days') + max(
    m.frame.index.max() for r in regions for m in r.metrics
    if pandas.api.types.is_datetime64_any_dtype(m.frame.index))

plots = []
for region in regions:
    print(f'Plotting {region.name}...')
    figure = matplotlib.figure.Figure(figsize=(8, 8))

    axes = figure.add_subplot()
    for i, m in enumerate(region.metrics):
        if pandas.api.types.is_datetime64_any_dtype(m.frame.index):
            if 'raw' in m.frame.columns and m.frame.raw.any():
                axes.plot(m.frame.index, m.frame.raw,
                          color=m.color, alpha=0.5, lw=1)
            if 'value' in m.frame.columns and m.frame.value.any():
                axes.plot(m.frame.index, m.frame.value,
                          color=m.color, label=m.name, lw=2)
        else:
            axes.hlines(
                m.frame.value, xmin=min_date, xmax=max_date,
                label=m.name, color=m.color, linestyle='--', alpha=0.5)

    axes.grid(color='g', alpha=0.2)
    axes.set_xlim(min_date, max_date)
    axes.set_ylim(0, 50)

    month_locator = matplotlib.dates.MonthLocator()
    week_locator = matplotlib.dates.WeekdayLocator(matplotlib.dates.SU)
    axes.xaxis.set_major_locator(month_locator)
    axes.xaxis.set_minor_locator(week_locator)
    axes.xaxis.set_tick_params(which='major', labelbottom=True)
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(5))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(1))

    file_base = region.name.lower().replace(' ', '_').replace('/', '.')
    plot = PlotData(
        region=region, 
        image_url=f'{urllib.parse.quote(file_base)}_full.png',
        thumb_url=f'{urllib.parse.quote(file_base)}_thumb.png')
    plots.append(plot)

    # Thumbnail version
    axes.xaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    axes.yaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    figure.set_tight_layout(True)
    figure.savefig(f'{args.output_dir}/{file_base}_thumb.png', dpi=50)

    # Full version
    axes.legend(loc='upper left')
    month_formatter = matplotlib.dates.ConciseDateFormatter(month_locator)
    axes.xaxis.set_major_formatter(month_formatter)
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())

    # TODO - use Roboto instead of default font for consistency
    figure.add_artist(matplotlib.text.Text(
        0.5, 0.5, region.name,
        ha='center', va='center', wrap=True,
        fontsize=65, fontweight='bold', alpha=0.25))
    figure.savefig(f'{args.output_dir}/{file_base}_full.png', dpi=200)

stylesheet_url = 'style.css'
open(f'{args.output_dir}/{stylesheet_url}', 'w').write('''
body {font-family: 'Roboto', sans;}
.thumb {display: inline-block; position: relative;}
.thumb .thumb_label {
    position: absolute; top: 18px; width: 100%; text-align: center;
    font-size: 24px; font-weight: 900;
    color: black; opacity: 0.5;
}
''')

doc, tag, text, line = yattag.Doc().ttl()
with tag('html'):
    with tag('head'):
        line('title', f'COVID-19 trends ({max_date.strftime("%Y-%m-%d")})')
        doc.stag('link', rel='stylesheet', href=stylesheet_url)
        doc.stag('link', rel='stylesheet',
                 href='https://fonts.googleapis.com/css?family=Roboto')
    with tag('body'):
        line('h1', f'COVID-19 trends ({max_date.strftime("%Y-%m-%d")})')
        for plot in plots:
            with tag('a', klass='thumb', href=plot.image_url):
                doc.line('span', plot.region.name, klass='thumb_label')
                doc.stag('img', width=200, height=200, src=plot.thumb_url)

open(f'{args.output_dir}/index.html', 'w').write(doc.getvalue())
