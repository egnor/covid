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

from . import cache_policy
from . import site_style
from . import fetch_cdc_mortality
from . import fetch_census_population
from . import fetch_covid_tracking
from . import fetch_state_policy

MetricData = collections.namedtuple(
    'MetricData', 'name color width frame')

DayData = collections.namedtuple(
    'PolicyData', 'date significance emojis events')

RegionData = collections.namedtuple(
    'RegionData', 'name population metrics days')

PlotData = collections.namedtuple(
    'PlotData', 'region image_url thumb_url')


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument('--state', nargs='*')
    parser.add_argument('--output_dir', default='site_out')

    args = parser.parse_args()
    select_fips = [us.states.lookup(n).fips for n in args.state or []]
    session = cache_policy.new_session(args)

    print('Reading data...')
    covid_states = fetch_covid_tracking.get_states(session=session)
    census_states = fetch_census_population.get_states(session=session)
    mortality_states = fetch_cdc_mortality.get_states(session=session)
    policy_states = fetch_state_policy.get_states(session=session)
    policy_by_state = policy_states.groupby('state_fips')

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

        def format_name(name, cap):
            def number(n):
                return numpy.format_float_positional(n, precision=3, trim='-')
            return (
                f'{name} / {number(cap / 1000000)}Mp' if cap >= 1000000 else
                f'{name} / {number(cap / 1000)}Kp' if cap >= 10000 else
                f'{name} / {number(cap)}p')

        def trend(name, color, width, date, raw, capita):
            nonzero_is, = (raw.values > 0).nonzero()
            first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(raw)
            date = date[first_i:]
            per_cap = raw[first_i:] * capita / census.POP

            frame = pandas.DataFrame(dict(
                date=date, raw=per_cap, value=per_cap.rolling(7).mean()))
            frame.set_index('date', inplace=True)
            return MetricData(format_name(name, capita), color, width, frame)

        def threshold(name, color, width, v, capita):
            frame = pandas.DataFrame(dict(value=[v * capita / census.POP]))
            return MetricData(format_name(name, capita), color, width, frame)

        covid = covid.sort_values(by='date')
        metrics = [
            trend('tests', 'tab:green', 2,
                  covid.date, covid.totalTestResultsIncrease, 1e4),
            trend('positives', 'tab:blue', 3,
                  covid.date, covid.positiveIncrease, 1e5),
            trend('hosp admit', 'tab:orange', 2,
                  covid.date, covid.hospitalizedIncrease, 25e4),
            trend('hosp current', 'tab:pink', 2,
                  covid.date, covid.hospitalizedCurrently, 25e3),
            trend('deaths', 'tab:red', 3,
                  covid.date, covid.deathIncrease, 1e6),
            threshold('baseline deaths', 'black', 2,
                      mortality.Deaths / 365, 1e6),
        ]

        days = []
        state_events = policy_by_state.get_group(fips)
        for date, date_events in state_events.groupby('date'):
            pass

        regions.append(RegionData(
            name=census.NAME, population=census.POP,
            metrics=metrics, days=days))

    if not regions:
        print('*** No data to plot!', file=sys.stderr)
        sys.exit(1)

    print('Making plots...')
    os.makedirs(args.output_dir, exist_ok=True)

    min_date = pandas.to_datetime('2020-03-01')

    max_date = max(
        m.frame.index.max() for r in regions for m in r.metrics
        if pandas.api.types.is_datetime64_any_dtype(m.frame.index))

    max_view_date = max_date + pandas.Timedelta(days=1)

    matplotlib.use('module://mplcairo.base')
    plots = []
    for region in regions:
        print(f'Plotting {region.name}...')
        figure = matplotlib.figure.Figure(figsize=(8, 8))

        axes = figure.add_subplot()
        axes.axvspan(max_date - pandas.Timedelta(weeks=2), max_view_date,
                     color='k', alpha=0.07, label='last 2 weeks')

        for i, m in enumerate(region.metrics):
            if pandas.api.types.is_datetime64_any_dtype(m.frame.index):
                if 'raw' in m.frame.columns and m.frame.raw.any():
                    axes.plot(m.frame.index, m.frame.raw,
                              color=m.color, alpha=0.5, lw=1)
                if 'value' in m.frame.columns and m.frame.value.any():
                    axes.plot(m.frame.index, m.frame.value,
                              color=m.color, lw=m.width, label=m.name)
                    axes.scatter(m.frame.index[-1:], m.frame.value.iloc[-1:],
                                 s=(m.width * 2) ** 2, c=m.color)
            else:
                axes.hlines(
                    m.frame.value, xmin=min_date, xmax=max_view_date,
                    label=m.name, color=m.color, lw=m.width, linestyle='--',
                    alpha=0.5)

        axes.grid(color='k', alpha=0.2)
        axes.set_xlim(min_date, max_view_date)
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

        figure.add_artist(matplotlib.text.Text(
            0.5, 0.5, region.name,
            ha='center', va='center', wrap=True,
            fontsize=65, fontweight='bold', alpha=0.25))
        figure.savefig(f'{args.output_dir}/{file_base}_full.png', dpi=200)

    stylesheet_url = 'style.css'
    open(f'{args.output_dir}/{stylesheet_url}', 'w').write('''
    body {font-family: 'Dejavu Sans', 'Bitstream Vera Sans', sans;}
    .thumb {display: inline-block; position: relative;}
    .thumb .thumb_label {
        position: absolute; top: 18px; width: 100%; text-align: center;
        font-size: 22px; font-weight: 900;
        color: black; opacity: 0.5;
    }
    ''')

    doc, tag, text, line = yattag.Doc().ttl()
    with tag('html'):
        with tag('head'):
            line('title', f'COVID-19 trends ({max_date.strftime("%Y-%m-%d")})')
            doc.stag('link', rel='stylesheet', href=stylesheet_url)
            site_style.add_icons_to_head(doc)
        with tag('body'):
            line('h1', f'COVID-19 trends ({max_date.strftime("%Y-%m-%d")})')
            for plot in plots:
                with tag('a', klass='thumb', href=plot.image_url):
                    doc.line('span', plot.region.name, klass='thumb_label')
                    doc.stag('img', width=200, height=200, src=plot.thumb_url)

    site_style.write_icon_files(args.output_dir)
    open(f'{args.output_dir}/index.html', 'w').write(doc.getvalue())


if __name__ == '__main__':
    main()
