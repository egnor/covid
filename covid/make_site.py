import argparse
import collections
import os
import pathlib
import signal
import sys
import textwrap

import dominate
import matplotlib
import matplotlib.dates
import matplotlib.figure
import matplotlib.lines
import matplotlib.pyplot
import matplotlib.ticker
import numpy
import pandas
import us
from dominate import tags

from . import cache_policy
from . import fetch_cdc_mortality
from . import fetch_census_population
from . import fetch_covid_tracking
from . import fetch_state_policy
from . import region_data
from . import style
from . import urls


def compute_regions(session, select_states):
    print('Reading data...')
    select_fips = [us.states.lookup(n).fips for n in select_states or []]

    covid_states = fetch_covid_tracking.get_states(session=session)
    census_states = fetch_census_population.get_states(session=session)
    mortality_states = fetch_cdc_mortality.get_states(session=session)
    policy_events = fetch_state_policy.get_events(session=session)

    attribution = {}
    attribution.update(fetch_covid_tracking.attribution())
    attribution.update(fetch_census_population.attribution())
    attribution.update(fetch_cdc_mortality.attribution())
    attribution.update(fetch_state_policy.attribution())

    print('Merging data...')
    update_date = covid_states.date.max()
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
        metrics = [
            region_data.trend(
                'tests', 'tab:green', 0, covid.date,
                covid.totalTestResultsIncrease, 1e4, census.POP),
            region_data.trend(
                'positives', 'tab:blue', 1, covid.date,
                covid.positiveIncrease, 1e5, census.POP),
            region_data.trend(
                'hosp admit', 'tab:orange', 0, covid.date,
                covid.hospitalizedIncrease, 25e4, census.POP),
            region_data.trend(
                'hosp current', 'tab:pink', 0, covid.date,
                covid.hospitalizedCurrently, 25e3, census.POP),
            region_data.trend(
                'deaths', 'tab:red', 1, covid.date,
                covid.deathIncrease, 1e6, census.POP),
            region_data.threshold(
                'historical deaths', 'black', -1,
                mortality.Deaths / 365, 1e6, census.POP),
        ]

        days = []
        state_events = events_by_state.get_group(fips)
        for date, events in state_events.groupby('date'):
            min_s, max_s = events.significance.min(), events.significance.max()
            sig = 0 if min_s == -max_s else min_s if min_s < -max_s else max_s
            emojis = list(sorted(set(
                e.emoji for e in events.itertuples()
                if e.significance == sig)))
            days.append(region_data.DayData(
                date=date, significance=sig, emojis=emojis, events=events))

        state = us.states.lookup(fips)
        regions.append(region_data.RegionData(
            id=state.abbr.lower(), name=state.name,
            population=census.POP, metrics=metrics, days=days,
            date=update_date, attribution=attribution))

    return regions


def make_plot(region, site_dir):
    print(f'Plotting {region.name}...')
    matplotlib.use('module://mplcairo.base')  # For decent emoji rendering.

    figure = matplotlib.pyplot.figure(figsize=(8, 8))
    axes = figure.add_subplot()
    legend_handles = []

    min_date = pandas.to_datetime('2020-03-01')
    max_date = region.date + pandas.Timedelta(days=1)

    for i, m in enumerate(region.metrics):
        width = 4 if m.importance >= 1 else 2
        style = '-' if m.importance >= 0 else '--'
        alpha = 1.0 if m.importance >= 0 else 0.5
        if 'raw' in m.frame.columns and m.frame.raw.any():
            axes.plot(m.frame.index, m.frame.raw,
                      c=m.color, alpha=alpha * 0.5, lw=1, ls=style)
        if 'value' in m.frame.columns and m.frame.value.any():
            axes.scatter(m.frame.index[-1:], m.frame.value.iloc[-1:],
                         c=m.color, alpha=alpha,
                         s=(width * 2) ** 2, zorder=3)
            legend_handles.extend(axes.plot(
                m.frame.index, m.frame.value, label=m.name,
                c=m.color, alpha=alpha, lw=width, ls=style))

    top_ticks, top_labels = [], []
    for d in (d for d in region.days if abs(d.significance) >= 2):
        # For some reason "VARIANT SELECTOR-16" gives warnings.
        top_labels.append('\n'.join(d.emojis).replace('\uFE0F', ''))
        top_ticks.append(d.date)
        color = 'tab:orange' if d.significance > 0 else 'tab:blue'
        axes.axvline(d.date, c=color, lw=2, ls='--', alpha=0.7, zorder=1)
        axes.add_line(matplotlib.lines.Line2D(
            [-0.05, 0, 0.05],
            [v if d.significance > 0 else -v for v in [-.03, 0.07, -.03]],
            color=color, lw=2, alpha=0.7, zorder=1, transform=(
                figure.dpi_scale_trans +
                matplotlib.transforms.ScaledTranslation(
                    matplotlib.dates.date2num(d.date), 0.95,
                    axes.get_xaxis_transform()))))

    legend_handles.append(matplotlib.lines.Line2D(
        [], [], c='tab:blue', lw=2, ls='--', alpha=0.7,
        label='mitigation orders'))

    legend_handles.append(matplotlib.lines.Line2D(
        [], [], c='tab:orange', lw=2, ls='--', alpha=0.7,
        label='reopening orders'))

    legend_handles.append(axes.axvspan(
        region.date - pandas.Timedelta(weeks=2), max_date,
        color='k', alpha=0.07, zorder=0, label='last 2 weeks'))

    axes.grid(c='k', alpha=0.2)
    axes.set_xlim(min_date, max_date)
    axes.set_ylim(0, 50)

    month_locator = matplotlib.dates.MonthLocator()
    week_locator = matplotlib.dates.WeekdayLocator(matplotlib.dates.SU)
    axes.xaxis.set_major_locator(month_locator)
    axes.xaxis.set_minor_locator(week_locator)
    axes.xaxis.set_tick_params(which='major', labelbottom=True)
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(5))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(1))

    # Thumbnail version
    axes.xaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    axes.yaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    figure.set_tight_layout(True)
    figure.savefig(urls.file(site_dir, urls.region_thumb(region)), dpi=50)

    # Full version
    axes.legend(handles=legend_handles, loc='upper left')
    month_formatter = matplotlib.dates.ConciseDateFormatter(month_locator)
    axes.xaxis.set_major_formatter(month_formatter)
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    top = axes.secondary_xaxis('top')
    top.set_xticks(top_ticks)
    top.set_xticklabels(
        top_labels, font=pathlib.Path(__file__).parent / 'NotoColorEmoji.ttf',
        fontsize=15, linespacing=1.1)

    figure.add_artist(matplotlib.text.Text(
        0.5, 0.5, region.name,
        ha='center', va='center', wrap=True,
        fontsize=65, fontweight='bold', alpha=0.25))
    figure.savefig(urls.file(site_dir, urls.region_plot(region)), dpi=200)

    # Explicit closure is required to reclaim memory.
    matplotlib.pyplot.close(figure)


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument('--state', nargs='*')
    parser.add_argument('--site_dir', type=pathlib.Path,
                        default=pathlib.Path('site_out'))

    args = parser.parse_args()
    session = cache_policy.new_session(args)
    regions = compute_regions(session, args.state)
    if not regions:
        print('*** No data to plot!', file=sys.stderr)
        sys.exit(1)

    print(f'Making plots in {args.site_dir}...')
    for region in regions:
        make_plot(region, args.site_dir)

    print(f'Writing HTML in {args.site_dir}...')
    style.write_style_files(args.site_dir)

    home_url = urls.home_page()
    title = f'COVID-19 trends ({regions[0].date.strftime("%Y-%m-%d")})'
    doc = dominate.document(title=title)
    doc_url = urls.home_page()

    with doc.head:
        style.add_head_style(doc_url)

    with doc.body:
        tags.h1(title)
        for r in regions:
            with tags.a(cls='thumb',
                        href=urls.link(doc_url, urls.region_plot(r))):
                tags.span(r.name, cls='thumb_label')
                tags.img(width=200, height=200,
                         src=urls.link(doc_url, urls.region_thumb(r)))

    with open(urls.file(args.site_dir, doc_url), 'w') as doc_file:
        doc_file.write(doc.render())


if __name__ == '__main__':
    main()
