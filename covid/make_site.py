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
from dominate import tags, util

from covid import cache_policy
from covid import combine_data
from covid import style
from covid import urls


def setup_plot_xaxis(axes, end_date, title=None, titlesize=65):
    """Sets common X axis and plot style."""

    min_date = pandas.to_datetime('2020-03-01')
    max_date = end_date + pandas.Timedelta(days=1)
    axes.set_xlim(min_date, max_date)
    axes.grid(color='black', alpha=0.1)

    week_locator = matplotlib.dates.WeekdayLocator(matplotlib.dates.SU)
    month_locator = matplotlib.dates.MonthLocator()
    month_formatter = matplotlib.dates.ConciseDateFormatter(month_locator)
    month_formatter.offset_formats[1] = ''  # Don't bother with year '2020'.

    axes.xaxis.set_minor_locator(week_locator)
    axes.xaxis.set_major_locator(month_locator)
    axes.xaxis.set_major_formatter(month_formatter)
    axes.xaxis.set_tick_params(which='major', labelbottom=True)

    if title:
        axes.text(
            0.5, 0.5, title, transform=axes.transAxes,
            ha='center', va='center', wrap=True,
            fontsize=titlesize, fontweight='bold', alpha=0.2)


def add_plot_legend(axes, legend_artists):
    """Adds a standard style plot legend using collected legend artists."""

    xmin, xmax = axes.get_xlim()
    legend_artists.append(axes.axvspan(
        xmax - 14, xmax, color='k', alpha=.07, zorder=0, label='last 2 weeks'))
    axes.legend(
        handles=legend_artists, loc='center left', bbox_to_anchor=(1, 0.5))


def plot_covid_metrics(axes, covid_metrics):
    """Plots COVID case-related metrics. Returns a list of legend artists."""

    axes.set_ylim(0, 55)
    axes.set_ylabel('number per capita')
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(5))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(1))

    legend_artists = []
    for i, (name, m) in enumerate(covid_metrics.items()):
        width = 4 if m.emphasis >= 1 else 2
        style = '-' if m.emphasis >= 0 else '--'
        alpha = 1.0 if m.emphasis >= 0 else 0.5
        if 'raw' in m.frame.columns and m.frame.raw.any():
            axes.plot(m.frame.index, m.frame.raw,
                      c=m.color, alpha=alpha * 0.5, lw=1, ls=style)
        if 'value' in m.frame.columns and m.frame.value.any():
            axes.scatter(m.frame.index[-1:], m.frame.value.iloc[-1:],
                         c=m.color, alpha=alpha,
                         s=(width * 2) ** 2, zorder=3)
            legend_artists.extend(axes.plot(
                m.frame.index, m.frame.value, label=name,
                c=m.color, alpha=alpha, lw=width, ls=style))

    return legend_artists


def plot_mobility_metrics(axes, mobility_metrics):
    axes.set_ylim(0, 250)
    axes.set_ylabel('% of same weekday in January')
    axes.axhline(100, c='black', lw=1)  # Identity line.
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(50))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(10))
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())

    legend_artists = []
    for i, (name, m) in enumerate(mobility_metrics.items()):
        if 'raw' in m.frame.columns and m.frame.raw.any():
            axes.plot(
                m.frame.index, m.frame.raw + 100, c=m.color, alpha=0.5, lw=1)
        if 'value' in m.frame.columns and m.frame.value.any():
            week_ago = m.frame.index[-1] - pandas.Timedelta(days=7)
            older, newer = m.frame.loc[:week_ago], m.frame.loc[week_ago:]
            legend_artists.extend(axes.plot(
                older.index, older.value + 100, label=name, c=m.color, lw=2))
            axes.plot(newer.index, newer.value + 100, c=m.color, lw=2, ls=':')

    return legend_artists


def plot_daily_events(axes, daily_events, with_emoji=True):
    """Plots important policy changes. Returns a list of legend artists."""

    legend_artists = []
    top_ticks, top_labels = [], []
    for d in (d for d in daily_events if abs(d.score) >= 2):
        if with_emoji:
            # For some reason "VARIANT SELECTOR-16" gives warnings.
            top_labels.append('\n'.join(d.emojis).replace('\uFE0F', ''))
            top_ticks.append(d.date)

        color = 'tab:orange' if d.score > 0 else 'tab:blue'
        axes.axvline(d.date, c=color, lw=2, ls='--', alpha=0.7, zorder=1)

    legend_artists.append(matplotlib.lines.Line2D(
        [], [], c='tab:blue', lw=2, ls='--', alpha=0.7,
        label='mitigation changes'))

    legend_artists.append(matplotlib.lines.Line2D(
        [], [], c='tab:orange', lw=2, ls='--', alpha=0.7,
        label='relaxation changes'))

    if top_ticks and top_labels:
        top = axes.secondary_xaxis('top')
        top.set_xticks(top_ticks)
        top.set_xticklabels(
            top_labels, fontdict=dict(fontsize=15), linespacing=1.1,
            font=pathlib.Path(__file__).parent / 'NotoColorEmoji.ttf')

    return legend_artists


def make_home(regions, site_dir):
    """Write site home with thumbnail links to lots of regions."""

    max_date = max(
        max(m.frame.index.max() for m in r.covid_metrics.values())
        for r in regions)

    title = f'US COVID-19 trends ({max_date.strftime("%Y-%m-%d")})'
    doc = dominate.document(title=title)
    doc_url = urls.index_page()
    with doc.head:
        style.add_head_style(doc_url)

    with doc.body:
        tags.h1(title)
        for r in regions:
            with tags.a(cls='thumb',
                        href=urls.link(doc_url, urls.region_page(r))):
                tags.span(r.name, cls='thumb_label')
                tags.img(width=200, height=200,
                         src=urls.link(doc_url, urls.thumb_image(r)))

    with open(urls.file(site_dir, doc_url), 'w') as doc_file:
        doc_file.write(doc.render())


def make_region_tree(region, site_dir):
    """Write region-specific page along with subregions."""

    # Recurse for subregions.
    for r in region.subregions.values():
        make_region_tree(r, site_dir)

    def get_nesting(region):
        return get_nesting(region.parent) + [region] if region else []
    nesting = get_nesting(region)
    print(f'Make: {"/".join(r.short_name for r in nesting)}')

    # Write HTML
    max_date = max(m.frame.index.max() for m in region.covid_metrics.values())
    title = f'{region.name} COVID-19 ({max_date.strftime("%Y-%m-%d")})'
    doc = dominate.document(title=title)
    doc_url = urls.region_page(region)
    with doc.head:
        style.add_head_style(doc_url)

    with doc.body:
        tags.h1(title)

        tags.img(cls='plot', src=urls.link(doc_url, urls.plot_image(region)))

        with tags.h2():
            tags.span('Mitigation', cls='event_close')
            util.text(' and ')
            tags.span('Relaxation', cls='event_open')
            util.text(' Changes')

        with tags.div(cls='events'):
            def score_css(s):
                return f'event_{"open" if s > 0 else "close"} score_{abs(s)}'
            for day in (d for d in region.daily_events if d.score):
                date = day.date.strftime('%Y-%m-%d')
                tags.div(date, cls=f'event_date {score_css(day.score)}')
                for event in (e for e in day.events.itertuples() if e.score):
                    score = score_css(event.score)
                    tags.div(event.emoji, cls=f'event_emoji {score}')
                    tags.div(event.policy, cls=f'event_policy {score}')

        with tags.p('Sources: ', cls='attribution'):
            for i, (url, text) in enumerate(region.attribution.items()):
                util.text(', ') if i > 0 else None
                tags.a(text, href=url)

    with open(urls.file(site_dir, doc_url), 'w') as doc_file:
        doc_file.write(doc.render())

    # Make plot image for page
    figure = matplotlib.pyplot.figure(figsize=(10, 12), tight_layout=True)
    covid_axes, mobility_axes = figure.subplots(
        nrows=2, ncols=1, sharex=True, gridspec_kw=dict(height_ratios=[8, 4]))

    setup_plot_xaxis(covid_axes, max_date, title=region.name)

    add_plot_legend(
        covid_axes,
        plot_covid_metrics(covid_axes, region.covid_metrics) +
        plot_covid_metrics(covid_axes, region.baseline_metrics) +
        plot_daily_events(covid_axes, region.daily_events))

    setup_plot_xaxis(mobility_axes, max_date,
                     title=f'{region.short_name} mobility', titlesize=45)

    add_plot_legend(
        mobility_axes,
        plot_mobility_metrics(mobility_axes, region.mobility_metrics) +
        plot_daily_events(mobility_axes, region.daily_events, with_emoji=0))

    figure.savefig(urls.file(site_dir, urls.plot_image(region)), dpi=200)
    matplotlib.pyplot.close(figure)  # Reclaim memory.

    # Make thumbnail for index page
    figure = matplotlib.pyplot.figure(figsize=(8, 8), tight_layout=True)
    thumb_axes = figure.add_subplot()
    setup_plot_xaxis(thumb_axes, max_date)
    plot_covid_metrics(thumb_axes, region.baseline_metrics)
    plot_covid_metrics(thumb_axes, region.covid_metrics)
    plot_daily_events(thumb_axes, region.daily_events, with_emoji=False)
    thumb_axes.set_xlabel(None)
    thumb_axes.set_ylabel(None)
    thumb_axes.xaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    thumb_axes.yaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    figure.savefig(urls.file(site_dir, urls.thumb_image(region)), dpi=50)
    matplotlib.pyplot.close(figure)  # Reclaim memory.


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument('--filter_regex')
    parser.add_argument('--site_dir', type=pathlib.Path,
                        default=pathlib.Path('site_out'))
    args = parser.parse_args()

    print('Loading data...')
    world = combine_data.get_world(
        session=cache_policy.new_session(args),
        filter_regex=args.filter_regex)

    print(f'Generating pages in {args.site_dir}...')
    matplotlib.use('module://mplcairo.base')  # For decent emoji rendering.
    style.write_style_files(args.site_dir)
    make_region_tree(world, args.site_dir)


if __name__ == '__main__':
    main()
