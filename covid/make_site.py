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

from . import cache_policy
from . import region_data
from . import style
from . import urls


def setup_plot(region, axes):
    min_date = pandas.to_datetime('2020-03-01')
    max_date = region.date + pandas.Timedelta(days=1)
    axes.set_xlim(min_date, max_date)
    axes.grid(c='k', alpha=0.1)

    week_locator = matplotlib.dates.WeekdayLocator(matplotlib.dates.SU)
    month_locator = matplotlib.dates.MonthLocator()
    month_formatter = matplotlib.dates.ConciseDateFormatter(month_locator)
    month_formatter.offset_formats[1] = ''  # Don't bother with year '2020'.

    axes.xaxis.set_minor_locator(week_locator)
    axes.xaxis.set_major_locator(month_locator)
    axes.xaxis.set_major_formatter(month_formatter)
    axes.xaxis.set_tick_params(which='major', labelbottom=True)

    return [axes.axvspan(
        region.date - pandas.Timedelta(weeks=2), max_date,
        color='k', alpha=0.07, zorder=0, label='last 2 weeks')]


def make_covid_plot(region, site_dir):
    matplotlib.use('module://mplcairo.base')  # For decent emoji rendering.

    figure = matplotlib.pyplot.figure(figsize=(8, 8))
    axes = figure.add_subplot()

    axes.set_ylim(0, 55)
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(5))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(1))

    legend_handles = []
    for i, m in enumerate(region.covid_metrics):
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
    for d in (d for d in region.daily_events if abs(d.score) >= 2):
        # For some reason "VARIANT SELECTOR-16" gives warnings.
        top_labels.append('\n'.join(d.emojis).replace('\uFE0F', ''))
        top_ticks.append(d.date)
        color = 'tab:orange' if d.score > 0 else 'tab:blue'
        axes.axvline(d.date, c=color, lw=2, ls='--', alpha=0.7, zorder=1)

    legend_handles.append(matplotlib.lines.Line2D(
        [], [], c='tab:blue', lw=2, ls='--', alpha=0.7,
        label='mitigation changes'))

    legend_handles.append(matplotlib.lines.Line2D(
        [], [], c='tab:orange', lw=2, ls='--', alpha=0.7,
        label='relaxation changes'))

    legend_handles.extend(setup_plot(region, axes))

    # Thumbnail version - save and remove the axis tick labels 
    x_formatter = axes.xaxis.get_major_formatter()
    axes.xaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    axes.yaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    figure.set_tight_layout(True)
    figure.savefig(urls.file(site_dir, urls.covid_plot_thumb(region)), dpi=50)

    # Full version - restore and install axis tick labels and legend
    axes.legend(handles=legend_handles, loc='upper left')
    axes.xaxis.set_major_formatter(x_formatter)
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    top = axes.secondary_xaxis('top')
    top.set_frame_on(False)
    top.set_xticks(top_ticks)
    top.set_xticklabels(
        top_labels, font=pathlib.Path(__file__).parent / 'NotoColorEmoji.ttf',
        fontsize=15, linespacing=1.1)

    figure.add_artist(matplotlib.text.Text(
        0.5, 0.5, region.name,
        ha='center', va='center', wrap=True,
        fontsize=65, fontweight='bold', alpha=0.2))

    figure.savefig(urls.file(site_dir, urls.covid_plot(region)), dpi=200)

    # Explicit closure is required to reclaim memory.
    matplotlib.pyplot.close(figure)


def make_mobility_plot(region, site_dir):
    figure = matplotlib.pyplot.figure(figsize=(8, 4))
    axes = figure.add_subplot()

    axes.set_ylim(50, 150)
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(10))
    # axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(1))

    legend_handles = []
    for i, m in enumerate(region.covid_metrics):
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
    for d in (d for d in region.daily_events if abs(d.score) >= 2):
        # For some reason "VARIANT SELECTOR-16" gives warnings.
        top_labels.append('\n'.join(d.emojis).replace('\uFE0F', ''))
        top_ticks.append(d.date)
        color = 'tab:orange' if d.score > 0 else 'tab:blue'
        axes.axvline(d.date, c=color, lw=2, ls='--', alpha=0.7, zorder=1)

    legend_handles.append(matplotlib.lines.Line2D(
        [], [], c='tab:blue', lw=2, ls='--', alpha=0.7,
        label='mitigation changes'))

    legend_handles.append(matplotlib.lines.Line2D(
        [], [], c='tab:orange', lw=2, ls='--', alpha=0.7,
        label='relaxation changes'))

    legend_handles.extend(setup_plot(region, axes))

    # Thumbnail version - save and remove the axis tick labels 
    x_formatter = axes.xaxis.get_major_formatter()
    axes.xaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    axes.yaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    figure.set_tight_layout(True)
    figure.savefig(urls.file(site_dir, urls.covid_plot_thumb(region)), dpi=50)

    # Full version - restore and install axis tick labels and legend
    axes.legend(handles=legend_handles, loc='upper left')
    axes.xaxis.set_major_formatter(x_formatter)
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    top = axes.secondary_xaxis('top')
    top.set_frame_on(False)
    top.set_xticks(top_ticks)
    top.set_xticklabels(
        top_labels, font=pathlib.Path(__file__).parent / 'NotoColorEmoji.ttf',
        fontsize=15, linespacing=1.1)

    figure.add_artist(matplotlib.text.Text(
        0.5, 0.5, region.name,
        ha='center', va='center', wrap=True,
        fontsize=65, fontweight='bold', alpha=0.2))

    figure.savefig(urls.file(site_dir, urls.covid_plot(region)), dpi=200)

    # Explicit closure is required to reclaim memory.
    matplotlib.pyplot.close(figure)



def make_home(regions, site_dir):
    date = max(r.date for r in regions)
    title = f'US COVID-19 trends ({date.strftime("%Y-%m-%d")})'
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
                         src=urls.link(doc_url, urls.covid_plot_thumb(r)))

    with open(urls.file(site_dir, doc_url), 'w') as doc_file:
        doc_file.write(doc.render())


def make_region_page(region, site_dir):
    title = f'{region.name} COVID-19 ({region.date.strftime("%Y-%m-%d")})'
    doc = dominate.document(title=title)
    doc_url = urls.region_page(region)
    with doc.head:
        style.add_head_style(doc_url)

    with doc.body:
        tags.img(cls='plot', src=urls.link(doc_url, urls.covid_plot(region)))

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


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument('--state', nargs='*')
    parser.add_argument('--site_dir', type=pathlib.Path,
                        default=pathlib.Path('site_out'))
    args = parser.parse_args()

    print('Reading data...')
    session = cache_policy.new_session(args)
    regions = region_data.get_regions(session, args.state)
    if not regions:
        print('*** No data to plot!', file=sys.stderr)
        sys.exit(1)

    print(f'Making plots in {args.site_dir}...')
    for region in regions:
        print(f'  {region.name}...')
        make_covid_plot(region, args.site_dir)
        make_region_page(region, args.site_dir)

    print(f'Writing HTML in {args.site_dir}...')
    style.write_style_files(args.site_dir)
    make_home(regions, args.site_dir)


if __name__ == '__main__':
    main()
