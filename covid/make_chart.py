"""Functions that generate trend charts from region metrics."""

import collections
import os
import pathlib

import matplotlib
import matplotlib.dates
import matplotlib.lines
import matplotlib.pyplot
import matplotlib.ticker
import pandas

from covid import urls

matplotlib.use('module://mplcairo.base')  # For decent emoji rendering.


def write_thumb_image(region, site_dir):
    # Make thumbnail for index page
    phi = (1 + 5 ** 0.5) / 2  # Nice pleasing aspect ratio.
    figure = matplotlib.pyplot.figure(figsize=(8, 8 / phi), tight_layout=True)
    thumb_axes = figure.add_subplot()
    _setup_xaxis(thumb_axes, region)
    thumb_axes.set_ylim(0, 50)
    _plot_covid_metrics(thumb_axes, region.baseline_metrics)
    _plot_covid_metrics(thumb_axes, region.covid_metrics)
    _plot_daily_events(thumb_axes, region.daily_events, emoji=False)
    thumb_axes.set_xlabel(None)
    thumb_axes.set_ylabel(None)
    thumb_axes.xaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    thumb_axes.yaxis.set_major_formatter(matplotlib.ticker.NullFormatter())
    figure.savefig(urls.file(site_dir, urls.thumb_image(region)), dpi=50)
    matplotlib.pyplot.close(figure)  # Reclaim memory.


def write_image(region, site_dir):
    covid_max = max(m.frame.value.max() for m in region.covid_metrics.values())
    covid_max = min(200, max(30, (covid_max // 10 + 1) * 10))
    covid_height = covid_max / 10

    if region.mobility_metrics:
        figure = matplotlib.pyplot.figure(
            figsize=(10, covid_height + 4), tight_layout=True)
        covid_axes, mobility_axes = figure.subplots(
            nrows=2, ncols=1, sharex=True,
            gridspec_kw=dict(height_ratios=[covid_height, 4]))
    else:
        figure = matplotlib.pyplot.figure(
            figsize=(10, covid_height), tight_layout=True)
        covid_axes, mobility_axes = figure.add_subplot(), None

    covid_axes.set_ylim(0, covid_max)
    _setup_xaxis(covid_axes, region, title=f'{region.short_name} COVID')
    _plot_covid_metrics(covid_axes, region.covid_metrics)
    _plot_covid_metrics(covid_axes, region.baseline_metrics)
    _plot_daily_events(covid_axes, region.daily_events, emoji=True)
    _plot_subregion_peaks(covid_axes, region)
    _add_plot_legend(covid_axes)

    if mobility_axes:
        _setup_xaxis(
            mobility_axes, region,
            title=f'{region.short_name} mobility')
        _plot_mobility_metrics(mobility_axes, region.mobility_metrics)
        _plot_daily_events(mobility_axes, region.daily_events, emoji=False)
        _add_plot_legend(mobility_axes)

    figure.savefig(urls.file(site_dir, urls.chart_image(region)), dpi=200)
    matplotlib.pyplot.close(figure)  # Reclaim memory.


def _plot_subregion_peaks(axes, region):
    (xmin, xmax), (ymin, ymax) = axes.get_xlim(), axes.get_ylim()
    rgb = matplotlib.colors.to_rgb('tab:blue')
    xs, ys, cs, ts = [], [], [], []
    max_p = max((s.population for s in region.subregions.values()), default=1)
    for sub in sorted(region.subregions.values(), key=lambda r: -r.population):
        m = sub.covid_metrics.get('positives / 100Kp')
        if m and m.peak and matplotlib.dates.date2num(m.peak[0]) >= xmin:
            xs.append(m.peak[0])
            ys.append(min(m.peak[1], ymax))
            cs.append(rgb + (max(0.2, (sub.population / max_p) ** 0.5),))
            ts.append(sub.short_name.replace(' ', '')[:3])
    if xs:
        _add_to_legend(axes, axes.scatter(
            xs, ys, c=cs, marker=6, label='subdiv peak positives'), order=+10)
        for x, y, c, t in zip(xs, ys, cs, ts):
            axes.annotate(
                t, c=c, xy=(x, y), ha='center', va='top',
                xytext=(0, -15), textcoords='offset pixels')


def _plot_covid_metrics(axes, covid_metrics):
    """Plots COVID case-related metrics."""

    # (This function does not set ylim.)
    axes.set_ylabel('number per capita')
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(5))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(1))

    for name, metric in covid_metrics.items():
        _plot_metric(axes, name, metric)


def _plot_mobility_metrics(axes, mobility_metrics):
    """Plots metrics of population mobility."""

    axes.set_ylim(0, 250)
    axes.set_ylabel('% of same weekday in January')
    axes.axhline(100, c='black', lw=1)  # Identity line.
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(50))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(10))
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())

    for name, metric in mobility_metrics.items():
        _plot_metric(axes, name, metric)


def _plot_metric(axes, name, metric):
    width = 4 if metric.emphasis >= 1 else 2
    style = '-' if metric.emphasis >= 0 else '--'
    alpha = 1.0 if metric.emphasis >= 0 else 0.5
    if 'raw' in metric.frame.columns and metric.frame.raw.any():
        axes.plot(metric.frame.index, metric.frame.raw,
                  c=metric.color, alpha=alpha * 0.5, lw=1, ls=style)
    if 'value' in metric.frame.columns and metric.frame.value.any():
        last_date = metric.frame.value.last_valid_index()
        axes.scatter(
            [last_date], [metric.frame.value.loc[last_date]],
            c=metric.color, alpha=alpha, s=(width * 2) ** 2, zorder=3)
        _add_to_legend(axes, *axes.plot(
            metric.frame.index, metric.frame.value, label=name,
            c=metric.color, alpha=alpha, lw=width, ls=style))


def _plot_daily_events(axes, daily_events, emoji):
    """Plots important policy changes. Returns a list of legend artists."""

    top_ticks, top_labels = [], []
    for d in (d for d in daily_events if abs(d.score) >= 2):
        if emoji:
            # For some reason "VARIANT SELECTOR-16" gives warnings.
            top_labels.append('\n'.join(d.emojis).replace('\uFE0F', ''))
            top_ticks.append(d.date)

        color = 'tab:orange' if d.score > 0 else 'tab:blue'
        axes.axvline(d.date, c=color, lw=2, ls='--', alpha=0.7, zorder=1)

    if [d for d in daily_events if d.score <= 0]:
        _add_to_legend(axes, matplotlib.lines.Line2D(
            [], [], c='tab:blue', lw=2, ls='--', alpha=0.7,
            label='mitigation changes'))

    if [d for d in daily_events if d.score > 0]:
        _add_to_legend(axes, matplotlib.lines.Line2D(
            [], [], c='tab:orange', lw=2, ls='--', alpha=0.7,
            label='relaxation changes'))

    if top_ticks and top_labels:
        top = axes.secondary_xaxis('top')
        top.set_xticks(top_ticks)
        top.set_xticklabels(
            top_labels, fontdict=dict(fontsize=15), linespacing=1.1,
            font=pathlib.Path(__file__).parent / 'NotoColorEmoji.ttf')


def _setup_xaxis(axes, region, title=None, titlesize=45):
    """Sets common X axis and plot style."""

    end = max(m.frame.index.max() for m in region.covid_metrics.values())
    xmin, xmax = pandas.Timestamp(2020, 3, 1), end + pandas.Timedelta(days=1)
    axes.set_xlim(xmin, xmax)
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
            0.5, 0.5, '\n'.join(title.split()), transform=axes.transAxes,
            ha='center', va='center', wrap=True,
            fontsize=titlesize, fontweight='bold', alpha=0.2)

    _add_to_legend(axes, axes.axvspan(
        xmax - pandas.Timedelta(weeks=2), xmax, color='k', alpha=.07, zorder=0,
        label='last 2 weeks'), order=+5)


def _add_to_legend(axes, *artists, order=0):
    """Returns our user defined legend artist list attached to plot axes."""

    order_artists = axes.__dict__.setdefault('covid_legend_artists', {})
    order_artists.setdefault(order, []).extend(artists)


def _add_plot_legend(axes):
    """Adds a standard plot legend using the legend_artists(axes) list."""

    order_artists = axes.__dict__.get('covid_legend_artists', {})
    axes.legend(
        loc='center left', bbox_to_anchor=(1, 0.5),
        handles=[
            artist for order, artists in sorted(order_artists.items())
            for artist in artists])