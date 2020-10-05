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

matplotlib.rcParams.update({'figure.max_open_warning': 0})


def write_images(region, site_dir):
    _write_thumb_image(region, site_dir)
    _write_chart_image(region, site_dir)


def _write_thumb_image(region, site_dir):
    # Make thumbnail for index page
    p = (1 + 5 ** 0.5) / 2  # Nice pleasing aspect ratio.
    fig = matplotlib.pyplot.figure(figsize=(8, 8 / p), dpi=50)
    thumb_axes = fig.add_subplot()
    _setup_xaxis(thumb_axes, region)
    thumb_axes.set_ylim(0, 50)
    _plot_covid_metrics(thumb_axes, region.covid_metrics)
    _plot_policy_changes(thumb_axes, region.policy_changes, emoji=False)
    thumb_axes.set_xlabel(None)
    thumb_axes.set_ylabel(None)
    thumb_axes.tick_params(
        which='both', bottom=False, top=False, left=False, right=False,
        labelbottom=False, labeltop=False, labelleft=False, labelright=False)
    fig.tight_layout(pad=0.1)
    fig.savefig(urls.file(site_dir, urls.thumb_image(region)))
    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _write_chart_image(region, site_dir):
    covid_max = max(m.frame.value.max() for m in region.covid_metrics.values())
    covid_max = min(200, max(30, (covid_max // 10 + 1) * 10))
    covid_height = covid_max / 10

    if region.mobility_metrics:
        fig = matplotlib.pyplot.figure(figsize=(10, covid_height + 4), dpi=200)
        covid_axes, mobility_axes = fig.subplots(
            nrows=2, ncols=1, sharex=True,
            gridspec_kw=dict(height_ratios=[covid_height, 4]))
    else:
        fig = matplotlib.pyplot.figure(figsize=(10, covid_height), dpi=200)
        covid_axes, mobility_axes = fig.add_subplot(), None

    covid_axes.set_ylim(0, covid_max)
    _setup_xaxis(covid_axes, region, title=f'{region.short_name} COVID')
    _plot_covid_metrics(covid_axes, region.covid_metrics)
    _plot_policy_changes(covid_axes, region.policy_changes, emoji=True)
    _plot_subregion_peaks(covid_axes, region)
    _add_plot_legend(covid_axes)

    if mobility_axes:
        _setup_xaxis(
            mobility_axes, region,
            title=f'{region.short_name} mobility')
        _plot_mobility_metrics(mobility_axes, region.mobility_metrics)
        _plot_policy_changes(mobility_axes, region.policy_changes, emoji=False)
        _add_plot_legend(mobility_axes)

    fig.align_ylabels()
    fig.tight_layout(pad=0, h_pad=1)
    fig.savefig(urls.file(site_dir, urls.chart_image(region)))
    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _plot_subregion_peaks(axes, region):
    (xmin, xmax), (ymin, ymax) = axes.get_xlim(), axes.get_ylim()

    def pop(r): return r.totals['population']
    rgb = matplotlib.colors.to_rgb('tab:blue')
    xs, ys, cs, ts = [], [], [], []
    max_p = max((pop(s) for s in region.subregions.values()), default=1)
    for sub in sorted(region.subregions.values(), key=lambda r: -pop(r)):
        m = sub.covid_metrics.get('positives / 100Kp')
        if m and m.peak and matplotlib.dates.date2num(m.peak[0]) >= xmin:
            xs.append(m.peak[0])
            ys.append(min(m.peak[1], ymax))
            cs.append(rgb + (max(0.2, (pop(sub) / max_p) ** 0.5),))
            ts.append(sub.short_name.replace(' ', '')[:3])

    if xs:
        _add_to_legend(axes, axes.scatter(
            xs, ys, c=cs, marker=6, label='subdiv peak positives'), order=+5)
        for x, y, c, t in zip(xs, ys, cs, ts):
            axes.annotate(
                t, c=c, xy=(x, y), ha='center', va='top',
                xytext=(0, -15), textcoords='offset pixels')


def _plot_covid_metrics(axes, covid_metrics):
    """Plots COVID case-related metrics."""

    # (This function does not set ylim.)
    axes.set_ylabel('number per capita')
    axes.yaxis.set_label_position('right')
    axes.yaxis.tick_right()
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(5))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(1))
    for name, metric in covid_metrics.items():
        _plot_metric(axes, name, metric)


def _plot_mobility_metrics(axes, mobility_metrics):
    """Plots metrics of population mobility."""

    axes.axhline(100, c='black', lw=1)  # Identity line.
    axes.set_ylim(0, 250)
    axes.set_ylabel('% of same weekday in January')
    axes.yaxis.set_label_position('right')
    axes.yaxis.tick_right()
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


def _plot_policy_changes(axes, policy_changes, emoji):
    """Plots important policy changes."""

    date_changes = {}
    for p in policy_changes:
        if abs(p.score) >= 2:
            date_changes.setdefault(p.date.round('d'), []).append(p)

    if not date_changes:
        return

    for date, changes in date_changes.items():
        color = 'tab:orange' if changes[0].score > 0 else 'tab:blue'
        axes.axvline(date, c=color, lw=2, ls='--', alpha=0.7, zorder=1)

    if any(changes[0].score < 0 for changes in date_changes.values()):
        _add_to_legend(axes, matplotlib.lines.Line2D(
            [], [], c='tab:blue', lw=2, ls='--', alpha=0.7,
            label='closing changes'))
    if any(changes[0].score > 0 for changes in date_changes.values()):
        _add_to_legend(axes, matplotlib.lines.Line2D(
            [], [], c='tab:orange', lw=2, ls='--', alpha=0.7,
            label='reopening changes'))

    if emoji:
        top = axes.secondary_xaxis('top')
        top.set_xticks(list(date_changes.keys()))
        top.set_xticklabels(
            ['\n'.join(c.emoji.replace('\uFE0F', '') for c in changes)
             for changes in date_changes.values()],
            fontdict=dict(fontsize=15), linespacing=1.1,
            font=pathlib.Path(__file__).parent / 'NotoColorEmoji.ttf')


def _setup_xaxis(axes, region, title=None, titlesize=45):
    """Sets common X axis and plot style."""

    latest = max(
        m.frame.index.max() for m in region.covid_metrics.values()
        if m.emphasis >= 0)

    xmin = pandas.Timestamp(2020, 3, 1)
    xmax = latest + pandas.Timedelta(days=1)
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
    for label in axes.get_xticklabels():
        label.set_horizontalalignment('left')

    if title:
        axes.text(
            0.5, 0.5, '\n'.join(title.split()), transform=axes.transAxes,
            fontsize=titlesize, fontweight='bold', alpha=0.2,
            ha='center', va='center')


def _add_to_legend(axes, *artists, order=0):
    """Returns our user defined legend artist list attached to plot axes."""

    order_artists = axes.__dict__.setdefault('covid_legend_artists', {})
    order_artists.setdefault(order, []).extend(artists)


def _add_plot_legend(axes):
    """Adds a standard plot legend using the legend_artists(axes) list."""

    order_artists = axes.__dict__.get('covid_legend_artists', {})
    axes.legend(loc='upper left', handles=[
        artist for order, artists in sorted(order_artists.items())
        for artist in artists])
