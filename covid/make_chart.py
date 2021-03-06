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
    thumb_axes.set_ylim(0, 100)
    _plot_covid_metrics(thumb_axes, region, detailed=False)
    _plot_vaccination_metrics(thumb_axes, region, detailed=False)
    _plot_policy_changes(thumb_axes, region, detailed=False)
    thumb_axes.set_xlabel(None)
    thumb_axes.set_ylabel(None)
    thumb_axes.tick_params(
        which='both', bottom=False, top=False, left=False, right=False,
        labelbottom=False, labeltop=False, labelleft=False, labelright=False)
    fig.tight_layout(pad=0.1)
    fig.savefig(urls.file(site_dir, urls.thumb_image(region)))
    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _write_chart_image(region, site_dir):
    covid_max = max(
        (
            m.frame.value.max()
            for m in region.covid_metrics.values()
            if m.frame.size > 2 and m.emphasis > 0
        ),
        default=0
    )

    covid_max = min(300, max(60, (covid_max // 10 + 1) * 10))
    heights = [covid_max / 25]

    if region.vaccination_metrics:
        heights.append(3)

    if region.mobility_metrics:
        heights.append(3)

    fig = matplotlib.pyplot.figure(figsize=(10, sum(heights)), dpi=200)
    axes_list = fig.subplots(
        nrows=len(heights), ncols=1, sharex=True, squeeze=False,
        gridspec_kw=dict(height_ratios=heights))[:, 0]

    covid_axes, axes_list = axes_list[0], axes_list[1:]
    covid_axes.set_ylim(0, covid_max)
    _setup_xaxis(covid_axes, region, title=f'{region.short_name} COVID')
    _plot_covid_metrics(covid_axes, region, detailed=True)
    _plot_policy_changes(covid_axes, region, detailed=True)
    _add_plot_legend(covid_axes)

    if region.vaccination_metrics:
        vax_axes, axes_list = axes_list[0], axes_list[1:]
        _setup_xaxis(
            vax_axes,
            region,
            title=f'{region.short_name} vaccination')
        _plot_vaccination_metrics(vax_axes, region, detailed=True)
        _plot_policy_changes(vax_axes, region, detailed=False)
        _add_plot_legend(vax_axes)

    if region.mobility_metrics:
        mobility_axes, axes_list = axes_list[0], axes_list[1:]
        _setup_xaxis(
            mobility_axes, region, title=f'{region.short_name} mobility')
        _plot_mobility_metrics(mobility_axes, region, detailed=True)
        _plot_policy_changes(mobility_axes, region, detailed=False)
        _add_plot_legend(mobility_axes)

    fig.align_ylabels()
    fig.tight_layout(pad=0, h_pad=1)
    fig.savefig(urls.file(site_dir, urls.chart_image(region)))
    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _plot_covid_metrics(axes, region, detailed):
    """Plots COVID case-related metrics."""

    # (This function does not set ylim.)
    axes.set_ylabel('daily change per capita')
    axes.yaxis.set_label_position('right')
    axes.yaxis.tick_right()
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(10))
    _plot_metrics(axes, region.covid_metrics, detailed=detailed)


def _plot_vaccination_metrics(axes, region, detailed):
    """Plots COVID vaccination metrics."""

    axes.set_ylim(0, 150)
    axes.set_ylabel('% of population (cumulative)')
    axes.yaxis.set_label_position('right')
    axes.yaxis.tick_right()
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(50))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(10))
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    if detailed:
        axes.axhline(100, c='black', lw=1)  # 100% line.
    _plot_metrics(axes, region.vaccination_metrics, detailed=detailed)


def _plot_mobility_metrics(axes, region, detailed):
    """Plots metrics of population mobility."""

    axes.set_ylim(0, 150)
    axes.set_ylabel('% of same weekday in January')
    axes.yaxis.set_label_position('right')
    axes.yaxis.tick_right()
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(50))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(10))
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    if detailed:
        axes.axhline(100, c='black', lw=1)  # Identity line.
    _plot_metrics(axes, region.mobility_metrics, detailed=detailed)


def _plot_metrics(axes, metrics, detailed):
    for name, m in sorted(metrics.items(), key=lambda nm: nm[1].order):
        if m.emphasis < 1 and not detailed:
            continue
        width = 4 if m.emphasis >= 1 else 2
        style = '-' if m.emphasis >= 0 else '--'
        alpha = 1.0 if m.emphasis >= 0 else 0.5
        zorder = 2.0 - m.order / 100
        if detailed and ('raw' in m.frame.columns):
            axes.plot(
                m.frame.index, m.frame.raw,
                c=m.color, alpha=alpha * 0.5, zorder=zorder + 0.001,
                lw=1, ls=style)
        if 'min' in m.frame.columns and 'max' in m.frame.columns:
            axes.fill_between(
                x=m.frame.index, y1=m.frame['min'], y2=m.frame['max'],
                color=m.color, alpha=0.2, zorder=zorder - 1)
        if 'value' in m.frame.columns and m.frame.value.any():
            last_date = m.frame.value.last_valid_index()
            blot_size = (width * 2) ** 2
            axes.scatter(
                [last_date], [m.frame.value.loc[last_date]],
                c=m.color, alpha=alpha, zorder=zorder + 0.002, s=blot_size)
            _add_to_legend(axes, *axes.plot(
                m.frame.index, m.frame.value, label=name,
                c=m.color, alpha=alpha, zorder=zorder, lw=width, ls=style))


def _plot_policy_changes(axes, region, detailed):
    """Plots important policy changes."""

    date_changes = {}
    for p in region.policy_changes:
        if abs(p.score) >= 2 or p == region.current_policy:
            date_changes.setdefault(p.date.round('d'), []).append(p)
    for date, changes in date_changes.items():
        s = changes[0].score
        color = 'tab:orange' if s > 0 else 'tab:blue' if s else 'tab:gray'
        axes.axvline(date, c=color, lw=2, ls='--', alpha=0.7, zorder=1)

    if detailed:
        if any(changes[0].score < 0 for changes in date_changes.values()):
            _add_to_legend(axes, matplotlib.lines.Line2D(
                [], [], c='tab:blue', lw=2, ls='--', alpha=0.7,
                label='closing changes'))

        if any(changes[0].score > 0 for changes in date_changes.values()):
            _add_to_legend(axes, matplotlib.lines.Line2D(
                [], [], c='tab:orange', lw=2, ls='--', alpha=0.7,
                label='reopening changes'))

        if date_changes:
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


if __name__ == '__main__':
    import argparse
    import signal
    from covid import cache_policy
    from covid import combine_data
    from pathlib import Path

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior.
    arg_parents = [cache_policy.argument_parser, combine_data.argument_parser]
    parser = argparse.ArgumentParser(parents=arg_parents)
    parser.add_argument('--region', required=True)
    parser.add_argument('--site_dir', type=Path, default=Path('site_out'))
    args = parser.parse_args()

    session = cache_policy.new_session(args)
    world = combine_data.get_world(session=session, args=args, verbose=True)
    region = world.lookup_path(args.region)
    write_images(region=region, site_dir=args.site_dir)
