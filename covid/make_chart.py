"""Functions that generate trend charts from region metrics."""

import pathlib

import matplotlib
import matplotlib.dates
import matplotlib.lines
import matplotlib.pyplot
import matplotlib.ticker
import numpy
import pandas
import textwrap

from covid import urls

matplotlib.use("module://mplcairo.base")  # For decent emoji rendering.

matplotlib.rcParams.update({"figure.max_open_warning": 0})

plot_start_date = pandas.Timestamp(2020, 3, 1)
plot_end_date = pandas.Timestamp.now().ceil("d") + pandas.Timedelta(days=7)


def write_images(region, site_dir):
    _write_thumb_image(region, site_dir)
    _write_chart_image(region, site_dir)


def _write_thumb_image(region, site_dir):
    # Make thumbnail for index page
    p = (1 + 5**0.5) / 2  # Nice pleasing aspect ratio.
    fig = matplotlib.pyplot.figure(figsize=(8, 8 / p), dpi=50)
    thumb_axes = fig.add_subplot()
    _setup_xaxis(thumb_axes)
    thumb_axes.set_ylim(0, 300)
    _plot_metrics(thumb_axes, region.metrics.covid, detailed=False)
    thumb_axes.set_xlabel(None)
    thumb_axes.set_ylabel(None)
    thumb_axes.tick_params(
        which="both", bottom=0, left=0, labelbottom=0, labelleft=0
    )
    fig.tight_layout(pad=0.1)
    fig.savefig(urls.file(site_dir, urls.thumb_image(region)))
    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _write_chart_image(region, site_dir):
    plotters = [
        _plot_covid,
        _plot_hospital,
        _plot_wastewater,
        _plot_variant,
        _plot_vaccine,
        _plot_mobility,
    ]

    height_stacks = [p(None, region) for p in plotters]
    fig = matplotlib.pyplot.figure(
        figsize=(10, sum(sum(stack) for stack in height_stacks)),
        dpi=200
    )

    subplots = fig.subplots(
        nrows=sum(len(stack) for stack in height_stacks),
        ncols=1,
        sharex=True,
        squeeze=False,
        gridspec_kw=dict(height_ratios=[h for s in height_stacks for h in s]),
    )[:, 0]

    top = True
    for plotter, stack in zip(plotters, height_stacks):
        if stack:
            axes_stack, subplots = subplots[:len(stack)], subplots[len(stack):]
            plotter(axes_stack, region)
            for axes in axes_stack:
                _plot_policy_changes(axes, region.metrics.policy, detailed=top)
                _add_plot_legend(axes)
                top = False

    fig.align_ylabels()
    fig.tight_layout(pad=0, h_pad=1)
    fig.savefig(urls.file(site_dir, urls.chart_image(region)))
    matplotlib.pyplot.close(fig)  # Reclaim memory.
    return

    plotters, heights = zip(
        *[(p, h) for p in plotters for h in (p(None, region),) if h > 0]
    )

    fig = matplotlib.pyplot.figure(figsize=(10, sum(heights)), dpi=200)
    subplots = fig.subplots(
        nrows=len(heights),
        ncols=1,
        sharex=True,
        squeeze=False,
        gridspec_kw=dict(height_ratios=heights),
    )

    for i, (axes, plotter) in enumerate(zip(subplots[:, 0], plotters)):
        plotter(axes, region)
        _plot_policy_changes(axes, region.metrics.policy, detailed=(i == 0))
        _add_plot_legend(axes)

    fig.align_ylabels()
    fig.tight_layout(pad=0, h_pad=1)
    fig.savefig(urls.file(site_dir, urls.chart_image(region)))
    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _plot_covid(stack, region):
    metrics = region.metrics.covid
    max_value = max(
        (m.frame.value.max() for m in metrics.values() if m.emphasis > 0),
        default=0,
    )

    ylim = min(1000, max(200, (max_value // 20 + 2) * 20))
    if stack is None:
        return [ylim / 75] if metrics else []

    _setup_xaxis(stack[0], title=f"{region.path[-1]} COVID")
    _setup_yaxis(stack[0], title="cases per capita", ylim=(0, ylim))
    _plot_metrics(stack[0], metrics)


def _plot_hospital(stack, region):
    metrics = region.metrics.hospital
    max_value = max(
        (
            m.frame.value.max()
            if m.emphasis >= 0
            else m.frame.value.quantile(0.9)
            for m in metrics.values()
        ),
        default=0,
    )

    ylim = min(1000, max(240, (max_value // 20 + 2) * 20))
    if stack is None:
        return [ylim / 120] if metrics else []

    _setup_xaxis(stack[0], title="Hospitals")
    _setup_yaxis(stack[0], title="per cap", ylim=(0, ylim), tick=(40, 20))
    _plot_metrics(stack[0], metrics)


def _plot_wastewater(stack, region):
    heights = []
    for i, (site, mets) in enumerate(region.metrics.wastewater.items()):
        max_value = max((m.frame.value.max() for m in mets.values()), default=0)
        ylim = min(3000, max(1500, (max_value // 100 + 2) * 100))
        heights.append(ylim / 1000)
        if stack is None:
            continue

        axes = stack[i]
        title = f"Wastewater:\n{site}"
        _setup_xaxis(axes, title=title, wrapchars=25, titlesize=35)
        _setup_yaxis(axes, title="COVID RNA", ylim=(0, ylim), tick=(500, 100))
        _plot_metrics(axes, mets)

    return heights


def _plot_variant(stack, region):
    metrics = region.metrics.variant
    if stack is None:
        return [2] if metrics else []

    _setup_xaxis(stack[0], title="Variants")
    _setup_yaxis(stack[0], title="% sequenced samples")

    # Label top variants by frequency, weighting recent frequency heavily
    freq_name = [
        (10 * m.frame.value.iloc[-5:].mean() + m.frame.value.mean(), name)
        for name, m in metrics.items()
    ]
    top_variants = set(name for f, name in sorted(freq_name)[-7:])

    prev = None
    for i, (name, v) in enumerate(reversed(metrics.items())):
        if prev is None:
            zeros = numpy.zeros(len(v.frame.value))
            prev = pandas.Series(data=zeros, index=v.frame.index)
        prev = prev.reindex(v.frame.value.index, copy=False)
        next = prev.add(v.frame.value, fill_value=0)
        assert next.index.equals(prev.index)

        artist = stack[0].fill_between(
            x=next.index, y1=prev, y2=next, color=v.color, label=name
        )
        if name in top_variants:
            _add_to_legend(stack[0], artist, order=-i)
        prev = next

    # _add_plot_legend(stack[0])


def _plot_vaccine(stack, region):
    metrics = region.metrics.vaccine
    if stack is None:
        return [2] if metrics else []

    _setup_xaxis(stack[0], title="Vaccination")
    _setup_yaxis(stack[0], title="% of pop (cumulative)")
    stack[0].axhline(100, c="black", lw=1)  # 100% line.
    _plot_metrics(stack[0], metrics)


def _plot_mobility(stack, region):
    metrics = region.metrics.mobility
    if stack is None:
        return [2] if metrics else []

    _setup_xaxis(stack[0], title="Mobility")
    _setup_yaxis(stack[0], title="% vs Jan 2020", ylim=(0, 150), tick=(50, 10))
    stack[0].axhline(100, c="black", lw=1)  # Identity line.
    _plot_metrics(stack[0], metrics)


def _plot_metrics(axes, metrics, detailed=True):
    for name, m in sorted(metrics.items(), key=lambda nm: nm[1].order):
        if m.emphasis < 1 and not detailed:
            continue

        width = 4 if m.emphasis >= 1 else 2
        style = "-" if m.emphasis >= 0 else "--"
        alpha = 1.0 if m.emphasis >= 0 else 0.8
        zorder = 2.0 - m.order / 100 - m.emphasis / 10

        if detailed and ("raw" in m.frame.columns):
            limit = m.frame.raw.quantile(0.99) * 2
            masked = m.frame.raw.mask(m.frame.raw.gt(limit))

            axes.plot(
                m.frame.index,
                masked,
                color=m.color,
                alpha=alpha * 0.5,
                zorder=zorder + 0.001,
                lw=0.5,
                ls=style,
            )

        if "value" in m.frame.columns and m.frame.value.any():
            last_date = m.frame.value.last_valid_index()
            blot_size = (width * 2) ** 2
            axes.scatter(
                [last_date],
                [m.frame.value.loc[last_date]],
                color=m.color,
                alpha=alpha,
                zorder=zorder + 0.002,
                s=blot_size,
            )
            artists = axes.plot(
                m.frame.index,
                m.frame.value,
                label=name,
                color=m.color,
                alpha=alpha,
                zorder=zorder,
                lw=width,
                ls=style,
            )
            _add_to_legend(axes, *artists)


def _plot_policy_changes(axes, changes, detailed):
    """Plots important policy changes."""

    date_changes = {}
    for p in changes:
        if abs(p.score) >= 2:
            date_changes.setdefault(p.date.round("d"), []).append(p)

    date_color = {
        date: "tab:gray"
        if not any(c.score for c in changes)
        else "tab:orange"
        if all(c.score >= 0 for c in changes)
        else "tab:blue"
        if all(c.score <= 0 for c in changes)
        else "tab:gray"
        for date, changes in date_changes.items()
    }

    for date, color in date_color.items():
        axes.axvline(date, c=color, lw=2, ls="--", alpha=0.7, zorder=1)

    for color in set(date_color.values()):
        t = {"tab:blue": "closing", "tab:orange": "reopening"}.get(color)
        if detailed and t:
            artist = matplotlib.lines.Line2D(
                [],
                [],
                c=color,
                lw=2,
                ls="--",
                alpha=0.7,
                label=t + " changes",
            )
            _add_to_legend(axes, artist)

    if detailed and date_changes:
        top = axes.secondary_xaxis("top")
        top.set_xticks(list(date_changes.keys()))
        top.set_xticklabels(
            [
                "\n".join(
                    emoji.replace("\uFE0F", "")
                    for emoji in {change.emoji: 1 for change in changes}
                )
                for changes in date_changes.values()
            ],
            fontdict=dict(fontsize=15),
            linespacing=1.1,
            font=pathlib.Path(__file__).parent / "NotoColorEmoji.ttf",
        )


def _setup_xaxis(axes, title=None, wrapchars=15, titlesize=45):
    """Sets common X axis and plot style."""

    xmin = plot_start_date
    xmax = plot_end_date
    axes.set_xlim(xmin, xmax)
    axes.grid(color="black", alpha=0.1)

    week_locator = matplotlib.dates.WeekdayLocator(matplotlib.dates.SU)
    month_locator = matplotlib.dates.MonthLocator()
    month_formatter = matplotlib.dates.ConciseDateFormatter(month_locator)
    month_formatter.offset_formats[1] = ""  # Don't bother with year '2020'.

    axes.xaxis.set_minor_locator(week_locator)
    axes.xaxis.set_major_locator(month_locator)
    axes.xaxis.set_major_formatter(month_formatter)
    axes.xaxis.set_tick_params(labelbottom=True)
    for label in axes.get_xticklabels():
        label.set_horizontalalignment("left")

    text = "\n".join(
        line
        for para in (title or '').splitlines()
        for line in textwrap.wrap(para, width=wrapchars, break_on_hyphens=False)
    )

    if title:
        axes.text(
            0.5,
            0.5,
            text,
            transform=axes.transAxes,
            fontsize=titlesize,
            fontweight="bold",
            alpha=0.2,
            ha="center",
            va="center",
        )


def _setup_yaxis(axes, title=None, ylim=(0, 100), tick=(20, 10)):
    axes.set_ylim(*ylim)
    axes.set_ylabel(title)
    axes.yaxis.set_label_position("right")
    axes.yaxis.tick_right()
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(tick[0]))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(tick[1]))


def _add_to_legend(axes, *artists, order=0):
    """Returns our user defined legend artist list attached to plot axes."""

    order_artists = axes.__dict__.setdefault("covid_legend_artists", {})
    order_artists.setdefault(order, []).extend(artists)


def _add_plot_legend(axes):
    """Adds a standard plot legend using the legend_artists(axes) list."""

    order_artists = axes.__dict__.get("covid_legend_artists", {})
    axes.legend(
        loc="upper left",
        handles=[
            artist
            for order, artists in sorted(order_artists.items())
            for artist in artists
        ],
    )
