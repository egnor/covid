"""Functions to help generate trend charts from region metrics."""

import contextlib
import logging
import textwrap

import matplotlib
import matplotlib.dates
import matplotlib.pyplot
import matplotlib.ticker
import pandas

matplotlib.rcParams.update({"figure.max_open_warning": 0})

_plot_start_date = pandas.Timestamp(2020, 3, 1)
_plot_end_date = pandas.Timestamp.now().ceil("d") + pandas.Timedelta(days=7)


@contextlib.contextmanager
def subplots_context(heights, filename):
    fig = matplotlib.pyplot.figure(figsize=(10, sum(heights)), dpi=200)
    subs = fig.subplots(
        nrows=len(heights),
        ncols=1,
        sharex=True,
        squeeze=False,
        gridspec_kw=dict(height_ratios=heights)
    )

    yield subs[:, 0]

    logging.debug(f"Writing: {filename}")
    fig.align_ylabels()
    fig.tight_layout(pad=0, h_pad=1)
    fig.savefig(filename)
    matplotlib.pyplot.close(fig)  # Reclaim memory


def setup_xaxis(axes, title=None, wrapchars=15, titlesize=45):
    """Sets common X axis and plot style."""

    xmin = _plot_start_date
    xmax = _plot_end_date
    axes.set_xlim(xmin, xmax)
    axes.grid(color="black", alpha=0.1)

    week_locator = matplotlib.dates.WeekdayLocator(matplotlib.dates.SU)
    month_locator = matplotlib.dates.MonthLocator()
    month_formatter = matplotlib.dates.ConciseDateFormatter(month_locator)
    month_formatter.offset_formats[1] = ""  # Don't bother with year '2020'.
    month_formatter.zero_formats[1] = "'%y"  # Abbreviate years.

    axes.xaxis.set_minor_locator(week_locator)
    axes.xaxis.set_major_locator(month_locator)
    axes.xaxis.set_major_formatter(month_formatter)
    axes.xaxis.set_tick_params(labelbottom=True)
    for label in axes.get_xticklabels():
        label.set_horizontalalignment("left")

    text = "\n".join(
        line
        for para in (title or "").splitlines()
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


def setup_yaxis(axes, title=None, ylim=(0, 100), tick=(20, 10)):
    axes.set_ylim(*ylim)
    axes.set_ylabel(title)
    axes.yaxis.set_label_position("right")
    axes.yaxis.tick_right()
    axes.yaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    axes.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(tick[0]))
    axes.yaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(tick[1]))


def plot_metrics(axes, metrics, detailed=True):
    for name, m in sorted(metrics.items(), key=lambda nm: nm[1].order):
        if m.emphasis < 1 and not detailed:
            continue

        width = 4 if m.emphasis >= 1 else 2
        style = "-" if m.emphasis >= 0 else "--"
        alpha = 1.0 if m.emphasis >= 0 else 0.8
        zorder = 2.0 - m.order / 100 - m.emphasis / 10

        deltas = (m.frame.notna().any(1)).index.to_series().diff()
        gaps = deltas[deltas > pandas.Timedelta(days=15)]
        breaks = pandas.DataFrame(index=gaps.index - gaps / 2)
        frame = pandas.concat([m.frame, breaks])
        frame.sort_index(inplace=True)

        if detailed and ("raw" in frame.columns) and frame.raw.any():
            limit = frame.raw.quantile(0.99) * 2
            masked = frame.raw.mask(frame.raw.gt(limit))
            axes.plot(
                frame.index,
                masked,
                color=m.color,
                alpha=alpha * 0.5,
                zorder=zorder + 0.001,
                lw=0.5,
                ls=style,
            )

        if ("value" in frame.columns) and frame.value.any():
            last_date = frame.value.last_valid_index()
            blot_size = (width * 2) ** 2
            axes.scatter(
                [last_date],
                [frame.value.loc[last_date]],
                color=m.color,
                alpha=alpha,
                zorder=zorder + 0.002,
                s=blot_size,
            )
            artists = axes.plot(
                frame.index,
                frame.value,
                label=name,
                color=m.color,
                alpha=alpha,
                zorder=zorder,
                lw=width,
                ls=style,
            )
            add_to_legend(axes, *artists)


def add_to_legend(axes, *artists, order=0):
    """Adds custom artists to the legend artist list grafted onto plot axes."""

    order_artists = axes.__dict__.setdefault("covid_legend_artists", {})
    order_artists.setdefault(order, []).extend(artists)


def plot_legend(axes):
    """Adds a standard plot legend using the legend_artists list."""

    order_artists = axes.__dict__.get("covid_legend_artists", {})
    axes.legend(
        loc="upper left",
        handles=[
            artist
            for order, artists in sorted(order_artists.items())
            for artist in artists
        ],
    )
