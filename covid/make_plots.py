"""Functions to generate chart images for region pages."""

import pathlib

import matplotlib
import matplotlib.lines
import matplotlib.pyplot
import numpy
import pandas

from covid import plot_metrics
from covid import urls

matplotlib.use("module://mplcairo.base")  # For decent emoji rendering.


def write_images(region, site_dir):
    _write_thumb_image(region, site_dir)
    _write_chart_image(region, site_dir)


def _write_thumb_image(region, site_dir):
    # Make thumbnail for index page
    p = (1 + 5**0.5) / 2  # Nice pleasing aspect ratio.
    fig = matplotlib.pyplot.figure(figsize=(8, 8 / p), dpi=50)
    thumb_axes = fig.add_subplot()
    plot_metrics.setup_xaxis(thumb_axes)
    thumb_axes.set_ylim(0, 300)
    plot_metrics.plot_metrics(thumb_axes, region.metrics.covid, detailed=False)
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
        *[
            lambda axes, region, s=site: _plot_wastewater(axes, region, s)
            for site in region.metrics.wastewater.keys()
        ],
        _plot_variant,
        _plot_vaccine,
        _plot_mobility,
    ]

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
        plot_metrics.plot_legend(axes)

    fig.align_ylabels()
    fig.tight_layout(pad=0, h_pad=1)
    fig.savefig(urls.file(site_dir, urls.chart_image(region)))
    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _plot_covid(axes, region):
    metrics = region.metrics.covid
    max_value = max(
        (m.frame.value.max() for m in metrics.values() if m.emphasis > 0),
        default=0,
    )

    ylim = min(1000, max(200, (max_value // 20 + 2) * 20))
    if axes is None:
        return ylim / 75 if metrics else 0

    plot_metrics.setup_xaxis(axes, title=f"{region.path[-1]} COVID")
    plot_metrics.setup_yaxis(axes, title="cases per capita", ylim=(0, ylim))
    plot_metrics.plot_metrics(axes, metrics)


def _plot_hospital(axes, region):
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
    if axes is None:
        return ylim / 120 if metrics else 0

    plot_metrics.setup_xaxis(axes, title="Hospitals")
    plot_metrics.setup_yaxis(
        axes, title="per cap", ylim=(0, ylim), tick=(40, 20)
    )
    plot_metrics.plot_metrics(axes, metrics)


def _plot_wastewater(axes, region, site):
    metrics = region.metrics.wastewater[site]
    max_value = max((m.frame.value.max() for m in metrics.values()), default=0)
    ylim = min(3000, max(1500, (max_value // 100 + 2) * 100))
    if axes is None:
        return ylim / 1000

    title = f"{site} wastewater"
    plot_metrics.setup_xaxis(axes, title=title, wrapchars=25, titlesize=35)
    plot_metrics.setup_yaxis(
        axes, title="COVID RNA", ylim=(0, ylim), tick=(500, 100)
    )
    plot_metrics.plot_metrics(axes, metrics)


def _plot_variant(axes, region):
    metrics = region.metrics.variant
    if axes is None:
        return 2 if metrics else 0

    plot_metrics.setup_xaxis(axes, title="Variants")
    plot_metrics.setup_yaxis(axes, title="% sequenced samples")

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

        artist = axes.fill_between(
            x=next.index, y1=prev, y2=next, color=v.color, label=name
        )
        if name in top_variants:
            plot_metrics.add_to_legend(axes, artist, order=-i)
        prev = next


def _plot_vaccine(axes, region):
    metrics = region.metrics.vaccine
    if axes is None:
        return 2 if metrics else 0

    plot_metrics.setup_xaxis(axes, title="Vaccination")
    plot_metrics.setup_yaxis(axes, title="% of pop (cumulative)")
    axes.axhline(100, c="black", lw=1)  # 100% line.
    plot_metrics.plot_metrics(axes, metrics)


def _plot_mobility(axes, region):
    metrics = region.metrics.mobility
    if axes is None:
        return 2 if metrics else 0

    plot_metrics.setup_xaxis(axes, title="Mobility")
    plot_metrics.setup_yaxis(
        axes, title="% vs Jan 2020", ylim=(0, 150), tick=(50, 10)
    )
    axes.axhline(100, c="black", lw=1)  # Identity line.
    plot_metrics.plot_metrics(axes, metrics)


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
            plot_metrics.add_to_legend(axes, artist)

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
