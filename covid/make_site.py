"""Main program to generate COVID stats static site."""

import argparse
import multiprocessing
import os
import pathlib

import dominate
from dominate import tags
from dominate import util

from covid import build_world
from covid import cache_policy
from covid import logging_policy  # noqa
from covid import make_chart
from covid import make_map
from covid import style
from covid import urls


def make_region_page(region, args):
    """Write region-specific page and associated images."""

    map_note = ""
    try:
        make_region_html(region, args)
        make_chart.write_images(region, args.site_dir)
        if urls.has_map(region):
            make_map.write_video(region, args.site_dir)
            map_note = " (+map video)"
    except Exception as e:
        print(f"*** Error making {region.path()}: {e} ***")
        raise Exception(f"Error making {region.path()}") from e

    print(f"Made: {region.path()}{map_note}")


def make_region_html(region, args):
    """Write region-specific HTML page."""

    latest = max(
        m.frame.index.max()
        for m in region.metrics["covid"].values()
        if m.emphasis >= 0
    )

    doc = dominate.document(title=f"{region.name} COVID-19 ({latest.date()})")
    doc_url = urls.region_page(region)

    def doc_link(url):
        return urls.link(doc_url, url)

    with doc.head:
        style.add_head_style(doc_url)

    with doc.body:
        tags.attr(id="map_key_target", tabindex="-1")
        with tags.h1():

            def write_breadcrumbs(r):
                if r is not None:
                    write_breadcrumbs(r.parent)
                    tags.a(r.short_name, href=doc_link(urls.region_page(r)))
                    util.text(" » ")

            write_breadcrumbs(region.parent)
            util.text(region.name)

        with tags.div():
            pop = region.totals["population"]
            vax = region.totals.get("vaccinated", 0)
            pos = region.totals.get("positives", 0)
            dead = region.totals.get("deaths", 0)

            nobreak = lambda t: tags.span(t, cls="nobreak")
            nobreak(f"{pop:,.0f} pop; ")
            if vax:
                nobreak(f"{vax:,.0f} ({100 * vax / pop:.2g}%) vacc, ")
            nobreak(f"{pos:,.0f} ({100 * pos / pop:.2g}%) pos, ")
            nobreak(f"{dead:,.0f} ({100 * dead / pop:.2g}%) deaths ")
            nobreak(f"as of {latest.date()}")

        if urls.has_map(region):
            with tags.div(cls="graphic"):
                with tags.video(id="map", preload="auto"):
                    href = urls.link(doc_url, urls.map_video_maybe(region))
                    tags.source(type="video/webm", src=f"{href}#t=1000")

                with tags.div(cls="map_controls"):

                    def i(n):
                        return tags.i(cls=f"fas fa-{n}")

                    tags.button(i("pause"), " ", i("play"), " P", id="map_play")
                    tags.button(i("repeat"), " L", id="map_loop")
                    tags.button(i("backward"), " R", id="map_rewind")
                    tags.button(i("step-backward"), " [", id="map_prev")
                    tags.input_(type="range", id="map_slider")
                    tags.button(i("step-forward"), " ]", id="map_next")
                    tags.button(i("forward"), " F", id="map_forward")

        tags.img(cls="graphic", src=doc_link(urls.chart_image(region)))

        notables = [p for p in region.policy_changes if p.score]
        if notables:
            tags.h2(
                tags.span("Closing", cls="policy_close"),
                " and ",
                tags.span("Reopening", cls="policy_open"),
                " policy changes",
            )

            with tags.div(cls="policies"):
                last_date = None
                for p in notables:
                    date, s = str(p.date.date()), p.score
                    if date != last_date:
                        tags.div(date, cls=f"date")
                        last_date = date

                    tags.div(p.emoji, cls=f"emoji")

                    tags.div(
                        p.text,
                        cls="text"
                        + (" policy_close" if s < 0 else "")
                        + (" policy_open" if s > 0 else "")
                        + (" policy_major" if abs(s) >= 2 else ""),
                    )

        subs = [
            r
            for r in region.subregions.values()
            if r.matches_regex(args.region_regex)
        ]
        if subs:
            sub_pop = sum(s.totals["population"] for s in subs)
            if len(subs) >= 10 and sub_pop > 0.9 * region.totals["population"]:

                def pop(r):
                    return r.totals.get("population", 0)

                def pos(r):
                    m = r.metrics["covid"].get("COVID positives / day / 100Kp")
                    return m.frame.value.iloc[-1] * pop(r) if m else 0

                tags.h2("Top 5 by population")
                for s in list(sorted(subs, key=pop, reverse=True))[:5]:
                    make_subregion_html(doc_url, s)

                tags.h2("Top 5 by new positives")
                for s in list(sorted(subs, key=pos, reverse=True))[:5]:
                    make_subregion_html(doc_url, s)

                tags.h2(f'All {"divisions" if region.parent else "countries"}')
            else:
                tags.h2("Subdivisions")
            for s in sorted(subs, key=lambda r: r.name):
                make_subregion_html(doc_url, s)

        r = region
        credits = dict(c for p in r.policy_changes for c in p.credits.items())
        for ms in r.metrics.values():
            credits.update(c for m in ms.values() for c in m.credits.items())
        with tags.p("Sources: ", cls="credits"):
            for i, (url, text) in enumerate(credits.items()):
                util.text(", ") if i > 0 else None
                tags.a(text, href=url)

    with open(urls.file(args.site_dir, doc_url), "w") as doc_file:
        doc_file.write(doc.render())


def make_subregion_html(doc_url, region):
    region_href = urls.link(doc_url, urls.region_page(region))
    with tags.a(cls="subregion", href=region_href):
        with tags.div(cls="subregion_label", __pretty=False):
            util.text(region.name)
            with tags.div():
                pop = region.totals["population"]
                util.text(f"{pop:,.0f}\xa0pop")
                vax = region.totals.get("vaccinated", 0)
                util.text(f", {100 * vax / pop:,.2g}%\xa0vax" if vax else "")

        tags.img(width=200, src=urls.link(doc_url, urls.thumb_image(region)))


def main():
    parser = argparse.ArgumentParser(
        parents=[cache_policy.argument_parser, build_world.argument_parser]
    )
    parser.add_argument("--processes", type=int)
    parser.add_argument("--chunk_size", type=int)
    parser.add_argument(
        "--site_dir", type=pathlib.Path, default=pathlib.Path("site_out")
    )
    parser.add_argument("--region_regex")
    args = parser.parse_args()
    make_map.setup(args)

    world = build_world.get_world(
        session=cache_policy.new_session(args), args=args
    )

    def get_regions(r):
        if r.matches_regex(args.region_regex):
            yield r
        yield from (a for s in r.subregions.values() for a in get_regions(s))

    all_regions = list(get_regions(world))

    print(f"Generating {len(all_regions)} pages in {args.site_dir}...")
    style.write_style_files(args.site_dir)

    # Process regions using multiple cores.
    processes = args.processes or os.cpu_count() * 2
    chunk_size = args.chunk_size or max(1, len(all_regions) // (4 * processes))
    with multiprocessing.Pool(processes=args.processes) as pool:
        pool.starmap(
            make_region_page,
            ((r, args) for r in all_regions),
            chunksize=chunk_size,
        )


if __name__ == "__main__":
    main()
