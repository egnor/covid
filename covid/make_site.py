"""Main program to generate COVID stats static site."""

import argparse
import collections
import multiprocessing
import os
import pathlib
import re
import signal

import dominate
from dominate import tags, util

from covid import cache_policy
from covid import combine_data
from covid import make_chart
from covid import make_map
from covid import style
from covid import urls


def make_region_page(region, site_dir):
    """Write region-specific page and associated images."""

    def get_path(r):
        return f'{get_path(r.parent)}{r.short_name}/' if r else ''
    path = f'{get_path(region.parent)}{region.short_name}'

    try:
        make_region_html(region, site_dir)
        make_chart.write_thumb_image(region, site_dir)
        make_chart.write_image(region, site_dir)
        make_map.write_video(region, site_dir)
    except Exception as e:
        print(f'*** Error making {path}: {e} ***')
        raise Exception(f'Error making {path}')

    print(f'Made: {path}{"*" if urls.map_video_maybe(region) else ""}')


def make_region_html(region, site_dir):
    """Write region-specific HTML page."""

    latest = max(m.frame.index.max() for m in region.covid_metrics.values())
    doc = dominate.document(title=f'{region.name} ({latest.date()}) COVID-19')
    doc_url = urls.region_page(region)
    def doc_link(url): return urls.link(doc_url, url)

    with doc.head:
        style.add_head_style(doc_url)

    with doc.body:
        with tags.h1():
            def write_breadcrumbs(r):
                if r is not None:
                    write_breadcrumbs(r.parent)
                    tags.a(r.short_name, href=doc_link(urls.region_page(r)))
                    util.text(' » ')

            write_breadcrumbs(region.parent)
            util.text(region.short_name)
            if region.name != region.short_name:
                util.text(f' / {region.name}')
            util.text(f' (pop. {region.population:,.0f})')

        tags.img(cls='plot', src=doc_link(urls.chart_image(region)))

        if region.daily_events:
            with tags.h2():
                tags.span('Mitigation', cls='event_close')
                util.text(' and ')
                tags.span('Relaxation', cls='event_open')
                util.text(' Changes')

            def score_css(s):
                return f'event_{"open" if s > 0 else "close"} score_{abs(s)}'
            with tags.div(cls='events'):
                for day in (d for d in region.daily_events if d.score):
                    date = str(day.date.date())
                    tags.div(date, cls=f'event_date {score_css(day.score)}')
                    for ev in (e for e in day.frame.itertuples() if e.score):
                        css = score_css(ev.score)
                        tags.div(ev.emoji, cls=f'event_emoji {css}')
                        tags.div(ev.policy, cls=f'event_policy {css}')

        if region.subregions:
            subs = list(region.subregions.values())
            if len(subs) >= 10:
                tags.h2('Top 5 by population')
                for s in list(sorted(subs, key=lambda r: -r.population))[:5]:
                    make_thumb_link_html(doc_url, s)
                tags.h2('All subdivisions')
            else:
                tags.h2('Subdivisions')
            for s in sorted(subs, key=lambda r: r.name):
                make_thumb_link_html(doc_url, s)

        with tags.p('Sources: ', cls='credits'):
            for i, (url, text) in enumerate(region.credits.items()):
                util.text(', ') if i > 0 else None
                tags.a(text, href=url)

    with open(urls.file(site_dir, doc_url), 'w') as doc_file:
        doc_file.write(doc.render())


def make_thumb_link_html(doc_url, region):
    region_href = urls.link(doc_url, urls.region_page(region))
    with tags.a(cls='thumb', href=region_href):
        with tags.div(cls='thumb_label'):
            util.text(region.name)
            tags.div(f'pop. {region.population:,.0f}', cls='thumb_pop')
        tags.img(width=200, src=urls.link(doc_url, urls.thumb_image(region)))


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior
    parser = argparse.ArgumentParser(parents=[
        cache_policy.argument_parser, combine_data.argument_parser])
    parser.add_argument('--processes', type=int)
    parser.add_argument('--chunk_size', type=int)
    parser.add_argument('--region_regex')
    parser.add_argument('--site_dir', type=pathlib.Path,
                        default=pathlib.Path('site_out'))
    args = parser.parse_args()

    print('Loading data...')
    make_map.setup(args)
    world = combine_data.get_world(
        session=cache_policy.new_session(args), args=args, verbose=True)

    print('Enumerating regions...')
    regex = args.region_regex and re.compile(args.region_regex, re.I)
    def all_regions(r):
        if combine_data.region_matches_regex(r, regex): yield r
        yield from (a for s in r.subregions.values() for a in all_regions(s))
    all = list(all_regions(world))

    print(f'Generating {len(all)} pages in {args.site_dir}...')
    style.write_style_files(args.site_dir)

    # Recurse for subregions.
    processes = args.processes or os.cpu_count() * 2
    chunk_size = args.chunk_size or max(1, len(all) // (4 * processes))
    with multiprocessing.Pool(processes=args.processes) as pool:
        pool.starmap(
            make_region_page, ((r, args.site_dir) for r in all),
            chunksize=args.chunk_size)


if __name__ == '__main__':
    main()
