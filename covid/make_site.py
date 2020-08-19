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


def make_region_page(region, args):
    """Write region-specific page and associated images."""

    map_note = ''
    try:
        make_region_html(region, args)
        make_chart.write_images(region, args.site_dir)
        if has_map(region, args):
            make_map.write_video(region, args.site_dir)
            map_note = ' (+map video)'
    except Exception as e:
        print(f'*** Error making {region.path()}: {e} ***')
        raise Exception(f'Error making {region.path()}')

    print(f'Made: {region.path()}{map_note}')


def make_region_html(region, args):
    """Write region-specific HTML page."""

    latest = max(m.frame.index.max() for m in region.covid_metrics.values())
    doc = dominate.document(title=f'{region.name} ({latest.date()}) COVID-19')
    doc_url = urls.region_page(region)
    def doc_link(url): return urls.link(doc_url, url)

    with doc.head:
        style.add_head_style(doc_url)

    with doc.body:
        tags.attr(id='map_key_target', tabindex='-1')
        with tags.h2():
            def write_breadcrumbs(r):
                if r is not None:
                    write_breadcrumbs(r.parent)
                    tags.a(r.short_name, href=doc_link(urls.region_page(r)))
                    util.text(' » ')

            write_breadcrumbs(region.parent)
            util.text(region.short_name)
            if region.name != region.short_name:
                util.text(f' / {region.name}')
            util.text(f' (pop. {region.population:,.0f}) {latest.date()}')

        tags.img(cls='graphic', src=doc_link(urls.chart_image(region)))

        if has_map(region, args):
            with tags.div(cls='graphic'):
                with tags.video(width='100%', id='map', preload='auto'):
                    href = urls.link(doc_url, urls.map_video_maybe(region)))
                    tags.source(type='video/webm', src=f'{href}#t=1000');

                with tags.div(cls='video_controls'):
                    def i(n): return tags.i(cls=f'fas fa-{n}')
                    tags.button(
                        i('pause'),
                        ' ',
                        i('play'),
                        ' P',
                        id='map_play')
                    tags.button(i('repeat'), ' L', id='map_loop')
                    tags.button(i('backward'), ' R', id='map_rewind')
                    tags.button(i('step-backward'), ' [', id='map_prev')
                    tags.button(i('step-forward'), ' ]', id='map_next')
                    tags.button(i('forward'), ' F', id='map_forward')

        if region.daily_events:
            tags.h2(
                tags.span('Mitigation', cls='event_close'), ' and ',
                tags.span('Relaxation', cls='event_open'), ' Changes')

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
            subs = [r for r in region.subregions.values()
                    if r.matches_regex(args.page_filter)]
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

    with open(urls.file(args.site_dir, doc_url), 'w') as doc_file:
        doc_file.write(doc.render())


def make_thumb_link_html(doc_url, region):
    region_href = urls.link(doc_url, urls.region_page(region))
    with tags.a(cls='thumb', href=region_href):
        pp = f'pop. {region.population:,.0f}'
        tags.div(region.name, tags.div(pp, cls='thumb_pop'), cls='thumb_label')
        tags.img(width=200, src=urls.link(doc_url, urls.thumb_image(region)))


def has_map(region, args):
    return urls.has_map(region) and region.matches_regex(args.map_filter)


def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior
    parser = argparse.ArgumentParser(parents=[
        cache_policy.argument_parser, combine_data.argument_parser])
    parser.add_argument('--processes', type=int)
    parser.add_argument('--chunk_size', type=int)
    parser.add_argument('--site_dir', type=pathlib.Path,
                        default=pathlib.Path('site_out'))
    parser.add_argument('--page_filter')
    parser.add_argument('--map_filter')
    args = parser.parse_args()

    print('Loading data...')
    make_map.setup(args)
    world = combine_data.get_world(
        session=cache_policy.new_session(args), args=args, verbose=True)

    print('Enumerating regions...')

    def get_regions(r):
        if r.matches_regex(args.page_filter):
            yield r
        yield from (a for s in r.subregions.values() for a in get_regions(s))
    all_regions = list(get_regions(world))

    print(f'Generating {len(all_regions)} pages in {args.site_dir}...')
    style.write_style_files(args.site_dir)

    # Recurse for subregions.
    processes = args.processes or os.cpu_count() * 2
    chunk_size = args.chunk_size or max(1, len(all_regions) // (4 * processes))
    with multiprocessing.Pool(processes=args.processes) as pool:
        pool.starmap(
            make_region_page, ((r, args) for r in all_regions),
            chunksize=args.chunk_size)


if __name__ == '__main__':
    main()
