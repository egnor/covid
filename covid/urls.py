"""Shared definitions of file placement within the static site."""

import os
import re


def index_page():
    return 'index.html'


def region_prefix(region, _tr=str.maketrans(' /.', '___')):
    return (
        '' if not region else
        '' if region.short_name.lower() == 'world' and not region.parent else
        region_prefix(region.parent) +
        re.sub(r'[\W]+', '_', region.short_name).strip('_').lower() + '/')


def region_page(region):
    return region_prefix(region) + index_page()


def thumb_image(region):
    return region_prefix(region) + 'thumb.png'


def chart_image(region):
    return region_prefix(region) + 'chart.png'


def map_image_maybe(region):
    pop = region.population
    sub_pop = sum(
        s.population for s in region.subregions.values()
        if s.lat_lon and (s.population < 0.5 * pop or map_image_maybe(s)))
    return (
        None if len(region.subregions) < 4 or sub_pop < 0.1 * pop else
        region_prefix(region) + 'map.png')


def link(from_urlpath, to_urlpath):
    """Returns the relative URL to get from from_urlpath to to_urlpath."""

    if to_urlpath[:1] == '/' or '/' not in from_urlpath:
        return to_urlpath
    if from_urlpath[:1] == '/':
        return '/' + to_urlpath
    if '/' in to_urlpath:
        f0, f1 = from_urlpath.split('/', 1)
        t0, t1 = to_urlpath.split('/', 1)
        if f0 == t0:
            return link(f1, t1)
    return ('../' * from_urlpath.count('/')) + to_urlpath


def file(site_dir, urlpath):
    """Returns the file path within site_dir corresponding to urlpath,
    creating parent directories as needed."""

    filepath = site_dir / urlpath.strip('/')
    os.makedirs(filepath.parent, exist_ok=True)
    return filepath
