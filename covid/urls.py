# Shared definitions of file placement within the site

import os


def index_page():
    return 'index.html'


def region_prefix(region):
    return region.id.replace(' ', '_').replace('/', '_').lower() + '/'


def region_page(region):
    return region_prefix(region) + index_page()


def covid_plot(region):
    return region_prefix(region) + 'plot.png'


def covid_plot_thumb(region):
    return region_prefix(region) + 'thumb.png'


def mobility_plot(region):
    return region_prefix(region) + 'mobility.png'


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
