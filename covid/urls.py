# Shared definitions of file placement within the site

import os


def home_page():
    return ''


def region_page(region):
    return region.id.replace(' ', '_').replace('/', '_').lower() + '/'

def region_plot(region):
    return f'{region_page(region)}plot.png'

def region_thumb(region):
    return f'{region_page(region)}thumb.png'


def link(from_urlpath, to_urlpath):
    if to_urlpath[:1] == '/' or '/' not in from_urlpath:
        return to_urlpath
    if from_urlpath[:1] == '/':
        return '/' + to_urlpath
    if '/' in to_urlpath:
        f0, f1 = from_urlpath.split('/', 1)
        t0, t1 = to_urlpath.split('/', 1)
        if f0 == t0:
            return url_jump(f1, t1)
    return ('../' * from_urlpath.count('/')) + to_urlpath


def file(site_dir, urlpath):
    """Returns the file path within site_dir corresponding to urlpath,
    creating parent directories as needed."""

    index = ('index.html' if urlpath[-1:] in ('/', '') else '')
    filepath = site_dir / (urlpath + index).strip('/')
    os.makedirs(filepath.parent, exist_ok=True)
    return filepath
