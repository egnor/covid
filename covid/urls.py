"""Shared definitions of file placement within the static site."""

import os
import re


def _prefix(r_or_p):
    path = r_or_p.path if hasattr(r_or_p, "path") else r_or_p
    return "".join(
        re.sub(r"[\W]+", "_", p).strip("_").lower() + "/" for p in path[1:]
    )


def region_page(region_or_path):
    return _prefix(region_or_path) + "index.html"


def thumb_image(region_or_path):
    return _prefix(region_or_path) + "thumb.png"


def chart_image(region_or_path):
    return _prefix(region_or_path) + "chart.png"


def has_map(region):
    rp = region.metrics.total["population"]
    non_biggest_pop = sum(
        s.metrics.total["population"]
        for s in region.subregions.values()
        if s.metrics.map
        and (s.metrics.total["population"] < 0.5 * rp or has_map(s))
    )
    return len(region.subregions) >= 3 and non_biggest_pop >= 0.1 * rp


def map_video_maybe(region):
    return _prefix(region) + "map.webm" if has_map(region) else None


def link(from_urlpath, to_urlpath):
    """Returns the relative URL to get from from_urlpath to to_urlpath."""

    if to_urlpath[:1] == "/" or "/" not in from_urlpath:
        return to_urlpath
    if from_urlpath[:1] == "/":
        return "/" + to_urlpath
    if "/" in to_urlpath:
        f0, f1 = from_urlpath.split("/", 1)
        t0, t1 = to_urlpath.split("/", 1)
        if f0 == t0:
            return link(f1, t1)
    return ("../" * from_urlpath.count("/")) + to_urlpath


def file(site_dir, urlpath):
    """Returns the file path within site_dir corresponding to urlpath,
    creating parent directories as needed."""

    filepath = site_dir / urlpath.strip("/")
    os.makedirs(filepath.parent, exist_ok=True)
    return filepath
