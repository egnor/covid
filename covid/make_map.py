"""Functions to generate maps based on region metrics."""

import cartopy
import cartopy.crs
import cartopy.io.shapereader
import collections
import datetime
import math
import matplotlib
import matplotlib.figure
import matplotlib.lines
import matplotlib.pyplot
import moviepy.video.io.bindings
import moviepy.video.VideoClip
import mplcairo.base
import numpy
import pandas
from shapely.geometry.base import BaseMultipartGeometry

from covid import urls


FPS = 3

_BaseMapShapes = collections.namedtuple(
    '_BaseMapShapes', 'admin_0 admin_1 water')

_base_map_shapes = None

_lat_lon_crs = cartopy.crs.PlateCarree()

_area_crs = cartopy.crs.Mollweide()


def setup(args):
    """Initialize cartopy globals from command line args."""

    cartopy.config['data_dir'] = args.cache_dir / 'cartopy'

    def earth(category, name):
        return list(cartopy.io.shapereader.Reader(
            cartopy.io.shapereader.natural_earth(
                resolution='50m', category=category, name=name)).records())

    global _base_map_shapes
    _base_map_shapes = _BaseMapShapes(
        admin_0=earth('cultural', 'admin_0_countries'),
        admin_1=earth('cultural', 'admin_1_states_provinces'),
        water=earth('physical', 'ocean') + earth('physical', 'lakes'))


def write_video(region, site_dir):
    date_region_metrics = list(_date_region_metrics(region).items())

    fig = matplotlib.pyplot.figure(figsize=(10, 10), dpi=150)
    axes = _setup_axes(fig, region)
    canvas = mplcairo.base.FigureCanvasCairo(fig)

    fig.tight_layout()
    fig.tight_layout()  # Needs to be called twice to fully settle??
    bbox = axes.get_tightbbox(canvas.get_renderer())

    def make_frame(t):
        frame_arts = []
        date, region_metrics = date_region_metrics[round(t * FPS)]
        frame_arts.append(axes.text(
            0.5, 0.5, f'{date.date()}', transform=axes.transAxes,
            fontsize=55, fontweight='bold', alpha=0.2,
            ha='center', va='center'))

        lons, lats = zip(*(r.lat_lon for r in region_metrics.keys()))
        size_per_pop = 3e4 / region.population
        frame_arts.append(axes.scatter(
            x=lats, y=lons,
            s=[r.population * size_per_pop for r in region_metrics.keys()],
            color=(0.0, 0.0, 0.0, 0.1), transform=_lat_lon_crs, zorder=2.1))

        # Use main region map metrics as a guide to ordering and color.
        for name, metric in region.map_metrics.items():
            frame_arts.append(axes.scatter(
                x=lats, y=lons, s=[
                    m.get(name, 0) * size_per_pop
                    for r, m in region_metrics.items()],
                color=metric.color, transform=_lat_lon_crs, zorder=2.2))

        canvas.draw()
        [a.remove() for a in frame_arts]
        return _rgb_from_canvas(canvas.get_renderer(), bbox)

    duration = (len(date_region_metrics) - 0.5) / FPS
    clip = moviepy.video.VideoClip.VideoClip(make_frame, duration=duration)
    clip.set_fps(FPS).write_videofile(
        str(urls.file(site_dir, urls.map_video_maybe(region))), logger=None,
        ffmpeg_params=(
            '-c:v vp9 -b:v 0 -pix_fmt yuv420p '
            '-quality good -speed 0').split())

    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _date_region_metrics(region):
    # Walk at most 2 layers down to find regions to map.
    subregions = [
        r for s in region.subregions.values()
        for r in (s.subregions.values() if urls.has_map(s) else (s,))
        if r.map_metrics]

    date_region_metrics = {}
    for r in subregions:
        for n, m in r.map_metrics.items():
            for t in m.frame.itertuples():
                if pandas.notna(t.value):
                    date_region_metrics.setdefault(
                        t.Index, {}).setdefault(r, {})[n] = max(0, t.value)

    return {
        d: {r: r_m.get(r, {}) for r in subregions}
        for d, r_m in sorted(date_region_metrics.items())
        if d.date() >= datetime.date(2020, 3, 1)
    }


def _setup_axes(figure, region):
    def get_path(r):
        return [] if not r else get_path(r.parent) + [r]
    region_path = get_path(region)
    a0_region, = region_path[1:2] or [None]
    a1_region, = region_path[2:3] or [None]

    a0_region_shapes = [
        s for s in _base_map_shapes.admin_0 if a0_region and
        s.attributes['ISO_A2'] == a0_region.iso_code]
    a0_region_a1_shapes = [
        s for s in _base_map_shapes.admin_1 if a0_region and
        s.attributes['iso_a2'] == a0_region.iso_code]
    a1_region_shapes = [
        s for s in _base_map_shapes.admin_1 if a1_region and
        s.attributes['iso_a2'] == a0_region.iso_code and (
            s.attributes['fips'] == 'US{a1_region.fips_code:02}' or
            s.attributes['name'] == a1_region.name or
            s.attributes['name'] == a1_region.short_name or
            s.attributes['abbrev'].strip('.') == a1_region.short_name or
            s.attributes['postal'] == a1_region.short_name)]

    if region.name == 'World':
        # Special projection for the whole world
        axes = figure.add_subplot(projection=cartopy.crs.Robinson())
        axes.set_box_aspect(0.505)
        axes.set_extent((-179, 179, -89, 89), _lat_lon_crs)
    else:
        def m(g): return isinstance(g, BaseMultipartGeometry)
        def split(g): return (p for s in g for p in split(s)) if m(g) else (g,)
        def area(g): return _area_crs.project_geometry(g, _lat_lon_crs).area

        shapes = a1_region_shapes or a0_region_shapes
        parts = (
            (s.geometry for s in shapes) if region.fips_code == 15 else  # HI
            (p for s in shapes for p in split(s.geometry)))

        main_area, main = max((area(p), p) for p in parts)
        (center_lon, center_lat), = main.centroid.coords
        axes = figure.add_subplot(projection=cartopy.crs.Orthographic(
            central_longitude=center_lon, central_latitude=center_lat))

        main_projected = axes.projection.project_geometry(main, _lat_lon_crs)
        x1, y1, x2, y2 = main_projected.bounds
        xp, yp = (x2 - x1) / 10, (y2 - y1) / 10
        axes.set_extent((x1 - xp, x2 + xp, y1 - yp, y2 + yp), axes.projection)
        axes.set_box_aspect(0.618)

    def add_shapes(shapes, **kwargs):
        axes.add_geometries(
            (s.geometry for s in shapes),
            **{'crs': _lat_lon_crs, 'ec': 'black', 'fc': 'none', **kwargs})

    add_shapes(_base_map_shapes.water, ec='none', fc='0.9')
    add_shapes(_base_map_shapes.admin_0, lw=0.5)
    add_shapes(a0_region_a1_shapes, lw=0.5)
    add_shapes(a1_region_shapes or a0_region_shapes, lw=1)

    L2D = matplotlib.lines.Line2D
    axes.legend(loc='lower left', handles=[
        L2D([], [], color=(0.0, 0.0, 0.0, 0.1),
            ls='none', marker='o', ms=15, label='population'),
        L2D([], [], color=(0.0, 0.0, 1.0, 0.2),
            ls='none', marker='o', ms=15, label='positives x2K'),
        L2D([], [], color=(1.0, 0.0, 0.0, 0.2),
            ls='none', marker='o', ms=15, label='deaths x200K')])

    return axes


def _rgb_from_canvas(renderer, bbox):
    x_min, x_max = max(0, math.floor(bbox.x0)), max(0, math.ceil(bbox.x1))
    y_min, y_max = max(0, math.floor(bbox.y0)), max(0, math.ceil(bbox.y1))
    bgra = renderer._get_buffer()
    return bgra[-y_max:-y_min, x_min:x_max, [2, 1, 0]]
