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
import warnings
from shapely.geometry.base import BaseMultipartGeometry

from covid import urls


FPS = 3

_admin_0_shapes = None
_admin_1_shapes = None
_water_shapes = None

_lat_lon_crs = cartopy.crs.PlateCarree()

_area_crs = cartopy.crs.Mollweide()


def setup(args, verbose=False):
    """Initialize cartopy globals from command line args."""

    vprint = lambda *a, **k: print(*a, **k) if verbose else None
    cartopy.config['data_dir'] = args.cache_dir / 'cartopy'

    def earth(category, name):
        vprint(f'Loading shapes: {category}/{name}')
        return list(cartopy.io.shapereader.Reader(
            cartopy.io.shapereader.natural_earth(
                resolution='50m', category=category, name=name)).records())

    global _admin_0_shapes, _admin_1_shapes, _water_shapes
    _admin_0_shapes = earth('cultural', 'admin_0_countries')
    _admin_1_shapes = earth('cultural', 'admin_1_states_provinces')
    _water_shapes = earth('physical', 'ocean') + earth('physical', 'lakes')


def write_video(region, site_dir, verbose=False):
    """Generates a map timeline video for the specified region."""

    vprint = lambda *a, **k: print(*a, **k) if verbose else None
    vprint(f'Creating map video: {region.path()}')

    subs = _mapped_subregions(region)
    max_time = max(m.frame.index.max() for m in region.map_metrics.values())
    d_m_r_v = list(
        (d, m_r_v)
        for d, m_r_v in _date_metric_region_value(subs).items()
        if datetime.date(2020, 3, 1) <= d.date() <= max_time.date()
    )

    if not d_m_r_v:
        warnings.warn(f'No metrics for map video: {region.path()}')
        return

    fig = matplotlib.pyplot.figure(figsize=(10, 6.5), dpi=150)
    axes = _setup_axes(fig, region)
    canvas = mplcairo.base.FigureCanvasCairo(fig)

    fig.tight_layout()
    fig.tight_layout()  # Needs to be called twice to fully settle??
    bbox = axes.get_tightbbox(canvas.get_renderer())

    def make_frame(t):
        frame = round(t * FPS)
        vprint(f'  {t:>5.2f}s: Frame {frame}')
        date, m_r_v = d_m_r_v[frame]
        prev_date, prev_m_r_v = d_m_r_v[frame - 1] if frame > 0 else (None, {})

        frame_arts = []
        frame_arts.append(axes.text(
            0.5, 0.5, f'{date.date()}', transform=axes.transAxes,
            fontsize=55, fontweight='bold', alpha=0.2,
            ha='center', va='center'))

        lons, lats = zip(*(r.lat_lon for r in subs))
        scale = 3e4 / region.totals['population']
        pop_sizes = [scale * r.totals['population'] for r in subs]
        frame_arts.append(axes.scatter(
            x=lats, y=lons, s=pop_sizes, color=(0.0, 0.0, 0.0, 0.1),
            transform=_lat_lon_crs, zorder=2.1))

        # Use the main region's map metrics to define ordering and color.
        for name, metric in region.map_metrics.items():
            now_r_v, prev_r_v = m_r_v.get(name, {}), prev_m_r_v.get(name, {})
            now_areas = [scale * max(0, now_r_v.get(r, 0)) for r in subs]
            prev_areas = [scale * max(0, prev_r_v.get(r, 0)) for r in subs]
            frame_arts.append(axes.scatter(
                x=lats, y=lons, zorder=2.2, transform=_lat_lon_crs,
                s=[min(n, p) for n, p in zip(now_areas, prev_areas)],
                color=metric.color, edgecolors='none'))

            now_radii = [0.5 * (a ** 0.5) for a in now_areas]
            prev_radii = [0.5 * (a ** 0.5) for a in prev_areas]
            ring_widths = [n - p for n, p in zip(now_radii, prev_radii)]
            ring_areas = [(n + p) ** 2 for n, p in zip(now_radii, prev_radii)]
            if metric.increase_color:
                frame_arts.append(axes.scatter(
                    x=lats, y=lons, zorder=2.2, transform=_lat_lon_crs,
                    s=ring_areas, linewidths=[max(0, w) for w in ring_widths],
                    color='none', edgecolors=metric.increase_color))

            if metric.decrease_color:
                frame_arts.append(axes.scatter(
                    x=lats, y=lons, zorder=2.2, transform=_lat_lon_crs,
                    s=ring_areas, linewidths=[max(0, -w) for w in ring_widths],
                    color='none', edgecolors=metric.decrease_color))

        canvas.draw()
        [a.remove() for a in frame_arts]
        return _rgb_from_canvas(canvas.get_renderer(), bbox)

    seconds = (len(d_m_r_v) - 0.5) / FPS
    vprint(f'Generating {len(d_m_r_v)} frames ({seconds:.2f}s * {FPS}fps)...')

    file_path = urls.file(site_dir, urls.map_video_maybe(region))
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = file_path.parent / ('tmp.' + file_path.name)
    params = '-c:v vp9 -b:v 0 -pix_fmt yuv420p -quality good -speed 0'
    clip = moviepy.video.VideoClip.VideoClip(make_frame, duration=seconds)
    clip.set_fps(FPS).write_videofile(
        str(temp_path), logger=None, ffmpeg_params=params.split())

    temp_path.rename(file_path)
    vprint(f'Saved map video: {file_path}')
    matplotlib.pyplot.close(fig)  # Reclaim memory.


def _mapped_subregions(region):
    # Walk at most 2 layers down to find regions plot on the map.
    return [r for s in region.subregions.values()
            for r in (s.subregions.values() if urls.has_map(s) else (s,))
            if r.map_metrics and r.lat_lon]


def _date_metric_region_value(regions):
    d_m_r_v = {}
    for r in regions:
        for n, m in r.map_metrics.items():
            for t in m.frame.itertuples():
                if pandas.notna(t.value):
                    r_v = d_m_r_v.setdefault(t.Index, {}).setdefault(n, {})
                    r_v[r] = max(0, t.value)

    return {d: m_r_v for d, m_r_v in sorted(d_m_r_v.items())}


def _setup_axes(figure, region):
    def get_path(r):
        return [] if not r else get_path(r.parent) + [r]
    region_path = get_path(region)
    a0_region, = region_path[1:2] or [None]
    a1_region, = region_path[2:3] or [None]

    a0_region_shapes = [
        s for s in _admin_0_shapes if a0_region and
        s.attributes['ISO_A2'] == a0_region.iso_code]
    a0_region_a1_shapes = [
        s for s in _admin_1_shapes if a0_region and
        s.attributes['iso_a2'] == a0_region.iso_code]
    a1_region_shapes = [
        s for s in _admin_1_shapes if a1_region and
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

    add_shapes(_water_shapes, ec='none', fc='0.9')
    add_shapes(_admin_0_shapes, lw=0.5)
    add_shapes(a0_region_a1_shapes, lw=0.5)
    add_shapes(a1_region_shapes or a0_region_shapes, lw=1)

    L2D = matplotlib.lines.Line2D
    axes.legend(loc='lower left', handles=[
        L2D([], [], color=(0.0, 0.0, 0.0, 0.1),
            ls='none', marker='o', ms=12, label='population'),
        L2D([], [], mfc=(0.0, 0.0, 1.0, 0.2), mec=(0.0, 0.0, 1.0, 0.6), mew=2,
            ls='none', marker='o', ms=11, label='positives x2K (incr.)'),
        L2D([], [], mfc=(0.0, 0.0, 1.0, 0.2), mec=(0.0, 1.0, 0.0, 0.6), mew=2,
            ls='none', marker='o', ms=11, label='positives x2K (decr.)'),
        L2D([], [], color=(1.0, 0.0, 0.0, 0.2), mec=(1.0, 0.0, 0.0, 0.6),
            mew=2, ls='none', marker='o', ms=11, label='deaths x200K')])

    return axes


def _rgb_from_canvas(renderer, bbox):
    x_min, x_max = max(0, math.floor(bbox.x0)), max(0, math.ceil(bbox.x1))
    y_min, y_max = max(0, math.floor(bbox.y0)), max(0, math.ceil(bbox.y1))
    bgra = renderer._get_buffer()
    return bgra[-y_max:-y_min, x_min:x_max, [2, 1, 0]]


if __name__ == '__main__':
    import argparse
    import signal
    from covid import cache_policy
    from covid import combine_data
    from pathlib import Path

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior.
    arg_parents = [cache_policy.argument_parser, combine_data.argument_parser]
    parser = argparse.ArgumentParser(parents=arg_parents)
    parser.add_argument('--region', required=True)
    parser.add_argument('--site_dir', type=Path, default=Path('site_out'))
    args = parser.parse_args()
    setup(args, verbose=True)

    session = cache_policy.new_session(args)
    world = combine_data.get_world(session=session, args=args, verbose=True)
    region = world.lookup_path(args.region)
    write_video(region=region, site_dir=args.site_dir, verbose=True)
