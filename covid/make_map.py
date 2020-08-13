"""Functions to generate maps based on region metrics."""

import cartopy
import cartopy.crs
import cartopy.io.shapereader
import collections
import matplotlib
import matplotlib.figure
import matplotlib.pyplot
import moviepy.video.io.bindings
import moviepy.video.VideoClip
import numpy
import shapely.geometry.base

from covid import urls


FPS = 10

_FrameSpec = collections.namedtuple(
    '_FrameSpec', 'date region_color')

_BaseMapShapes = collections.namedtuple(
    '_BaseMapShapes', 'admin_0 admin_1 water')

_base_map_shapes = None

_lat_lon_crs = cartopy.crs.PlateCarree()

_equal_area_crs = cartopy.crs.Mollweide()


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
    map_url = urls.map_video_maybe(region)
    if not map_url:
        return  # No map for this region.

    # Walk at most 2 layers down to find regions to map.
    plot_regions = [
        m for s1 in region.subregions.values()
        for m in (
            (s2 for s2 in s1.subregions.values() if s2.lat_lon)
            if urls.map_video_maybe(s1) else (s1,))]

    frame_specs = _make_frame_specs(plot_regions)

    figure = matplotlib.pyplot.figure(
        figsize=(10, 10), dpi=100, tight_layout=True)

    axes = _make_axes(figure, region)

    def make_frame(t):
        spec = frame_specs[round(t * FPS)]
        pop_size = 2e4 / sum(m.population for m in spec.region_color.keys())
        circles = axes.scatter(
            x=[m.lat_lon[1] for m in spec.region_color.keys()],
            y=[m.lat_lon[0] for m in spec.region_color.keys()],
            s=[m.population * pop_size for m in spec.region_color.keys()],
            c=list(spec.region_color.values()),
            transform=_lat_lon_crs, zorder=2.5)

        # TODO WORK WITH axes.get_tightbbox()
        image = moviepy.video.io.bindings.mplfig_to_npimage(figure)
        circles.remove()
        return image

    duration = (len(frame_specs) - 0.5) / FPS
    clip = moviepy.video.VideoClip.VideoClip(make_frame, duration=duration)
    clip.set_fps(FPS).write_videofile(
        str(urls.file(site_dir, map_url)),
        ffmpeg_params=[
            '-c:v', 'vp9', '-b:v', '0', '-quality', 'good', '-speed', '0',
            '-pix_fmt', 'yuv420p'],
        logger=None)

    matplotlib.pyplot.close(figure)  # Reclaim memory.


def _make_frame_specs(regions):
    return [
        _FrameSpec(
            date=None,
            region_color={r: (0.0, 0.0, 0.0, 0.1) for r in regions})
    ]


def _make_axes(figure, region):
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
        BMG = shapely.geometry.base.BaseMultipartGeometry
        mult = lambda g: isinstance(g, BMG)
        split = lambda g: (p for s in g for p in split(s)) if mult(g) else (g,)
        geoms = (s.geometry for s in (a1_region_shapes or a0_region_shapes))
        parts = (p for g in geoms for p in split(g))

        area = lambda g: _equal_area_crs.project_geometry(g, _lat_lon_crs).area
        main_area, main = max((area(p), p) for p in parts)
        (center_lon, center_lat), = main.centroid.coords
        axes = figure.add_subplot(projection=cartopy.crs.Orthographic(
            central_longitude=center_lon, central_latitude=center_lat))

        main_projected = axes.projection.project_geometry(main, _lat_lon_crs)
        x1, y1, x2, y2 = main_projected.bounds
        xp, yp = (x2 - x1) / 10, (y2 - y1) / 10
        axes.set_extent((x1 - xp, x2 + xp, y1 - yp, y2 + yp), axes.projection)

    def add_shapes(shapes, **kwargs):
        axes.add_geometries(
            (s.geometry for s in shapes),
            **{'crs': _lat_lon_crs, 'ec': 'black', 'fc': 'none', **kwargs})

    add_shapes(_base_map_shapes.water, ec='none', fc='0.9')
    add_shapes(_base_map_shapes.admin_0, lw=0.5)
    add_shapes(a0_region_a1_shapes, lw=0.5)
    add_shapes(a1_region_shapes or a0_region_shapes, lw=1)
    return axes
