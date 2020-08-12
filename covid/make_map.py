"""Functions to generate maps based on region metrics."""

import cartopy
import cartopy.crs
import cartopy.io.shapereader
import collections
import matplotlib
import matplotlib.figure
import matplotlib.pyplot
import numpy
import shapely.geometry.multipolygon

from covid import urls


_BaseMapShapes = collections.namedtuple(
    '_BaseMapShapes', 'admin_0 admin_1 water')

_base_map_shapes = None

_geo_crs = cartopy.crs.PlateCarree()


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


def write_image(region, site_dir):
    map_url = urls.map_image_maybe(region)
    if not map_url:
        return  # No map for this region.

    # Walk at most 2 layers down to find regions to map.
    mappable_regions = [
        m for s1 in region.subregions.values()
        for m in (
            (s2 for s2 in s1.subregions.values() if s2.lat_lon)
            if urls.map_image_maybe(s1) else (s1,))]

    n = len(mappable_regions)
    lats = numpy.fromiter((m.lat_lon[0] for m in mappable_regions), float, n)
    lons = numpy.fromiter((m.lat_lon[1] for m in mappable_regions), float, n)
    pops = numpy.fromiter((m.population for m in mappable_regions), int, n)
    pop_sum = pops.sum()

    if (region.iso_code or '').lower() == 'us':
        # Special case bounds to show the continental US
        min_lat, max_lat, min_lon, max_lon = (25.8, 49.5, -124.5, -66.9)
    else:
        lat_ord, lon_ord = numpy.argsort(lats), numpy.argsort(lons)
        cuts = numpy.array((0, .01, .05, .95, .99, 1)) * pop_sum
        latp = numpy.interp(cuts, pops[lat_ord].cumsum(), lats[lat_ord])
        lonp = numpy.interp(cuts, pops[lon_ord].cumsum(), lons[lon_ord])
        min_lat, max_lat, min_lon, max_lon = (
            (max if p100 > p0 else min)(
                p0 - (p100 - p0) / 10,
                p1 - (p99 - p1) / 2,
                p5 - (p95 - p5) / 1)
            for p0, p1, p5, p95, p99, p100 in (
                latp, latp[::-1], lonp, lonp[::-1]))

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

    figure = matplotlib.pyplot.figure(figsize=(10, 10), tight_layout=True)
    if max_lon - min_lon < 170 and max_lat - min_lat < 170:
        axes = figure.add_subplot(projection=cartopy.crs.Orthographic(
            central_longitude=(min_lon + max_lon) / 2,
            central_latitude=(min_lat + max_lat) / 2))
        axes.set_extent((min_lon, max_lon, min_lat, max_lat))
    else:
        axes = figure.add_subplot(projection=cartopy.crs.Robinson())

    _add_shapes(axes, _base_map_shapes.water, ec='none', fc='0.9')
    _add_shapes(axes, _base_map_shapes.admin_0, lw=0.5)
    _add_shapes(axes, a0_region_a1_shapes, lw=0.5)
    _add_shapes(axes, a1_region_shapes or a0_region_shapes, lw=1)

    norm_pops = pops / pop_sum
    axes.scatter(lons, lats, norm_pops * 2e4, alpha=0.2, c='k',
                 transform=_geo_crs, zorder=2.5)

    axes.set_aspect('equal', 'datalim', 'C')
    figure.savefig(urls.file(site_dir, map_url), dpi=200)
    matplotlib.pyplot.close(figure)  # Reclaim memory.


def _add_shapes(axes, shapes, **kwargs):
    axes.add_geometries(
        (s.geometry for s in shapes),
        **{'crs': _geo_crs, 'ec': 'black', 'fc': 'none', **kwargs})
