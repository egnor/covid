"""Build a basic hierarchy of place names to add COVID metrics to."""


import pycountry

from covid import fetch_jhu_csse
from covid.region_data import Region
from covid.region_data import RegionAtlas


def _get_sub(parent, key, name=None):
    """Get or add a subregion by key, setting names with defaults."""

    key = str(key)
    region = parent.subregions.get(key)
    if not region:
        region = parent.subregions[key] = Region(
            name=name or key, short_name=key, parent=parent
        )
    return region


def get_atlas(session):
    """Returns an RegionAtlas populated with places."""

    atlas = RegionAtlas()
    atlas.world = Region(name="World", short_name="World")
    for p in fetch_jhu_csse.get_places(session).itertuples(name="Place"):
        if not (p.Population > 0):
            continue  # Analysis requires population data.

        try:
            # Put territories under the parent, even with their own ISO codes
            iso2 = pycountry.countries.lookup(p.Country_Region).alpha_2
        except LookupError:
            iso2 = p.iso2

        region = _get_sub(atlas.world, iso2, p.Country_Region)
        region.iso_code = iso2

        if p.Province_State:
            region = _get_sub(region, p.Province_State)
            if p.iso2 != iso2:
                region.iso_code = p.iso2  # Must be for a territory

        if p.FIPS in (36005, 36047, 36061, 36081, 36085):
            region = _get_sub(region, "NYC", "New York City")
        elif p.FIPS in (49003, 49005, 49033):
            region = _get_sub(region, "Bear River", "Bear River Area")
        elif p.FIPS in (49023, 49027, 49039, 49041, 49031, 49055):
            region = _get_sub(region, "Central Utah", "Central Utah Area")
        elif p.FIPS in (49007, 49015, 49019):
            region = _get_sub(region, "Southeast Utah", "Southeast Utah Area")
        elif p.FIPS in (49001, 49017, 49021, 49025, 49053):
            region = _get_sub(region, "Southwest Utah", "Southwest Utah Area")
        elif p.FIPS in (49009, 49013, 49047):
            region = _get_sub(region, "TriCounty", "TriCounty Area")
        elif p.FIPS in (49057, 49029):
            region = _get_sub(region, "Weber-Morgan", "Weber-Morgan Area")

        if p.Admin2:
            region = _get_sub(region, p.Admin2)

        if p.FIPS:
            region.fips_code = int(p.FIPS)

        region.place_id = p.Index
        region.totals["population"] = p.Population
        if p.Lat or p.Long_:
            region.lat_lon = (p.Lat, p.Long_)

    # Initialize world population for direct world metrics
    atlas.world.totals["population"] = sum(
        sub.totals["population"] for sub in atlas.world.subregions.values()
    )

    # Index by various forms of ID for merging data in.
    def index_region_tree(r):
        for index_dict, key in [
            (atlas.by_iso2, r.iso_code),
            (atlas.by_fips, r.fips_code),
            (atlas.by_jhu_id, r.place_id),
        ]:
            if key is not None:
                index_dict[key] = r
        for sub in r.subregions.values():
            index_region_tree(sub)

    index_region_tree(atlas.world)
    return atlas


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading place data...")
    atlas = get_atlas(session)
    print(atlas.world.debug_tree())
