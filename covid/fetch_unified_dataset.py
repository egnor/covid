"""Module to retrieve data from the Unified COVID-Dataset dashboard."""

import io
import re
import tempfile

import pandas
import pyreadr

from covid import cache_policy


REPO_DIR = 'https://github.com/hsbadr/COVID-19/blob/master'
LOOKUP_CSV_URL = f'{REPO_DIR}/COVID-19_LUT.csv?raw=true'
COVID19_RDATA_URL = f'{REPO_DIR}/COVID-19.RData?raw=true'

_place_by_id = None


def get_places(session):
    """Returns a dict mapping UID number to place metadata."""

    global _place_by_id
    if _place_by_id is None:
        response = session.get(LOOKUP_CSV_URL)
        response.raise_for_status()
        places = pandas.read_csv(
            io.StringIO(response.text),
            keep_default_na=False, na_values={
                f: [''] for f in ('Latitude', 'Longitude', 'Population')})

        places.Population.fillna(0, inplace=True)
        places.Population = places.Population.astype(int)
        _place_by_id = {
            place.ID: place
            for place in places.itertuples(index=False, name='Place')
        }

    return _place_by_id


def get_data(session):
    url = COVID19_RDATA_URL
    csv_cache = cache_policy.cached_derived_data_path(session, url)
    if csv_cache.exists():
        df = pandas.read_csv(csv_cache, keep_default_na=False, na_values=[''])
    else:
        response = session.get(url)
        response.raise_for_status()

        csv_cache.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(response.content)
            temp.flush()
            df = pyreadr.read_r(temp.name)['COVID19']

        temp = csv_cache.parent / ('tmp.' + csv_cache.name)
        df.to_csv(temp, index=False)
        temp.rename(csv_cache)

    df.Date = pandas.to_datetime(df.Date, utc=True)
    for c in ('Cases', 'Cases_New'):
        df[c].fillna(0, inplace=True)
        df[c] = df[c].astype(int)

    key_columns = ['ID', 'Type', 'Source', 'Age', 'Sex', 'Date']
    df.sort_values(by=key_columns, inplace=True)
    df.set_index(key_columns, inplace=True)
    return df


def credits():
    return {'https://github.com/hsbadr/COVID-19': 'Unified COVID-19 Dataset'}


if __name__ == '__main__':
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    session = cache_policy.new_session(parser.parse_args())
    places = get_places(session)
    data = get_data(session)

    print('=== SOURCES ===')
    codes = {}
    for (source, type), origin_data in data.groupby(level=['Source', 'Type']):
        codes[(source, type)] = code = len(codes) + 1
        place_count = len(origin_data.index.unique(level='ID'))
        print(f'{f"[{code}]":>4} {place_count:4d}p {source:<3} {type}')

    print()
    print('=== REGIONS ===')
    for id, id_data in data.groupby(level='ID'):
        p = places[id]
        days = id_data.index.unique(level='Date')
        refs = [codes[s] for s, d in id_data.groupby(level=['Source', 'Type'])]

        line = f'{p.ID:<7} {p.Population:9d}p {len(days):>3d}d {p.ISO2_UID}'
        line += f' [{",".join(str(r) for r in refs)}]'
        line += f' f={p.FIPS}' if p.FIPS else ''
        line += f' z={p.ZCTA}' if p.ZCTA else ''
        line += ' ' + ': '.join(a for a in (p.Admin2, p.Admin3) if a)
        print(line)
