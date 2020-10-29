"""Module to retrieve data from the Unified COVID-Dataset dashboard."""

import io
import re
import tempfile

import pandas
import pyreadr

from covid.cache_policy import cached_path, temp_to_rename


REPO_DIR = 'https://github.com/hsbadr/COVID-19/raw/master'
LOOKUP_CSV_URL = f'{REPO_DIR}/COVID-19_LUT.csv'
COVID19_RDATA_URL = f'{REPO_DIR}/COVID-19.RData'
HYDROMET_RDATA_URL = f'{REPO_DIR}/Hydromet.RData'

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


def get_covid(session):
    """Returns a DataFrame of COVID-19 daily records."""

    cache_path = cached_path(session, f'{COVID19_RDATA_URL}.feather')
    if cache_path.exists():
        df = pandas.read_feather(cache_path)
    else:
        df = _read_rdata_url(session, COVID19_RDATA_URL)
        df.Date = pandas.to_datetime(df.Date, utc=True)
        for c in ('Cases', 'Cases_New'):
            df[c].fillna(0, inplace=True)
            df[c] = df[c].astype(int)
        with temp_to_rename(cache_path) as temp_path:
            df.to_feather(temp_path)

    key_columns = ['ID', 'Type', 'Source', 'Age', 'Sex', 'Date']
    df.sort_values(by=key_columns, inplace=True)
    df.set_index(key_columns, inplace=True)
    if df.index.duplicated().any():
        dups = df.index[df.index.duplicated(keep=False)]
        raise ValueError(
            'Dups:\n' + '\n'.join(', '.join(str(p) for p in d) for d in dups))

    return df


def get_hydromet(session):
    """Returns a DataFrame of hydrometeological daily records."""
    cache_path = cached_path(session, f'{HYDROMET_RDATA_URL}.feather')
    if cache_path.exists():
        df = pandas.read_feather(cache_path)
    else:
        df = _read_rdata_url(session, HYDROMET_RDATA_URL)
        df.Date = pandas.to_datetime(df.Date, utc=True)
        with temp_to_rename(cache_path) as temp_path:
           df.to_feather(temp_path)

    key_columns = ['ID', 'Date', 'Source']
    df.sort_values(by=key_columns, inplace=True)
    df.set_index(key_columns, inplace=True)
    if df.index.duplicated().any():
        dups = df.index[df.index.duplicated(keep=False)]
        raise ValueError(
            'Dups:\n' + '\n'.join(', '.join(str(p) for p in d) for d in dups))

    return df


def _read_rdata_url(session, url):
    """Downloads an RData file from an URL and returns it as a DataFrame."""

    response = session.get(url)
    response.raise_for_status()
    with tempfile.NamedTemporaryFile() as tf:
        tf.write(response.content)
        tf.flush()
        try:
            rdata = pyreadr.read_r(tf.name)
        except:
            raise ValueError(f'Error parsing RData: {url}')
        if len(rdata) != 1:
            objects = ', '.join(f'"{k}"' for k in rdata.keys())
            raise ValueError('Multiple R objects ({objects}): {url}')

        return next(iter(rdata.values()))


def credits():
    return {'https://github.com/hsbadr/COVID-19': 'Unified COVID-19 Dataset'}


if __name__ == '__main__':
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    session = cache_policy.new_session(parser.parse_args())
    print('COVID cache:', cached_path(session, f'{COVID19_RDATA_URL}.feather'))
    print('Hmet cache:', cached_path(session, f'{HYDROMET_RDATA_URL}.feather'))

    print()
    print('Loading places...')
    places = get_places(session)
    print('Loading COVID data...')
    covid = get_covid(session)
    print('Loading hydromet data...')
    hydromet = get_hydromet(session)

    print()
    print('=== COVID SOURCES ===')
    codes = {}
    for (source, type), source_data in covid.groupby(level=['Source', 'Type']):
        codes[(source, type)] = code = len(codes) + 1
        place_count = len(source_data.index.unique(level='ID'))
        print(f'{f"[{code}]":>4} {place_count:4d}p {source:<3} {type}')

    print()
    print('=== HYDROMET SOURCES ===')
    for source, source_data in hydromet.groupby(level=['Source']):
        codes[source] = code = len(codes) + 1
        place_count = len(source_data.index.unique(level='ID'))
        print(f'{f"<{code}>":>4} {place_count:4d}p {source:<3}')

    print()
    print('=== REGIONS ===')
    hydromet_by_id = hydromet.groupby(level='ID')
    for id, id_data in covid.groupby(level='ID'):
        p = places[id]
        days = id_data.index.unique(level='Date')
        refs = [codes[s] for s, d in id_data.groupby(level=['Source', 'Type'])]
        h_refs = ([codes[s] for s, h in
                   hydromet_by_id.get_group(id).groupby(level='Source')]
                  if (id in hydromet_by_id.groups) else [])

        line = f'{p.ID:<11} {p.Population:9d}p {len(days):>3d}d {p.ISO2_UID}'
        line += f' [{",".join(str(r) for r in refs)}]' if refs else ''
        line += f' <{",".join(str(h) for h in h_refs)}>' if h_refs else ''
        line += f' f={p.FIPS}' if p.FIPS else ''
        line += f' z={p.ZCTA}' if p.ZCTA else ''
        line += ' ' + ': '.join(a for a in (p.Admin2, p.Admin3) if a)
        print(line)
