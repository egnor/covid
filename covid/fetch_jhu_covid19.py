# Module to retrieve data from the JHU COVID-19 dashboard.
# (Can also be run as a standalone program for testing.)

import io
import re

import pandas


REPO_DIR = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19'
DATA_DIR = REPO_DIR + '/master/csse_covid_19_data'


_place_by_uid = None


def _fetch_csv(session, url):
    response = session.get(url)
    response.raise_for_status()
    return pandas.read_csv(
        io.StringIO(response.text), keep_default_na=False, na_values=[''])


def get_places(session):
    """Returns a dict mapping UID number to place metadata."""

    global _place_by_uid
    if _place_by_uid is None:
        places_url = DATA_DIR + '/UID_ISO_FIPS_LookUp_Table.csv'
        places = _fetch_csv(session, places_url)
        places.iso2.fillna('XX', inplace=True)
        places.Province_State.fillna('', inplace=True)
        places.Admin2.fillna('', inplace=True)
        places.FIPS.fillna(0, inplace=True)
        places.FIPS = places.FIPS.astype(int)
        places.UID = places.UID.astype(int)
        _place_by_uid = {
            place.UID: place
            for place in places.itertuples(index=False, name='Place')
        }

    return _place_by_uid


def get_data(session):
    """Returns a pandas.DataFrame of time series data from JHU."""

    lookup = {
        (p.Country_Region, p.Province_State.replace(' SAR', '')): p.UID
        for p in get_places(session).values()
    }

    def fetch_series(url, value_name):
        df = _fetch_csv(session, url)
        if 'UID' not in df.columns:
            cols = ['Country/Region', 'Province/State']
            for col in cols:
                df[col].fillna('', inplace=True)
            df['UID'] = [lookup[k] for k in df[cols].itertuples(index=False)]

        date_columns = [c for c in df.columns if re.match(r'\d+/\d+/\d+$', c)]
        df = df.melt('UID', date_columns, 'date', value_name)
        df.date = pandas.to_datetime(df.date, utc=True)
        df.sort_values(by=['UID', 'date'], inplace=True)
        df.set_index(['UID', 'date'], inplace=True)
        return df

    prefix = DATA_DIR + '/csse_covid_19_time_series/time_series_covid19_'
    global_cases = fetch_series(prefix + 'confirmed_global.csv', 'total_cases')
    global_deaths = fetch_series(prefix + 'deaths_global.csv', 'total_deaths')
    global_data = global_cases.join(global_deaths, how='outer')
    us_cases = fetch_series(prefix + 'confirmed_US.csv', 'total_cases')
    us_deaths = fetch_series(prefix + 'deaths_US.csv', 'total_deaths')
    us_data = us_cases.join(us_deaths, how='outer')
    all_data = pandas.concat([global_data, us_data])
    all_data.sort_index(inplace=True)
    return all_data


def attribution():
    return {'https://github.com/CSSEGISandData/COVID-19':
            'JHU CSSE COVID-19 Data'}


if __name__ == '__main__':
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    session = cache_policy.new_session(parser.parse_args())
    places = get_places(session)
    data = get_data(session)

    for uid, series in data.groupby(level='UID'):
        p = places[uid]
        name = ' / '.join(
            n for n in [p.Country_Region, p.Province_State, p.Admin2] if n)
        fips = f'[{p.FIPS}] ' if p.FIPS else ''
        print(f'{p.Population:9.0f}p {len(series):3d}d {fips}{name}')
