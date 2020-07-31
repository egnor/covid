# Module to retrieve data from the JHU COVID-19 dashboard.
# (Can also be run as a standalone program for testing.)

import io

import pandas



def get_data(session):
    """Returns a pandas.DataFrame of time series data from JHU."""

    def fetch_csv(url):
        response = session.get(url)
        response.raise_for_status()
        return pandas.read_csv(io.StringIO(response.text))

    repo_dir = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19'
    data_dir = repo_dir + '/master/csse_covid_19_data'
    lookup_table = fetch_csv(data_dir + '/UID_ISO_FIPS_LookUp_Table.csv')

    series_prefix = data_dir + '/csse_covid_19_time_series/time_series_'
    us_cases = fetch_csv(series_prefix + 'covid19_confirmed_US.csv')
    us_deaths = fetch_csv(series_prefix + 'covid19_deaths_US.csv')
    global_cases = fetch_csv(series_prefix + 'covid19_confirmed_global.csv')
    global_deaths = fetch_csv(series_prefix + 'covid19_deaths_global.csv')
    return lookup_table


def attribution():
    return {'https://github.com/CSSEGISandData/COVID-19':
            'JHU CSSE COVID-19 Data'}


if __name__ == '__main__':
    import argparse
    import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    data = get_data(session=cache_policy.new_session(parser.parse_args()))
    print(data.dtypes)
    print()
    print('Arbitrary record:')
    print(data.iloc[len(data) // 2])
