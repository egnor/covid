"""Module to retrieve COVID vaccination data from ourworldindata.org."""

import io

import pandas


REPO_DIR = 'https://raw.githubusercontent.com/owid/covid-19-data/master'
VACCINATIONS_DATA_DIR = f'{REPO_DIR}/public/data/vaccinations'
LOCATIONS_CSV_URL = f'{VACCINATIONS_DATA_DIR}/locations.csv'
VACCINATIONS_CSV_URL = f'{VACCINATIONS_DATA_DIR}/vaccinations.csv'
US_VACCINATIONS_CSV_URL = f'{VACCINATIONS_DATA_DIR}/us_state_vaccinations.csv'


def get_locations(session):
    loc_response = session.get(LOCATIONS_CSV_URL)
    loc_response.raise_for_status()
    loc_table = pandas.read_csv(io.StringIO(loc_response.text))
    loc_table.last_observation_date = pandas.to_datetime(
        loc_table.last_observation_date, utc=True)
    return loc_table


def get_vaccinations(session):
    vax_response = session.get(VACCINATIONS_CSV_URL)
    vax_response.raise_for_status()
    vax_table = pandas.read_csv(io.StringIO(vax_response.text))
    vax_table.rename(columns={'location': 'country'}, inplace=True)

    us_vax_response = session.get(US_VACCINATIONS_CSV_URL)
    us_vax_response.raise_for_status()
    us_vax_table = pandas.read_csv(io.StringIO(us_vax_response.text))
    us_vax_table.rename(columns={'location': 'state'}, inplace=True)
    us_vax_table['country'] = 'United States'
    us_vax_table['iso_code'] = 'USA'

    data_table = pandas.concat([vax_table, us_vax_table], ignore_index=True)
    data_table.date = pandas.to_datetime(data_table.date, utc=True)
    return data_table


def credits():
    return {
        'https://ourworldindata.org/covid-vaccinations':
        'Our World In Data'
    }


if __name__ == '__main__':
    import argparse
    import signal
    from covid import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print('=== locations ===')
    locations = get_locations(session)
    print(locations.dtypes)
    print()
    print('Arbitrary record:')
    print(locations.iloc[len(locations) // 2])
    print()

    print('=== vaccinations ===')
    vaccinations = get_vaccinations(session)
    print(vaccinations.dtypes)
    print()

    vaccinations.set_index(['iso_code', 'state', 'date'], inplace=True)
    vaccinations.sort_index(level=vaccinations.index.names, inplace=True)
    print('Arbitrary record:')
    print(vaccinations.iloc[len(vaccinations) // 2])
    print()
    print('Last all-USA record:')
    print(vaccinations.loc[('USA', None)].iloc[-1])
    print()
    print('Last California record:')
    print(vaccinations.loc[('USA', 'California')].iloc[-1])
