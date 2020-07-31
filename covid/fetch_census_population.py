# Module to retrieve Census population data.
# (Can also be run as a standalone program for testing.)

import json

import pandas


URL_BASE = 'https://api.census.gov/data/2019/pep/population'
API_KEY = 'c0e3aa89dc3a4a7f3be500700f83c292e5556024'


def get_states(session):
    """Returns a pandas.DataFrame of state-level population data."""

    response = session.get(
        f'{URL_BASE}?get=NAME,POP&for=state:*&key={API_KEY}')
    response.raise_for_status()

    json_data = json.loads(response.text)
    data = pandas.DataFrame(json_data[1:], columns=json_data[0])
    data.POP = data.POP.astype(int)
    data.state = data.state.astype(int)
    data.set_index('state', inplace=True)
    data.sort_index(inplace=True)
    return data


def attribution():
    return {
        'https://www.census.gov/data/developers/data-sets/popest-popproj.html':
        'US Census PEP'
    }


if __name__ == '__main__':
    import argparse
    import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    states = get_states(session=cache_policy.new_session(parser.parse_args()))
    print(states)
