#!/usr/bin/env python3
# Module to retrieve Census population data.
# (Can also be run as a standalone program for testing.)

import json
import pandas
import requests


URL_BASE = 'https://api.census.gov/data/2019/pep/population'
API_KEY = 'c0e3aa89dc3a4a7f3be500700f83c292e5556024'


def get_states(session=None):
    """Returns a pandas.DataFrame of state-level population data."""

    if not session: session = requests.Session()
    response = session.get(f'{URL_BASE}?get=NAME,POP&for=state:*&key={API_KEY}')
    response.raise_for_status()

    json_data = json.loads(response.text)
    data = pandas.DataFrame(json_data[1:], columns=json_data[0])
    data.set_index('state', inplace=True)
    data.sort_index(inplace=True)
    return data


if __name__ == '__main__':
    import argparse
    import signal

    import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()

    states = get_states(session=cache_policy.new_session(args))
    print(states)
