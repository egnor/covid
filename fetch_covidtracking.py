#!/usr/bin/env python3
# Module to retrieve data from covidtracking.com.
# (Can also be run as a standalone program for testing.)

import io
import pandas
import requests


def get_states(session=None):
    """Returns a pandas.DataFrame of state-level data from covidtracking."""

    if not session: session = requests.Session()
    response = session.get(
        'https://covidtracking.com/api/v1/states/daily.csv',
        stream=True)
    response.raise_for_status()
    data = pandas.read_csv(io.StringIO(response.text))
    data.date = pandas.to_datetime(data.date, format='%Y%m%d')
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
