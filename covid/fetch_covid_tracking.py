#!/usr/bin/env python3
# Module to retrieve data from covidtracking.com.
# (Can also be run as a standalone program for testing.)

import io

import pandas
import requests


def get_states(session):
    """Returns a pandas.DataFrame of state-level data from covidtracking."""

    response = session.get('https://covidtracking.com/api/v1/states/daily.csv')
    response.raise_for_status()
    data = pandas.read_csv(io.StringIO(response.text), dtype={'fips': str})

    def to_datetime(series, format):
        if '%Y' not in format:
            series, format = ('2020 ' + series, '%Y ' + format)
        parsed = pandas.to_datetime(series, format=format)
        return parsed.dt.tz_localize('US/Eastern')

    data.date = to_datetime(data.date, format='%Y%m%d')
    data.lastUpdateEt = to_datetime(data.lastUpdateEt, '%m/%d/%Y %H:%M')
    data.dateModified = pandas.to_datetime(data.dateModified)
    data.checkTimeEt = to_datetime(data.checkTimeEt, '%m/%d %H:%M')
    data.dateChecked = pandas.to_datetime(data.dateChecked)
    return data


def attribution():
    return { 'https://covidtracking.com/': 'The COVID Tracking Project' }


if __name__ == '__main__':
    import argparse
    import signal

    import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()

    states = get_states(session=cache_policy.new_session(args))
    print(states)
    print()
    print('Sample record:')
    print(states.iloc[len(states) // 2])
