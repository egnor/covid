"""Module to retrieve data from covidtracking.com."""

import io

import pandas


def get_states(session):
    """Returns a pandas.DataFrame of state-level data from covidtracking."""

    response = session.get('https://covidtracking.com/api/v1/states/daily.csv')
    response.raise_for_status()
    data = pandas.read_csv(
        io.StringIO(response.text),
        na_values=[''], keep_default_na=False)

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
    data.sort_values(by=['fips', 'date'], inplace=True)
    data.set_index(['fips', 'date'], inplace=True)
    return data


def credits():
    return {'https://covidtracking.com/': 'The COVID Tracking Project'}


if __name__ == '__main__':
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    states = get_states(session=cache_policy.new_session(parser.parse_args()))
    print(states.dtypes)
    print()
    print('Arbitrary record:')
    print(states.iloc[len(states) // 2])
