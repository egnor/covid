# Module to retrieve Google's Community Mobility Reports data
# (https://www.google.com/covid19/mobility/).

import io

import pandas


def get_mobility(session):
    """Returns a pandas.DataFrame of mobility data from Google."""

    response = session.get(
        'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv')
    response.raise_for_status()
    data = pandas.read_csv(
        io.StringIO(response.text),
        parse_dates=['date'],
        dtype={'sub_region_2': str, 'census_fips_code': str})

    # Use '' for empty location fields for consistent typing & groupby().
    # (Keep nan values for missing mobility data values.)
    data.sub_region_1.fillna('', inplace=True)
    data.sub_region_2.fillna('', inplace=True)
    data.iso_3166_2_code.fillna('', inplace=True)
    data.census_fips_code.fillna('', inplace=True)
    return data


def attribution():
    return {
        'https://www.google.com/covid19/mobility/':
        'Google Community Mobility Reports'
    }


if __name__ == '__main__':
    import argparse

    import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    data = get_mobility(cache_policy.new_session(parser.parse_args()))
    print(data.dtypes)
    print()
    print('Sample record:')
    print(data.iloc[len(data) // 2])
