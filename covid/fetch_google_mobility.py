"""Module to retrieve Google's Community Mobility Reports data
(https://www.google.com/covid19/mobility/)."""

import io

import pandas
import us.states


def get_mobility(session):
    """Returns a pandas.DataFrame of mobility data from Google."""

    response = session.get(
        'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv')
    response.raise_for_status()
    data = pandas.read_csv(
        io.StringIO(response.text),
        na_values=[''], keep_default_na=False,
        parse_dates=['date'],
        dtype={'sub_region_2': str, 'metro_area': str, 'iso_3166_2_code': str})

    # Use '' for empty string fields for consistent typing & groupby().
    data.sub_region_1.fillna('', inplace=True)
    data.sub_region_2.fillna('', inplace=True)
    data.metro_area.fillna('', inplace=True)
    data.iso_3166_2_code.fillna('', inplace=True)

    # Use int for FIPS, with 0 for N/A.
    data.census_fips_code.fillna(0, inplace=True)
    data.census_fips_code = data.census_fips_code.astype(int)

    # Fill in missing state-level FIPS codes.
    for state in us.states.STATES_AND_TERRITORIES:
        mask = data.iso_3166_2_code.eq(f'US-{state.abbr}')
        data.census_fips_code.mask(mask, int(state.fips), inplace=True)

    return data


def credits():
    return {'https://www.google.com/covid19/mobility/':
            'Google Community Mobility Reports'}


if __name__ == '__main__':
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    data = get_mobility(cache_policy.new_session(parser.parse_args()))
    print(data.dtypes)
    print()
    print('Arbitrary record:')
    print(data.iloc[len(data) // 2])
    print()
    print('Last California record:')
    print(data[data.census_fips_code.eq(6)].iloc[-1])
