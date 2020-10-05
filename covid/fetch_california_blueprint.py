"""Module to retrieve the California Blueprint Data Chart."""

import collections
import io
import re
import urllib.parse

import addfips
import bs4
import numpy
import pandas


TierDescription = collections.namedtuple('TierDescription', 'emoji color name')

TIER_DESCRIPTION = {
    1: TierDescription('🟣', 'Purple', 'Widespread'),
    2: TierDescription('🔴', 'Red', 'Substantial'),
    3: TierDescription('🟠', 'Orange', 'Moderate'),
    4: TierDescription('🟡', 'Yellow', 'Minimal')
}

HTML_URL = 'https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/COVID19CountyMonitoringOverview.aspx'


def get_counties(session):
    html_response = session.get(HTML_URL)
    html_response.raise_for_status()
    html = bs4.BeautifulSoup(html_response.text, features='html.parser')
    links = html.find_all(name='a', string=re.compile('data chart', re.I))
    targets = [urllib.parse.urljoin(HTML_URL, l['href']) for l in links]
    if not targets:
        raise ValueError(f'No data links found: {HTML_URL}')
    if not all(t == targets[0] for t in targets):
        raise ValueError('Inconsistent data links in {HTML_URL}: {targets}')

    xlsx_response = session.get(targets[0])
    xlsx_response.raise_for_status()
    data = pandas.read_excel(
        io=xlsx_response.content, sheet_name='County Tiers and Metrics',
        header=1, na_filter=False)

    # Trim rows after any null/footnote row.
    footnote = re.compile(r'[*^]|small county|$', re.I)
    empty = data.index[data.County.str.match(footnote)]
    if len(empty):
        data = data.loc[:empty[0]].iloc[:-1]

    af = addfips.AddFIPS()
    data.County = data.County.str.replace('*', '')
    data['FIPS'] = data.County.apply(
        lambda c: int(af.get_county_fips(c, 'CA')))
    data.set_index('FIPS', drop=True, inplace=True)

    data.replace(re.compile(r'^\s*(-|NA|)\s*$', re.I), numpy.nan, inplace=True)
    for col in data.columns:
        if 'Date' in col:
            data[col] = pandas.to_datetime(data[col])
        elif 'Linear Adjustment Factor' in col:
            data[col].fillna(1.0, inplace=True)

    return data.convert_dtypes()


def credits():
    return {HTML_URL: 'California Blueprint Data Chart'}


if __name__ == '__main__':
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    session = cache_policy.new_session(parser.parse_args())
    counties = get_counties(session)
    print(counties.info())
