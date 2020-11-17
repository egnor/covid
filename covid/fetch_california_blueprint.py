"""Module to retrieve the California Blueprint Data Chart."""

import collections
import io
import re
import urllib.parse
import warnings

import addfips
import bs4
import numpy
import pandas


CountyData = collections.namedtuple('CountyData', 'fips name tier_history')

Tier = collections.namedtuple('Tier', 'number emoji color name')

TIERS = [
    None,
    Tier(1, '🟣', 'Purple', 'Widespread'),
    Tier(2, '🔴', 'Red', 'Substantial'),
    Tier(3, '🟠', 'Orange', 'Moderate'),
    Tier(4, '🟡', 'Yellow', 'Minimal')
]

OVERVIEW_URL = 'https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/COVID19CountyMonitoringOverview.aspx'

ARCHIVE_URL = 'https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/CaliforniaBlueprintDataCharts.aspx'


def get_counties(session):
    xlsx_urls = set()
    for html_url in (ARCHIVE_URL, OVERVIEW_URL):
        html_response = session.get(html_url)
        html_response.raise_for_status()
        html = bs4.BeautifulSoup(html_response.text, features='html.parser')
        targets = [
            urllib.parse.urljoin(html_url, l['href'])
            for l in html.find_all(name='a')
            if l.get('href', '').endswith('.xlsx')]
        if not targets:
            warnings.warn(f'No CA .xlsx links found: {html_url}')
        xlsx_urls.update(targets)

    if not xlsx_urls:
        warnings.warn(f'No CA blueprint .xlsx files found!')
        return {}

    counties = {}
    for xlsx_url in sorted(xlsx_urls):
        if '08.31.20' in xlsx_url:
            continue  # Skip the first sheet, which used color coding only.

        if not re.search(r'blueprint[^/]*[.]xlsx', xlsx_url, re.I):
            warnings.warn(f'Unexpected CA .xlsx link: {xlsx_url}')
            continue

        try:
            for f, c in _counties_from_xlsx(session, xlsx_url).items():
                counties.setdefault(f, c).tier_history.update(c.tier_history)
        except Exception as e:
            raise ValueError(f'Error parsing CA .xlsx: {xlsx_url}')

    out = {}
    for fips, county in sorted(counties.items()):
        change_history, last_tier = {}, None
        for date, tier in sorted(county.tier_history.items()):
            if tier != last_tier:
                change_history[date] = last_tier = tier
        out[fips] = county._replace(tier_history=change_history)

    return out


def _counties_from_xlsx(session, xlsx_url):
    response = session.get(xlsx_url)
    response.raise_for_status()
    xlsx = pandas.read_excel(io=response.content, header=None, na_filter=False)

    # Find the header row.
    header_rows = xlsx.index[xlsx[0].isin(('County', 'Location'))]
    if len(header_rows) < 1:
        raise ValueError(f'No "County" in first column')

    # Assign column names and trim rows before the header row.
    xlsx.columns = xlsx.iloc[header_rows[0]]
    xlsx = xlsx.iloc[header_rows[0] + 1:]

    # Trim rows after any null/footnote row.
    footnote = re.compile(r'[*^]|small county|red text|$', re.I)
    empty = xlsx.index[xlsx.iloc[:, 0].str.match(footnote)]
    if len(empty):
        xlsx = xlsx.loc[:empty[0]].iloc[:-1]

    # Find important columns.
    def rename_col(name, *regexes):
        for rx in regexes:
            rep = [name if re.search(rx, c, re.I) else c for c in xlsx.columns]
            if rep.count(name) > 1:
                raise ValueError(f'Multiple /{rx}/ in header: {xlsx.columns}')
            elif rep.count(name) == 1:
                xlsx.columns = rep
                return
        raise ValueError(f'No {regexes} in header: {xlsx.columns}')

    rename_col('County', '^county$', '^location$')
    rename_col('Date', '^first date in current ',
               '^date of tier ass(ess|ign)ment')
    rename_col('Tier', '^final tier ',
               '^(updated )?(overall )?tier (status|ass(ign|ass)ment)')

    # Clean up county names.
    county_regex = re.compile(r'\W*(\w[\w\s]*\w)\W*')
    xlsx.County = xlsx.County.str.replace(county_regex, r'\1')

    # Assign county FIPS codes.
    add_fips = addfips.AddFIPS()
    def get_county_fips(county):
        try:
            return int(add_fips.get_county_fips(county, 'CA'))
        except BaseException:
            raise ValueError(f'Error looking up county "{county}"')

    xlsx['FIPS'] = xlsx.County.apply(get_county_fips)

    # Convert dates.
    xlsx.Date = pandas.to_datetime(xlsx.Date)

    # Return CountyData based on all the work above.
    return {
        row.FIPS: CountyData(fips=row.FIPS, name=row.County,
                             tier_history={row.Date: TIERS[row.Tier]})
        for row in xlsx.itertuples()
    }


def credits():
    return {OVERVIEW_URL: 'California Blueprint Data Chart'}


if __name__ == '__main__':
    import argparse
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    session = cache_policy.new_session(parser.parse_args())
    counties = get_counties(session)
    for fips, county in counties.items():
        assert fips == county.fips
        print(county.fips, county.name)
        for date, tier in county.tier_history.items():
            print(f'    {date.strftime("%Y-%m-%d")} '
                  f'{tier.number}: {tier.emoji} {tier.color} ({tier.name})')
