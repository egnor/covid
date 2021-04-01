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
    # Impute a date from the URL.
    filename = xlsx_url.split('/')[-1]
    date_match = re.search(r'[01][0-9][0123][0-9][2][01]', filename)
    if not date_match:
        raise ValueError(f'No MMDDYY date in "{filename}"')
    url_date = pandas.to_datetime(date_match.group(), format='%m%d%y')

    # Download the spreadsheet.
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
    def rename_col(name, *regexes, required=False):
        for rx in (re.compile(r, re.I) for r in regexes):
            cols = [name if rx.search(c.strip()) else c for c in xlsx.columns]
            if cols.count(name) > 1:
                raise ValueError(f'Multiple /{r}/ in header {xlsx.columns}')
            elif cols.count(name) == 1:
                xlsx.columns = cols
                return
        if name not in xlsx.columns and required:
            raise ValueError(f'No match for {regexes} in {xlsx.columns}')

    rename_col('County', '^county$', '^location$', required=True)
    rename_col('Date', '^first date in current ', '^date of tier ass(ess|ign)')
    rename_col('LastTier', r'^tier (ass(ign|ass)ment) on ')
    rename_col('Tier', r'^(updated )?(overall )?tier (status|ass(ign|ass))',
               r'^final tier ', required=True)

    # Fill or convert date values.
    if 'Date' not in xlsx.columns:
        xlsx['Date'] = url_date
    else:
        xlsx.Date = pandas.to_datetime(xlsx.Date)

    # Clean up county names and add FIPS codes.
    county_regex = re.compile(r'\W*(\w[\w\s]*\w)\W*')
    xlsx.County = xlsx.County.str.replace(county_regex, r'\1')
    xlsx['FIPS'] = xlsx.County.apply(_fips_from_county)

    # Return CountyData based on all the work above.
    return {
        row.FIPS: CountyData(
            fips=row.FIPS, name=row.County,
            tier_history={row.Date: TIERS[row.Tier]})
        for row in xlsx.itertuples()
    }


_add_fips = addfips.AddFIPS()


def _fips_from_county(county):
    try:
        return int(_add_fips.get_county_fips(county, 'CA'))
    except BaseException:
        raise ValueError(f'Error looking up county "{county}"')


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
