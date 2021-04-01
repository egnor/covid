"""Module to retrieve data from the Unified COVID-Dataset dashboard."""

import io
import re
import requests.exceptions
import tempfile

import pandas
import pyreadr

from covid.cache_policy import cached_path, temp_to_rename


REPO_DIR = 'https://raw.githubusercontent.com/hsbadr/COVID-19/master'
LOOKUP_CSV_URL = f'{REPO_DIR}/COVID-19_LUT.csv'
COVID19_RDATA_URL = f'{REPO_DIR}/COVID-19.rds'
HYDROMET_RDATA_URL = f'{REPO_DIR}/Hydromet/Hydromet_YYYYMM.rds'

_place_by_id = None


def get_places(session):
    """Returns a dict mapping UID number to place metadata."""

    global _place_by_id
    if _place_by_id is None:
        response = session.get(LOOKUP_CSV_URL)
        response.raise_for_status()
        places = pandas.read_csv(
            io.StringIO(response.text),
            keep_default_na=False, na_values={
                f: [''] for f in ('Latitude', 'Longitude', 'Population')})

        places.Population.fillna(0, inplace=True)
        places.Population = places.Population.astype(int)
        _place_by_id = pid = {
            place.ID: place
            for place in places.itertuples(index=False, name='Place')
        }

        # Data error patches. TODO: Remove when fixed upstream.
        # Puerto Rico
        pid['US72'] = pid['US72']._replace(Population=3193694)
        pid['US72001'] = pid['US72001']._replace(Population=17363)
        pid['US72003'] = pid['US72003']._replace(Population=36694)
        pid['US72005'] = pid['US72005']._replace(Population=50265)
        pid['US72007'] = pid['US72007']._replace(Population=24814)
        pid['US72009'] = pid['US72009']._replace(Population=22108)
        pid['US72011'] = pid['US72011']._replace(Population=26161)
        pid['US72013'] = pid['US72013']._replace(Population=81966)
        pid['US72015'] = pid['US72015']._replace(Population=17238)
        pid['US72017'] = pid['US72017']._replace(Population=23727)
        pid['US72019'] = pid['US72019']._replace(Population=27725)
        pid['US72021'] = pid['US72021']._replace(Population=169269)
        pid['US72023'] = pid['US72023']._replace(Population=47515)
        pid['US72025'] = pid['US72025']._replace(Population=124606)
        pid['US72027'] = pid['US72027']._replace(Population=30504)
        pid['US72029'] = pid['US72029']._replace(Population=44674)
        pid['US72031'] = pid['US72031']._replace(Population=146984)
        pid['US72033'] = pid['US72033']._replace(Population=23121)
        pid['US72035'] = pid['US72035']._replace(Population=42409)
        pid['US72037'] = pid['US72037']._replace(Population=10904)
        pid['US72039'] = pid['US72039']._replace(Population=15808)
        pid['US72041'] = pid['US72041']._replace(Population=38307)
        pid['US72043'] = pid['US72043']._replace(Population=38336)
        pid['US72045'] = pid['US72045']._replace(Population=18648)
        pid['US72047'] = pid['US72047']._replace(Population=32293)
        pid['US72049'] = pid['US72049']._replace(Population=1714)
        pid['US72051'] = pid['US72051']._replace(Population=36141)
        pid['US72053'] = pid['US72053']._replace(Population=29454)
        pid['US72054'] = pid['US72054']._replace(Population=11317)
        pid['US72055'] = pid['US72055']._replace(Population=15383)
        pid['US72057'] = pid['US72057']._replace(Population=39465)
        pid['US72059'] = pid['US72059']._replace(Population=17623)
        pid['US72061'] = pid['US72061']._replace(Population=83728)
        pid['US72063'] = pid['US72063']._replace(Population=47093)
        pid['US72065'] = pid['US72065']._replace(Population=39218)
        pid['US72067'] = pid['US72067']._replace(Population=15518)
        pid['US72069'] = pid['US72069']._replace(Population=50653)
        pid['US72071'] = pid['US72071']._replace(Population=40423)
        pid['US72073'] = pid['US72073']._replace(Population=13891)
        pid['US72075'] = pid['US72075']._replace(Population=44679)
        pid['US72077'] = pid['US72077']._replace(Population=38155)
        pid['US72079'] = pid['US72079']._replace(Population=22010)
        pid['US72081'] = pid['US72081']._replace(Population=24276)
        pid['US72083'] = pid['US72083']._replace(Population=7927)
        pid['US72085'] = pid['US72085']._replace(Population=37007)
        pid['US72087'] = pid['US72087']._replace(Population=24553)
        pid['US72089'] = pid['US72089']._replace(Population=17665)
        pid['US72091'] = pid['US72091']._replace(Population=37287)
        pid['US72093'] = pid['US72093']._replace(Population=5430)
        pid['US72095'] = pid['US72095']._replace(Population=10321)
        pid['US72097'] = pid['US72097']._replace(Population=71530)
        pid['US72099'] = pid['US72099']._replace(Population=34891)
        pid['US72101'] = pid['US72101']._replace(Population=30335)
        pid['US72103'] = pid['US72103']._replace(Population=25761)
        pid['US72105'] = pid['US72105']._replace(Population=27349)
        pid['US72107'] = pid['US72107']._replace(Population=20220)
        pid['US72109'] = pid['US72109']._replace(Population=16211)
        pid['US72111'] = pid['US72111']._replace(Population=19249)
        pid['US72113'] = pid['US72113']._replace(Population=131881)
        pid['US72115'] = pid['US72115']._replace(Population=22918)
        pid['US72117'] = pid['US72117']._replace(Population=13656)
        pid['US72119'] = pid['US72119']._replace(Population=48025)
        pid['US72121'] = pid['US72121']._replace(Population=21712)
        pid['US72123'] = pid['US72123']._replace(Population=27128)
        pid['US72125'] = pid['US72125']._replace(Population=30227)
        pid['US72127'] = pid['US72127']._replace(Population=318441)
        pid['US72129'] = pid['US72129']._replace(Population=35989)
        pid['US72131'] = pid['US72131']._replace(Population=35528)
        pid['US72133'] = pid['US72133']._replace(Population=21209)
        pid['US72135'] = pid['US72135']._replace(Population=72025)
        pid['US72137'] = pid['US72137']._replace(Population=74271)
        pid['US72139'] = pid['US72139']._replace(Population=63674)
        pid['US72141'] = pid['US72141']._replace(Population=27395)
        pid['US72143'] = pid['US72143']._replace(Population=36061)
        pid['US72145'] = pid['US72145']._replace(Population=50023)
        pid['US72147'] = pid['US72147']._replace(Population=8386)
        pid['US72149'] = pid['US72149']._replace(Population=21372)
        pid['US72151'] = pid['US72151']._replace(Population=32282)
        pid['US72153'] = pid['US72153']._replace(Population=33575)

        # US Virgin Islands
        pid['US78020'] = pid['US78020']._replace(Population=4170)
        pid['US78030'] = pid['US78030']._replace(Population=51634)

    return _place_by_id


def get_covid(session):
    """Returns a DataFrame of COVID-19 daily records."""

    cache_path = cached_path(session, f'{COVID19_RDATA_URL}:feather')
    if cache_path.exists():
        df = pandas.read_feather(cache_path)
    else:
        df = _read_rdata_url(session, COVID19_RDATA_URL)
        df.Date = pandas.to_datetime(df.Date, utc=True)
        for c in ('Cases', 'Cases_New'):
            df[c].fillna(0, inplace=True)
            df[c] = df[c].astype(int)
        with temp_to_rename(cache_path) as temp_path:
            df.to_feather(temp_path)

    return df.groupby(['ID', 'Type', 'Source', 'Age', 'Sex', 'Date']).first()


def get_hydromet(session):
    """Returns a DataFrame of hydrometeological daily records."""
    cache_path = cached_path(session, f'{HYDROMET_RDATA_URL}:feather')
    if cache_path.exists():
        df = pandas.read_feather(cache_path)
    else:
        frames = []
        try:
            for month in range(120):
                yyyymm = f'{2020 + month // 12}{1 + month % 12:02d}'
                url = HYDROMET_RDATA_URL.replace('YYYYMM', yyyymm)
                frame = _read_rdata_url(session, url)
                frames.append(frame)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

        df = pandas.concat(frames, ignore_index=True)
        df.Date = pandas.to_datetime(df.Date, utc=True)
        with temp_to_rename(cache_path) as temp_path:
            df.to_feather(temp_path)

    return df.groupby(['ID', 'Date', 'HydrometSource']).first()


def _read_rdata_url(session, url):
    """Downloads an RData file from an URL and returns it as a DataFrame."""

    response = session.get(url)
    response.raise_for_status()
    with tempfile.NamedTemporaryFile() as tf:
        tf.write(response.content)
        tf.flush()
        try:
            rdata = pyreadr.read_r(tf.name)
        except BaseException:
            raise ValueError(f'Error parsing RData: {url}')
        if len(rdata) != 1:
            objects = ', '.join(f'"{k}"' for k in rdata.keys())
            raise ValueError('Multiple R objects ({objects}): {url}')

        frame = next(iter(rdata.values()))
        # frame['url'] = url.split('/')[-1]
        return frame


def credits():
    return {'https://github.com/hsbadr/COVID-19': 'Unified COVID-19 Dataset'}


if __name__ == '__main__':
    import argparse
    import signal
    from covid import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument('--id_regex')
    parser.add_argument('--print_data', action='store_true')
    parser.add_argument('--print_types', action='store_true')
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print('Loading places...')
    places = get_places(session)
    print('Loading COVID data...')
    covid = get_covid(session)
    print('Loading hydromet data...')
    hydromet = get_hydromet(session)

    print()
    print('=== COVID DATA ===')
    covid.info(null_counts=True)
    print()
    codes = {}
    for (source, type), source_data in covid.groupby(level=['Source', 'Type']):
        codes[(source, type)] = code = len(codes) + 1
        place_count = len(source_data.index.unique(level='ID'))
        print(f'{f"[{code}]":>4} {place_count:4d}p {source:<3} {type}')

    print()
    print('=== HYDROMET DATA ===')
    hydromet.info(null_counts=True)
    print()
    for source, source_data in hydromet.groupby(level=['HydrometSource']):
        codes[source] = code = len(codes) + 1
        place_count = len(source_data.index.unique(level='ID'))
        print(f'{f"<{code}>":>4} {place_count:4d}p {source:<3}')

    print()
    print('=== REGIONS ===')
    id_regex = args.id_regex and re.compile(args.id_regex, re.I)
    hydromet_by_id = hydromet.groupby(level='ID')
    for id, c_data in covid.groupby(level='ID'):
        if id_regex and not id_regex.match(id):
            continue

        p = places[id]
        c_by_source_type = c_data.groupby(level=['Source', 'Type'])
        c_refs = [codes[s] for s in c_by_source_type.groups]

        h_data, h_by_type, h_refs = None, None, []
        if id in hydromet_by_id.groups:
            h_data = hydromet_by_id.get_group(id)
            h_by_type = h_data.groupby(level='HydrometSource')
            h_refs = [codes[s] for s in h_by_type.groups]

        line = f'{p.ID:<11} {p.Population:9d}p'
        line += f' {p.ISO2_UID}'
        line += f' [{",".join(str(r) for r in c_refs)}]' if c_refs else ''
        line += f' <{",".join(str(h) for h in h_refs)}>' if h_refs else ''
        line += f' f={p.FIPS}' if p.FIPS else ''
        line += f' z={p.ZCTA}' if p.ZCTA else ''
        line += ' ' + ': '.join(a for a in (p.Admin2, p.Admin3) if a)
        print(line)

        if args.print_data or args.print_types:
            for (source, type), data in c_by_source_type or []:
                days = data.index.unique(level='Date')
                print(f'  {f"[{codes[source, type]}]":>4}'
                      f' {min(days).strftime("%Y-%m-%d")}'
                      f' - {max(days).strftime("%y-%m-%d")}'
                      f' COV ({source}) {type}')
                if args.print_data:
                    print(data)

            for type, data in h_by_type or []:
                days = data.index.unique(level='Date')
                print(f'  {f"<{codes[type]}>":>4}'
                      f' {min(days).strftime("%Y-%m-%d")}'
                      f' - {max(days).strftime("%y-%m-%d")}'
                      f' Hyd {type}')
                if args.print_data:
                    print(data)

            print()
