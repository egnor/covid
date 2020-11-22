"""Module to get COVID-19 US State Policy Database
(tinyurl.com/statepolicies)."""

import collections
import io
import json
import re
import urllib.parse

import pandas
import us


API_KEY = 'AIzaSyA9L3KnYcG1FDC1EVfH6gNqZbp2FfA5nHw'
DOC_ID = '1zu9qEWI8PsOI_i8nI_S29HDGHlIp2lfVMsGxpQ5tvAQ'


def get_events(session):
    """Returns a pandas.DataFrame of state policy actions."""

    # Fetch document metadata, including a list of sheet tabs.
    doc_url = f'https://sheets.googleapis.com/v4/spreadsheets/{DOC_ID}'
    doc_response = session.get(f'{doc_url}?key={urllib.parse.quote(API_KEY)}')
    doc_response.raise_for_status()
    doc_json = doc_response.json()
    tab_titles = [s['properties']['title'] for s in doc_json['sheets']]

    fetch_params = {'key': API_KEY, 'ranges': ','.join(tab_titles[:2])}
    fetch_query = urllib.parse.urlencode(fetch_params)
    fetch_response = session.get(
        f'{doc_url}/values:batchGet?key={urllib.parse.quote(API_KEY)}' +
        '&valueRenderOption=UNFORMATTED_VALUE' +
        '&dateTimeRenderOption=FORMATTED_STRING' +
        ''.join(f'&ranges={urllib.parse.quote(t)}' for t in tab_titles))
    fetch_response.raise_for_status()
    fetch_json = fetch_response.json()

    out_data = {
        'state_fips': [],
        'state_name': [],
        'state_abbrev': [],
        'date': [],
        'policy_area': [],
        'policy': [],
        'policy_detail': [],
        'score': [],
        'emoji': []
    }

    skip_tabs = [
        'Information', 'Vote By Mail', 'Racial Disparities',
        'Codebook', 'Notes/Details',
        'Pre-COVID UI Monetary Eligibility Thresholds',
    ]

    for tab_json in fetch_json['valueRanges']:
        # Skip tabs with general info or odd formatting (Racial Disparities)
        tab_title = tab_json['range'].split('!')[0].strip("'").strip()
        if tab_title in skip_tabs or re.fullmatch(r'Sheet\d+', tab_title):
            continue

        try:
            tab_values = tab_json['values']
        except KeyError:
            raise ValueError(f'No values in "{tab_title}"')

        header = tab_values[0]
        header = header[:next(
            (i for i, h in enumerate(header) if not h), None)]
        if header[:3] != ['State', 'State Abbreviation', 'State FIPS Code']:
            raise ValueError(
                f'Unexpected columns in "{tab_title}": '
                f'"{header[0]}", "{header[1]}", "{header[2]}"')

        rows = [[str(v) for v in r] for r in tab_values[1:52]]
        for i, r in enumerate(rows):
            if (not r[1].isupper() or len(r[1]) != 2 or not r[2].isdigit()):
                raise ValueError(
                    f'Unexpected data "{r[:3]}" in "{tab_title}" row {i + 2}')
            if len(r) < len(header):
                raise ValueError(
                    f'Short row {i + 2} ({r[1]}) in "{tab_title}": '
                    f'len={len(r)} < {header}')

        Col = collections.namedtuple('Col', 'index name ctype emoji score')
        coldefs = []
        for c in range(3, len(header)):
            name = header[c].strip()
            area_norm, norm = tab_title.lower(), name.lower()

            # Hacks for data glitches go here!
            name = name.replace('Stay at home order\'', 'Stay at home order')

            if all(r[c] in ('0', '') for r in rows):
                continue  # Empty data
            elif all(r[c] in ('0', '1') for r in rows):
                ctype = bool
            elif all(r[c] in ('0', '1') or '/' in r[c] for r in rows):
                ctype = pandas.Timestamp
            elif all(re.fullmatch(r'\d+', r[c]) for r in rows):
                ctype = int
            elif all(re.fullmatch(r'\d+(\.\d+)?', r[c]) for r in rows):
                ctype = float
            elif all('moved to' in r[c] or not r[c] for r in rows):
                continue  # Relocated data
            else:
                raise ValueError(
                    f'Inscrutable values in "{tab_title}" / "{name}": [' +
                    ', '.join(repr(r[c]) for r in rows) + ']')

            emoji = (
                '🚨' if 'state of emergency' in norm else
                '🏠' if 'stay at home' in norm else
                '⏲️' if 'quarantine' in norm else
                '😷' if 'face mask' in norm else
                '🍎' if 'schools' in norm else
                '🧒' if ('day cares' in norm or 'childcare' in norm) else
                '🧓' if 'nursing homes' in norm else
                '🏢' if 'businesses' in norm else
                '🛍️' if 'retail' in norm else
                '🍾' if 'alcohol' in norm else
                '🍝' if ('restaurants' in norm or 'dining' in norm) else
                '🏋️' if 'gyms' in norm else
                '📽️' if 'movie theaters' in norm else
                '🍻' if 'bars' in norm else
                '💇' if 'hair salons' in norm else
                '🚧' if 'construction' in norm else
                '🛐' if 'religious' in norm else
                '🚪' if 'eviction' in norm else
                '🍞' if 'snap' in norm else
                '💵' if ('rent' in norm or 'mortgage' in norm) else
                '🔌' if 'utility' in norm else
                '🕴️' if 'unemployment' in norm else
                '📞' if 'tele' in norm else
                '💊' if ('medication' in norm or 'prescription' in norm) else
                '💊' if 'dea registration' in norm else
                '⚕️' if 'medicaid' in norm else
                '🩺' if 'medical' in norm else
                '👮' if ('prisons' in norm or 'incarcerated' in area_norm) else
                '💼' if 'unemployment' in area_norm else
                '')

            score = (
                +3 if ('stay at home' in norm and 'reopen' in area_norm) else
                +3 if 'end stay at home' in norm else
                -3 if 'stay at home' in norm else
                -2 if 'state of emergency' == norm else
                -1 if 'begin to re-close' in norm else
                -2 if 'closed k-12 schools' in norm else
                -2 if 'closed non-essential businesses' in norm else
                -2 if 'closed restaurants' in norm else
                -2 if 'close indoor dining' in norm else
                -2 if 'closed bars' in norm else
                -2 if 'close bars' in norm else
                -1 if 'physical distance closures' in area_norm else
                +2 if 'reopen businesses' in norm else
                +2 if 'reopen restaurants' in norm else
                +2 if 'reopen bars' in norm else
                +2 if 'reopen non-essential retail' in norm else
                +1 if 'reopening' in area_norm else
                -2 if 're-close indoor dining' in norm else
                -2 if 're-close bars' in norm else
                -1 if 're-close' in norm else
                +2 if ('ended statewide' in norm and 'masks' in area_norm) else
                +2 if ('prevent local' in norm and 'masks' in area_norm) else
                -2 if ('public spaces' in norm and 'masks' in area_norm) else
                -1 if 'masks' in area_norm else
                +1 if 'quarantines ended' in norm else
                -1 if 'quarantine rules' in area_norm else
                -1 if 'suspended elective' in norm else
                -1 if ('stop' in norm and 'incarcerated' in area_norm) else
                +1 if ('resume' in norm and 'incarcerated' in area_norm) else
                0)

            if name[:5].lower() == 'date ':
                name = name[5:6].upper() + name[6:]

            coldefs.append(Col(
                index=c, name=name, ctype=ctype, emoji=emoji, score=score))

        for r, row in enumerate(rows):
            last_detail = {}
            for cdef in coldefs:
                value = row[cdef.index]
                if cdef.ctype == pandas.Timestamp:
                    last_detail = {}

                    # Codes for policies in effect pre-COVID.
                    date = value.replace('already in effect', '').strip()
                    date = date.replace('*', '')  # Footnote.
                    date = '1/1/2020' if date == '1/0/1900' else date
                    date = '1/1/2020' if date == '1' else date
                    date = date + '/2020' if date.count('/') == 1 else date
                    date = '' if date == '0' else date
                    if not date:
                        continue

                    try:
                        date = pandas.Timestamp(date)
                    except ValueError:
                        raise ValueError(
                            f'Bad date "{date}" in "{tab_title}" / '
                            f'"{cdef.name}" for {row[1]} (row {r + 2})')

                    out_data['state_name'].append(row[0])
                    out_data['state_abbrev'].append(row[1])
                    out_data['state_fips'].append(int(row[2]))
                    out_data['date'].append(pandas.Timestamp(date))
                    out_data['policy_area'].append(tab_title)
                    out_data['policy'].append(cdef.name)
                    out_data['policy_detail'].append(last_detail)
                    out_data['score'].append(cdef.score)
                    out_data['emoji'].append(cdef.emoji)

                else:
                    try:
                        last_detail[cdef.name] = (cdef.ctype)(value)
                    except ValueError:
                        raise ValueError(
                            f'Bad value "{value}" in "{tab_title}" / '
                            f'"{cdef.name}" for {row[1]} (row {r + 2}), '
                            f'expected {cdef.ctype}')

    frame = pandas.DataFrame(out_data)
    frame.sort_values(by=['state_fips', 'date'], inplace=True)
    frame.set_index(['state_fips', 'date'], inplace=True)
    return frame


def credits():
    return {'https://tinyurl.com/statepolicies':
            'COVID-19 US State Policy Database'}


if __name__ == '__main__':
    import argparse
    import textwrap
    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    events = get_events(session=cache_policy.new_session(parser.parse_args()))
    for state_fips, state_events in events.groupby(level='state_fips'):
        state = us.states.lookup(f'{state_fips:02d}')
        print(f'{state.name}:')
        for date, date_events in state_events.groupby(level='date'):
            print(date.strftime('  %Y-%m-%d'))
            for area, area_events in date_events.groupby('policy_area'):
                print(f'    {area}')
                for e in area_events.itertuples():
                    s = ['⬇️ ', '🔷', '🔹', '▪️ ', '🔸', '🔶', '⏫'][e.score + 3]
                    text = ' '.join(x for x in [s, e.emoji, e.policy] if x)
                    print(textwrap.TextWrapper(
                        initial_indent='     ',
                        subsequent_indent='         ',
                        width=79).fill(text))
                    for k, v in e.policy_detail.items():
                        vt = {True: '✔️ ', False: '❌'}.get(v, f'{v}:')
                        print(textwrap.TextWrapper(
                            initial_indent='        ',
                            subsequent_indent='           ',
                            width=79).fill(f'{vt} {k}'))
            print()
