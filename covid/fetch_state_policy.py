#!/usr/bin/env python3
# Module to get COVID-19 US State Policy Database (tinyurl.com/statepolicies)
# (Can also be run as a standalone program for testing.)

import collections
import io
import json
import pandas
import requests
import urllib.parse


API_KEY = 'AIzaSyA9L3KnYcG1FDC1EVfH6gNqZbp2FfA5nHw'
DOC_ID = '1zu9qEWI8PsOI_i8nI_S29HDGHlIp2lfVMsGxpQ5tvAQ'


def get_states(session):
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
        'significance': [],
        'emoji': []
    }

    for tab_json in fetch_json['valueRanges']:
        # Skip tabs with general info or odd formatting (Racial Disparities)
        tab_title = tab_json['range'].split('!')[0].strip("'")
        if tab_title in ('Information', 'Racial Disparities', 'Notes/Details'):
            continue

        tab_values = tab_json['values']
        header = tab_values[0]
        if header[:3] != ['State', 'State Abbreviation', 'State FIPS Code']:
            raise ValueError(
                f'Unexpected columns in "{tab_title}": '
                f'"{header[0]}", "{header[1]}", "{header[2]}"')

        rows = tab_values[1:52]
        for i, r in enumerate(rows):
            if (not all(isinstance(*vt) for vt in zip(r[:3], (str, str, int)))
                or not r[1].isupper() or len(r[1]) != 2):
                raise ValueError(
                    f'Unexpected data in "{tab_title}" row {i + 2}: {r[:3]}')

        ColDef = collections.namedtuple('ColDef', 'index name ctype emoji sig')
        coldefs = []
        for c in range(3, len(header)):
            name = header[c]
            area_norm, norm = tab_title.lower().strip(), name.lower().strip()

            # Hacks for data glitches!
            if ('incarcerated' in area_norm and 'attorney visits' in norm and
                rows[42][c] == 1):
                rows[42][c] = '3/12/2020'

            if ('masks' in area_norm and 'legal enforcement' in norm and
                rows[18][c] == 'f'):
                rows[18][c] = 0

            ctype=(
                bool if all(r[c] in (0, 1) for r in rows) else
                pandas.Timestamp if all(
                    r[c] == 0 or '/' in str(r[c]) for r in rows) else int)

            emoji=(
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
                '🍴' if ('restaurants' in norm or 'dining' in norm) else
                '🏋️' if 'gyms' in norm else
                '📽️' if 'movie theaters' in norm else
                '🍻' if 'bars' in norm else
                '💇' if 'hair salons' in norm else
                '🚧' if 'construction' in norm else
                '🛐' if 'religious' in norm else
                '🚪' if 'eviction' in norm else
                '💵' if ('rent' in norm or 'mortgage' in norm) else
                '🔌' if 'utility' in norm else
                '🕴️' if 'unemployment' in norm else
                '🍞' if 'snap' in norm else
                '📞' if 'tele' in norm else
                '💊' if ('medication' in norm or 'prescription' in norm) else
                '💊' if 'dea registration' in norm else
                '⚕️' if 'medicaid' in norm else
                '🩺' if 'medical' in norm else
                '👮' if 'prisons' in norm else
                '')

            sig = (
                -1 if 'state of emergency' == norm else
                -2 if 'closed k-12 schools' in norm else
                -2 if 'closed restaurants' in norm else
                -2 if 'closed non-essential businesses' in norm else
                -1 if 'physical distance closures' in area_norm else
                -2 if 'stay at home' in area_norm else
                +2 if ('stay at home' in norm and 'reopen' in area_norm) else
                +2 if 'reopen businesses' in norm else
                +2 if 'reopen restaurants' in norm else
                +2 if 'reopen non-essential retail' in norm else
                +1 if 'reopening' in area_norm else
                -1 if 're-close' in norm else
                -2 if ('public spaces' in norm and 'masks' in area_norm) else
                -1 if 'masks' in area_norm else
                -1 if 'quarantine rules' in area_norm else
                -1 if 'suspended elective' in norm else
                -1 if 'incarcerated' in area_norm else
                0)

            coldefs.append(ColDef(
                index=c, name=name, ctype=ctype, emoji=emoji, sig=sig))

        for r, row in enumerate(rows):
            last_detail = {}
            for cdef in coldefs:
                value = row[cdef.index]
                if cdef.ctype == pandas.Timestamp:
                    last_detail = {}
                    if value == 0:
                        continue

                    date = value.replace('already in effect', '').strip()
                    out_data['state_name'].append(row[0])
                    out_data['state_abbrev'].append(row[1])
                    out_data['state_fips'].append(f'{row[2]:02d}')
                    out_data['date'].append(pandas.Timestamp(date))
                    out_data['policy_area'].append(tab_title)
                    out_data['policy'].append(cdef.name)
                    out_data['policy_detail'].append(last_detail)
                    out_data['significance'].append(cdef.sig)
                    out_data['emoji'].append(cdef.emoji)

                else:
                    try:
                        last_detail[cdef.name] = (cdef.ctype)(value)
                    except ValueError as e:
                        raise ValueError(f'Bad "{cdef.name}" @ row {r}') from e
                    

    frame = pandas.DataFrame.from_dict(out_data)
    return frame


if __name__ == '__main__':
    import argparse
    import signal
    import textwrap

    import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()

    states = get_states(session=cache_policy.new_session(args))
    for state, state_events in states.groupby('state_name'):
        print(f'{state}:')
        for date, date_events in state_events.groupby('date'):
            print(date.strftime('  %Y-%m-%d'))
            for area, area_events in date_events.groupby('policy_area'):
                print(f'    {area}')
                for e in area_events.itertuples():
                    sig = ['⬇️ ', '🔹', '▪️ ', '🔸', '⏫'][e.significance + 2]
                    text = ' '.join(x for x in [sig, e.emoji, e.policy] if x)
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
