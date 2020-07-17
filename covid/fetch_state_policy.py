#!/usr/bin/env python3
# Module to get COVID-19 US State Policy Database (tinyurl.com/statepolicies)
# (Can also be run as a standalone program for testing.)

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

    return None


if __name__ == '__main__':
    import argparse
    import signal

    import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()

    states = get_states(session=cache_policy.new_session(args))
    print(states)
