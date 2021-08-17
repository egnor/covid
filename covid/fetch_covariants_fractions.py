"""Module to retrieve variant fraction data from coviariants.org."""

import io

import pandas


REPO_DIR = 'https://raw.githubusercontent.com/hodcroftlab/covariants/master'
CLUSTER_TABLES_DIR = f'{REPO_DIR}/cluster_tables'
COUNTRY_JSON_FILES = [
    (None, f'{CLUSTER_TABLES_DIR}/EUClusters_data.json'),
    ('us', f'{CLUSTER_TABLES_DIR}/USAClusters_data.json'),
]


def get_variants(session):
    pass


def credits():
    return {'https://covariants.org/': 'CoVariants.org'}


if __name__ == '__main__':
    import argparse
    import signal
    from covid import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    df = get_variants(session)
    for v in df.itertuples():
        print(v)
        print()
