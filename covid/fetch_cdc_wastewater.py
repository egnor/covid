"""Module to receive US CDC wastewater data."""

import io

import pandas

DATA_URL = "https://data.cdc.gov/api/views/2ew6-ywp6/rows.csv"


def get_wastewater(session):
    """Returns a DataFrame of wastewater sampling data."""

    TODO


def credits():
    return {
        "https://covid.cdc.gov/covid-data-tracker/": "US CDC COVID Data Tracker"
    }


if __name__ == "__main__":
    import argparse
    import signal

    from covid import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading prevalence...")
    df = get_prevalence(session)
    df.info(verbose=True, show_counts=True)
    print()

    print("=== REGIONS ===")
    rcols = ["Region Abbreviation", "Region"]
    for (abbr, region), v in df.groupby(rcols, as_index=False):
        v.reset_index(rcols, drop=True, inplace=True)
        print(
            f"{v.index[0].strftime('%Y-%m')} - {v.index[-1].strftime('%Y-%m')} "
            f"{abbr} / {region} ({len(v)})"
        )
