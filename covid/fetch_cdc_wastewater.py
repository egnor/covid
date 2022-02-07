"""Module to receive US CDC wastewater data."""

import io

import pandas

DATA_URL = "https://data.cdc.gov/api/views/2ew6-ywp6/rows.csv"


def get_wastewater(session):
    """Returns a DataFrame of wastewater sampling data."""

    response = session.get(DATA_URL)
    response.raise_for_status()
    df = pandas.read_csv(
         io.StringIO(response.text),
         parse_dates=["date_start", "date_end"],
         date_parser=lambda v: pandas.to_datetime(v, utc=True),
    )

    key_cols = [
        "county_fips",
        "wwtp_id",
        "key_plot_id",
        "date_start",
    ]

    df.set_index(key_cols, drop=True, inplace=True, verify_integrity=True)
    df.sort_index(inplace=True)

    return df


def credits():
    return {
        "https://covid.cdc.gov/covid-data-tracker/": "US CDC COVID Data Tracker"
    }


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading wastewater data...")
    df = get_wastewater(session)
    df.info(verbose=True, show_counts=True)
    print()

    df.dropna(subset=["ptc_15d"], inplace=True)
    for fips, v in df.groupby(level=["county_fips"]):
        row = v.iloc[0]
        print(f"=== {row.wwtp_jurisdiction} / {row.county_names} ===")
        for (tp, key), w in v.groupby(["wwtp_id", "key_plot_id"]):
            print(
                f"{' / '.join(key.split('_'))}: "
                f"{w.ptc_15d.count()}d (last {w.ptc_15d.iloc[-1]:+.0f}%)"
            )

        print()
