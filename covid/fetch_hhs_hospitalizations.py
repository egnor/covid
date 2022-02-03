"""Module to receive US HHS hospital usage data."""

import io

import pandas

from covid.cache_policy import cached_path
from covid.cache_policy import temp_to_rename

DATA_URL = "https://healthdata.gov/api/views/anag-cw7u/rows.csv"


def get_hospitalizations(session):
    """Returns a DataFrame of per-facility hospitalization stats."""

    cache_path = cached_path(session, f"{DATA_URL}:feather")
    if cache_path.exists():
        df = pandas.read_feather(cache_path)
    else:
        response = session.get(DATA_URL)
        response.raise_for_status()
        df = pandas.read_csv(
            io.StringIO(response.text),
            dtype={"fips_code": "Int64"},
            parse_dates=["collection_week"],
            date_parser=lambda v: pandas.to_datetime(v, utc=True),
        )

        df.dropna(subset=["fips_code"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        with temp_to_rename(cache_path) as temp_path:
            df.to_feather(temp_path)

    df.set_index(
        ["fips_code", "hospital_pk", "collection_week"],
        drop=True,
        inplace=True,
        verify_integrity=True,
    )
    df.sort_index(inplace=True)
    return df


def credits():
    return {"https://healthdata.gov/": "HealthData.gov"}


if __name__ == "__main__":
    import argparse
    import signal

    from covid import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Cache:", cached_path(session, f"{DATA_URL}:feather"))
    print("Loading hospitalizations...")
    df = get_hospitalizations(session)
    df.info(verbose=True, show_counts=True)
    print()

    print("=== FACILITIES ===")
    key_cols = ["fips_code", "hospital_pk"]
    for (fips, pk), rows in df.groupby(level=key_cols):
        last = rows.iloc[-1]
        print(f"--- fips={fips} pk={pk} t={last.name[-1]} ---")
        for key, val in last.iteritems():
            print(key)
            print(f"    {val}")
        print()
