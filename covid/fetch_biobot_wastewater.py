"""Module to fetch wastewater data from Biobot Analytics."""

import io

import pandas

DATA_URL = "https://github.com/biobotanalytics/covid19-wastewater-data/raw/master/wastewater_by_county.csv"


def get_wastewater(session):
    """Returns a DataFrame of wastewater sampling data."""

    response = session.get(DATA_URL)
    response.raise_for_status()
    df = pandas.read_csv(
        io.StringIO(response.text),
        date_parser=lambda v: pandas.to_datetime(v, utc=True),
        parse_dates=["sampling_week"],
        dtype={
            "effective_concentration_rolling_average": float,
            "region": str,
            "state": str,
            "name": str,
            "fipscode": int,
        },
    )

    key_cols = ["fipscode", "sampling_week"]
    df.set_index(key_cols, drop=True, inplace=True, verify_integrity=True)
    return df.sort_index()


def credits():
    return {"https://biobot.io/data/": "Biobot Analytics"}


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading Biobot Analytics wastewater data...")
    df = get_wastewater(session)
    df.info(verbose=True, show_counts=True)
    print()

    for fips, site_rows in df.groupby(level="fipscode"):
        site_rows.reset_index("fipscode", drop=True, inplace=True)
        first = site_rows.iloc[0]
        print(f"=== [{fips}] {first['name']} ===")
        for row in site_rows.itertuples():
            print(
                f"{row.Index.strftime('%Y-%m-%d')} "
                f"{row.effective_concentration_rolling_average:.1f}"
            )
        print()
