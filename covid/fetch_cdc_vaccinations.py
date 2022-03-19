"""Module to receive US CDC vaccination data."""

import io

import pandas

from covid.cache_policy import cached_path
from covid.cache_policy import temp_to_rename

DATA_URL = "https://data.cdc.gov/api/views/8xkx-amqh/rows.csv"


def get_vaccinations(session):
    """Returns a DataFrame of county-level vaccination stats."""

    cache_path = cached_path(session, f"{DATA_URL}:feather")
    if cache_path.exists():
        df = pandas.read_feather(cache_path)
    else:
        response = session.get(DATA_URL)
        response.raise_for_status()
        df = pandas.read_csv(
            io.StringIO(response.text),
            dtype={"FIPS": "Int64"},
            na_values=["UNK"],
        )

        df = df[
            [
                "Date",
                "FIPS",
                "Administered_Dose1_Recip",
                "Series_Complete_Yes",
                "Booster_Doses",
            ]
        ]

        df.dropna(subset=["FIPS"], inplace=True)
        df.Date = pandas.to_datetime(df.Date, utc=True)
        df.reset_index(drop=True, inplace=True)
        with temp_to_rename(cache_path) as temp_path:
            df.to_feather(temp_path)

    df.set_index(
        ["FIPS", "Date"], drop=True, inplace=True, verify_integrity=True
    )
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
    parser.add_argument("--fips", type=int)
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Cache:", cached_path(session, f"{DATA_URL}:feather"))
    print("Loading vaccinations...")
    df = get_vaccinations(session)
    df.info(verbose=True, show_counts=True)
    print()

    if args.fips:
        for t in df.loc[args.fips].itertuples():
            print(
                t.Index,
                t.Administered_Dose1_Recip,
                t.Series_Complete_Yes,
                t.Booster_Doses,
            )

    else:
        print("=== COUNTY FIPS, DOSE1, COMPLETE, BOOSTER ===")
        for fips, sub_df in df.groupby(level="FIPS", as_index=False):
            sub_df.reset_index(level="FIPS", drop=True, inplace=True)
            latest = sub_df.iloc[-1]
            print(
                fips,
                latest.Administered_Dose1_Recip,
                latest.Series_Complete_Yes,
                latest.Booster_Doses,
            )
