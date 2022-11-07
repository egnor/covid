"""Module to retrieve COVID vaccination data from ourworldindata.org."""

import io

import pandas

REPO_DIR = "https://raw.githubusercontent.com/owid/covid-19-data/master"
HOSPITAL_DATA_DIR = f"{REPO_DIR}/public/data/hospitalizations"
HOSPITAL_CSV_URL = f"{HOSPITAL_DATA_DIR}/covid-hospitalizations.csv"


def _get_table(session, prefix, mult):
    response = session.get(HOSPITAL_CSV_URL)
    response.raise_for_status()
    df = pandas.read_csv(io.StringIO(response.text))
    df.date = pandas.to_datetime(df.date, utc=True, dayfirst=True)
    df = df[
        df.indicator.str.startswith(prefix + " ")
        & ~df.indicator.str.endswith(" per million")
    ]
    df.indicator = df.indicator.str.slice(start=len(prefix) + 1)
    df.value = df.value * mult
    return df.pivot(["iso_code", "date"], "indicator", "value")


def get_occupancy(session):
    return _get_table(session, "Daily", 1)


def get_admissions(session):
    return _get_table(session, "Weekly", 1 / 7)


def credits():
    return {"https://ourworldindata.org/": "Our World In Data"}


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading occupancy...")
    occ = get_occupancy(session)
    occ.info(verbose=True, show_counts=True)
    print()

    print("Loading admissions...")
    adm = get_admissions(session)
    adm.info(verbose=True, show_counts=True)
    print()

    print("=== OCCUPANCY ===")
    print(occ.groupby(["iso_code"]).last())
    print()

    print("=== ADMISSIONS ===")
    print(adm.groupby(["iso_code"]).last())
    print()
