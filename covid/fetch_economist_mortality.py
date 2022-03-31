"""Module to load The Economist's excess mortality model."""

import io

import pandas

DATA_URL = (
    "https://raw.githubusercontent.com/TheEconomist/"
    "covid-19-the-economist-global-excess-deaths-model/"
    "main/output-data/export_country.csv"
)


def get_mortality(session):
    """Returns a DataFrame of modeled excess deaths."""

    response = session.get(DATA_URL)
    response.raise_for_status()
    data = io.StringIO(response.text)
    df = pandas.read_csv(data, na_values="NA", keep_default_na=False)
    df.date = pandas.to_datetime(df.date, utc=True)
    df.set_index(["iso3c", "date"], inplace=True)
    return df


def credits():
    return {
        "https://www.economist.com/graphic-detail/"
        "coronavirus-excess-deaths-estimates": "The Economist"
    }


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading excess mortality model...")
    df = get_mortality(session)
    df.info(verbose=True, show_counts=True)
    print()

    print("=== COUNTRIES ===")
    for iso3, rows in df.groupby(level="iso3c"):
        print(f"--- {iso3} (pop={rows.iloc[0].population}) ---")
        rows.reset_index("iso3c", drop=True, inplace=True)
        for row in rows.itertuples():
            est = row.estimated_daily_excess_deaths
            act = row.daily_excess_deaths
            cov = row.daily_covid_deaths
            print(
                f"{row.Index} est={est:<+7.1f} "
                f"act={act:<+7.1f} cov={cov:<7.1f}")
        print()
