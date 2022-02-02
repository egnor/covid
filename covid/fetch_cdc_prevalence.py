"""Module to receive US CDC seroprevalence estimates."""

import io

import pandas

INFECTION_URL = "https://data.cdc.gov/api/views/mtc3-kq6r/rows.csv"
COMBINED_URL = "https://data.cdc.gov/api/views/wi5c-cscz/rows.csv"


def _get_dataset(session, url):
    response = session.get(url)
    response.raise_for_status()
    data = io.StringIO(response.text)
    df = pandas.read_csv(data)
    df.rename(columns=lambda c: " ".join(c.split()), inplace=True)
    for date_col in ("Year and Month", "Median Donation Date"):
        df[date_col] = pandas.to_datetime(df[date_col], utc=True)
    df.set_index(
        ["Region Abbreviation", "Region", "Year and Month"], inplace=True
    )
    return df


def get_prevalence(session):
    infection = _get_dataset(session, INFECTION_URL)
    combined = _get_dataset(session, COMBINED_URL)
    return infection.join(
        combined, how="outer", lsuffix=" Infection", rsuffix=" Combined"
    )


def get_credits():
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
