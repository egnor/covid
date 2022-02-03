"""Module to retrieve data from the JHU CSSE dashboard."""

import io

import pandas

from covid.cache_policy import cached_path
from covid.cache_policy import temp_to_rename

REPO_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master"
LOOKUP_CSV_URL = f"{REPO_URL}/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv"
REPORTS_URL = f"{REPO_URL}/csse_covid_19_data/csse_covid_19_daily_reports"

_places = None


def get_places(session):
    """Returns a DataFrame of place metadata."""

    global _places
    if _places is None:
        response = session.get(LOOKUP_CSV_URL)
        response.raise_for_status()
        data = io.StringIO(response.text)
        _places = pandas.read_csv(data, na_values="", keep_default_na=False)
        for str_col in _places.select_dtypes("object").columns:
            _places[str_col].fillna("", inplace=True)
        for int_col in ["code3", "FIPS", "Population"]:
            _places[int_col].fillna(0, inplace=True)
            _places[int_col] = _places[int_col].astype(int)
        _places.rename(columns={"UID": "ID"}, inplace=True)
        _places.set_index("ID", inplace=True)
    return _places


def get_covid(session):
    """Returns a DataFrame of COVID-19 daily records."""

    cache_path = cached_path(session, f"{REPORTS_URL}:feather")
    if cache_path.exists():
        df = pandas.read_feather(cache_path)
    else:
        reports = []
        start_day = pandas.Timestamp("01-22-2020", tz="UTC")
        days = pandas.date_range(start_day, pandas.Timestamp.now(tz="UTC"))
        for day in days:
            mm_dd_yyyy = day.strftime("%m-%d-%Y")
            response = session.get(f"{REPORTS_URL}/{mm_dd_yyyy}.csv")
            if response.status_code != 404:
                response.raise_for_status()
                data = io.StringIO(response.text)
                rep = pandas.read_csv(data, na_values="", keep_default_na=False)
                rep.drop(
                    columns=[
                        "Active",
                        "Case-Fatality_Ratio",
                        "Case_Fatality_Ratio",
                        "Combined_Key",
                        "FIPS",
                        "Incident_Rate",
                        "Incidence_Rate",
                        "Lat",
                        "Long_",
                        "Latitude",
                        "Longitude",
                        "Recovered",
                    ],
                    inplace=True,
                    errors="ignore",
                )
                rep.rename(
                    columns={
                        "Country/Region": "Country_Region",
                        "Last Update": "Last_Update",
                        "Province/State": "Province_State",
                    },
                    inplace=True,
                )

                rep["Date"] = day
                rep.Last_Update = pandas.to_datetime(rep.Last_Update, utc=True)
                reports.append(rep)

        place_cols = ["Country_Region", "Province_State", "Admin2"]
        ids = get_places(session).reset_index().set_index(place_cols)[["ID"]]

        df = pandas.concat(reports, ignore_index=True)
        for str_col in df.select_dtypes("object").columns:
            df[str_col].fillna("", inplace=True)

        df = df.merge(ids, left_on=place_cols, right_index=True)
        df.drop(columns=place_cols, inplace=True)
        df.reset_index(drop=True, inplace=True)
        with temp_to_rename(cache_path) as temp_path:
            df.to_feather(temp_path)

    df = df.groupby(["ID", "Date"], as_index=False).first()
    df.set_index(["ID", "Date"], inplace=True, verify_integrity=True)
    return df


def credits():
    return {"https://coronavirus.jhu.edu/": "JHU COVID Resource Center"}


if __name__ == "__main__":
    import argparse
    import signal

    from covid import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)  # Sane ^C behavior
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument("--id", type=int)
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading places...")
    places = get_places(session)
    places.info()
    print()

    print("Cache:", cached_path(session, f"{REPORTS_URL}:feather"))
    print("Loading COVID data...")
    covid = get_covid(session)
    covid.info()
    print()

    if args.id:
        print(f"=== PLACE ID={args.id} ===")
        place = places.loc[args.id]
        print(place)
        print()

        print(f"=== COVID ID={args.id} ===")
        print(covid.loc[args.id])

    else:
        print("=== PLACES ===")
        for place in places.itertuples():
            print(
                f"id={place.Index:<8} "
                f"pop={place.Population:<9} "
                f"{place.iso2:<2}/{place.iso3:<3}/{place.code3:<3} "
                + (f"f={place.FIPS:<5} " if place.FIPS else "")
                + f"{place.Country_Region}/{place.Province_State}/"
                f"{place.Admin2} "
            )
