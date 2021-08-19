"""Module to retrieve data from the HIT-Covid project
(https://akuko.io/post/covid-intervention-tracking)."""

import collections
import io

import numpy
import pandas


def get_data(session):
    """Returns a pandas.DataFrame of raw data from HIT-Covid."""

    response = session.get(
        "https://raw.githubusercontent.com/HopkinsIDD/hit-covid/master"
        "/data/hit-covid-longdata.csv"
    )
    response.raise_for_status()
    data = pandas.read_csv(
        io.StringIO(response.text),
        na_values=[""],
        keep_default_na=False,
        index_col="unique_id",
        parse_dates=["entry_time", "date_of_update"],
        dtype={
            "usa_county_code": "Int64",
            "size": "Int64",
            "duration": "Int64",
        },
    )

    # Use '' for missing string values for consistent typing & groupby().
    for col, dtype in data.dtypes.items():
        if pandas.api.types.is_string_dtype(dtype):
            data[col].fillna("", inplace=True)

    # TODO: Track implementation and record deltas?
    # It's tricky -- see Texas store closures, etc.

    sig = numpy.select(
        (
            numpy.equal(data.status_simp, "Implementation Suspended"),
            numpy.isin(
                data.status_simp,
                ("Strongly Implemented", "Partially Implemented"),
            ),
        ),
        (1, -1),
        default=0,
    )

    sig *= numpy.where(
        numpy.isin(
            data.intervention_group,
            (
                "school_closed",
                "household_confined",
                "office_closed",
                "entertainment_closed",
                "store_closed",
                "restaurant_closed",
                "state_of_emergency",
                "limited_mvt",
                "closed_border",
                "mask",
            ),
        )
        & numpy.equal(data.status_simp, "Strongly Implemented")
        & numpy.isin(data.subpopulation, ("entire population", "")),
        2,
        1,
    )

    data["significance"] = sig
    return data


if __name__ == "__main__":
    import argparse
    import textwrap

    from covid import cache_policy

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument("--region", nargs="*")
    args = parser.parse_args()
    selected = [r.lower().replace(" ", "") for r in args.region]

    data = get_data(session=cache_policy.new_session(args))
    data.sort_values("date_of_update", inplace=True)
    region_columns = ["country", "admin1_name", "locality", "usa_county"]
    for region, regional_data in data.groupby(region_columns):
        region_name = " / ".join(name for name in region if name)
        if selected and region_name.lower().replace(" ", "") not in selected:
            continue

        print(f"Region: {region_name}")
        for date, events in regional_data.groupby("date_of_update"):

            def abbr(t):
                return " ".join([w[:3] for w in t.split() if w])

            print(date.strftime("  %Y-%m-%d"))
            for e in events.itertuples():
                print(
                    f"    {e.significance:+d} "
                    f"[{e.intervention_group}] "
                    f'{abbr(e.required) or "?req"} / '
                    f'{abbr(e.subpopulation) or "?ent pop"} / '
                    f'{abbr(e.status_simp) or "?simp"}'
                )
                print(f"       {e.intervention_name}: {e.status}")
                detail = textwrap.TextWrapper(
                    subsequent_indent=" " * 7, initial_indent=" " * 7, width=79
                ).fill(e.details)
                print(detail + ("\n" if detail else ""))
