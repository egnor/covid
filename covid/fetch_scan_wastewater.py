"""Module to fetch wastewater data from Wastewater SCAN (Stanford/Verily)."""

import io

import pandas

DATA_URL = "https://publichealth.verily.com/api/csv"


def get_wastewater(session):
    """Returns a DataFrame of wastewater sampling data."""

    response = session.get(DATA_URL)
    response.raise_for_status()
    df = pandas.read_csv(
        io.StringIO(response.text),
        parse_dates=["Collection_Date"],
        date_parser=lambda v: pandas.to_datetime(v, utc=True),
    )

    key_cols = ["Site_Name", "County_FIPS", "Collection_Date"]
    df.County_FIPS.fillna('', inplace=True)
    df.set_index(key_cols, drop=True, inplace=True, verify_integrity=False)
    return df.sort_index()


def credits():
    return {
        "https://wastewaterscan.org/": "Sewer Coronavirus Alert Network",
    }


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading SCAN wastewater data...")
    df = get_wastewater(session)
    df.info(verbose=True, show_counts=True)
    print()

    for site, rows in df.groupby(level=["County_FIPS", "Site_Name"]):
        timestamps = set()
        print(f"=== {site[0]}, {site[1]} / {site[2]} ===")
        for row in rows.itertuples():
            city, state, site, timestamp = row.Index
            print(
                f"{row.Sample_ID} {timestamp.strftime('%Y-%m-%d')} "
                f"N={row.SC2_N_gc_g_dry_weight:<10.1f} "
                f"S={row.SC2_S_gc_g_dry_weight:<10.1f} "
                f"RSV={row.RSV_gc_g_dry_weight:<10.1f} "
                f"PMMoV={row.PMMoV_gc_g_dry_weight:<10.1f}"
            )
            if timestamp in timestamps:
                print("*** DUP TIMESTAMP", site, timestamp)
            timestamps.add(timestamp)

        print()
