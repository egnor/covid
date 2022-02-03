"""Module to retrieve variant fraction data from coviariants.org."""

import collections

import pandas

REPO_DIR = "https://raw.githubusercontent.com/hodcroftlab/covariants/master"
CLUSTER_TABLES_DIR = f"{REPO_DIR}/cluster_tables"
COUNTRY_JSON_FILES = [
    (None, f"{CLUSTER_TABLES_DIR}/EUClusters_data.json"),
    ("United States", f"{CLUSTER_TABLES_DIR}/USAClusters_data.json"),
]


def get_variants(session):
    out = collections.defaultdict(list)
    for country, url in COUNTRY_JSON_FILES:
        country_response = session.get(url)
        country_response.raise_for_status()
        country_json = country_response.json()
        for place, place_json in country_json["countries"].items():
            place_dates = ()
            for key, data in place_json.items():
                if key == "week":
                    place_dates = pandas.to_datetime(data, format="%Y-%m-%d")
                    continue

                if len(data) != len(place_dates):
                    raise ValueError(
                        f'{url}:\n  country="{country}" place="{place}":\n  '
                        f"dates={len(place_dates)} != data={len(data)}"
                    )

                variant = "" if key == "total_sequences" else key
                out["country"].extend([country or place or ""] * len(data))
                out["region"].extend([(country and place) or ""] * len(data))
                out["variant"].extend([variant] * len(data))
                out["date"].extend(place_dates)
                out["found"].extend(data)

    return pandas.DataFrame(out)


def credits():
    return {"https://covariants.org/": "CoVariants.org"}


if __name__ == "__main__":
    import argparse
    import signal

    from covid import cache_policy

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    df = get_variants(session)
    for v in df.itertuples():
        print(v)
        print()
