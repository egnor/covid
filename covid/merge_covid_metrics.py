"""Function to merge basic case metrics into a RegionAtlas"""

import logging
from warnings import warn

import covid.fetch_jhu_csse
from covid.region_data import make_metric


def add_metrics(session, atlas):
    logging.info("Loading JHU CSSE dataset...")
    jhu_covid = covid.fetch_jhu_csse.get_covid(session)

    logging.info("Merging JHU CSSE dataset...")
    for id, df in jhu_covid.groupby(level="ID", sort=False):
        region = atlas.by_jhu_id.get(id)
        if not region:
            continue  # Pruned out of the skeleton

        if df.empty:
            warn(f"No COVID data: {region.debug_path()}")
            continue

        df.Confirmed.fillna(method="ffill", inplace=True)
        df.Deaths.fillna(method="ffill", inplace=True)

        pos, deaths = df.Confirmed.iloc[-1], df.Deaths.iloc[-1]
        pop = region.metrics.total["population"]
        if not (0 <= pos <= pop + 1000):
            warn(f"Bad positives: {region.debug_path()} ({pos}/{pop}p)")
            continue
        if not (0 <= deaths <= pop + 1000):
            warn(f"Bad deaths: {region.debug_path()} ({deaths}/{pop}p)")
            continue

        df.reset_index(level="ID", drop=True, inplace=True)
        region.metrics.total["positives"] = pos
        region.metrics.total["deaths"] = deaths
        region.credits.update(covid.fetch_jhu_csse.credits())

        region.metrics.covid["COVID positives / day / 100Kp"] = make_metric(
            c="tab:blue",
            em=1,
            ord=1.0,
            cum=df.Confirmed * 1e5 / pop,
        )

        region.metrics.covid["COVID deaths / day / 10Mp"] = make_metric(
            c="tab:red",
            em=1,
            ord=1.3,
            cum=df.Deaths * 1e7 / pop,
        )


if __name__ == "__main__":
    import argparse

    from covid import build_atlas
    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    parser.add_argument("--print_data", action="store_true")

    args = parser.parse_args()
    session = cache_policy.new_session(args)
    atlas = build_atlas.get_atlas(session)
    add_metrics(session=session, atlas=atlas)
    print(atlas.world.debug_tree(with_data=args.print_data))
