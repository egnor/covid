"""Function to merge basic case metrics into a RegionAtlas"""

import logging
import warnings

import covid.fetch_jhu_csse
from covid.region_data import make_metric


def add_metrics(session, atlas):
    logging.info("Loading JHU CSSE dataset...")
    jhu_credits = covid.fetch_jhu_csse.credits()
    jhu_covid = covid.fetch_jhu_csse.get_covid(session)

    logging.info("Merging JHU CSSE dataset...")
    for id, df in jhu_covid.groupby(level="ID", sort=False):
        region = atlas.by_jhu_id.get(id)
        if not region:
            continue  # Pruned out of the skeleton

        if df.empty:
            warnings.warn(f"No COVID data: {region.path()}")
            continue

        df.Confirmed.fillna(method="ffill", inplace=True)
        df.Deaths.fillna(method="ffill", inplace=True)

        pos, deaths = df.Confirmed.iloc[-1], df.Deaths.iloc[-1]
        pop = region.totals["population"]
        if not (0 <= pos <= pop + 1000):
            warnings.warn(f"Bad positives: {region.path()} ({pos}/{pop}p)")
            continue
        if not (0 <= deaths <= pop + 1000):
            warnings.warn(f"Bad deaths: {region.path()} ({deaths}/{pop}p)")
            continue

        df.reset_index(level="ID", drop=True, inplace=True)
        region.totals["positives"] = pos
        region.totals["deaths"] = deaths

        region.metrics["covid"]["COVID positives / day / 100Kp"] = make_metric(
            c="tab:blue",
            em=1,
            ord=1.0,
            cred=jhu_credits,
            cum=df.Confirmed * 1e5 / pop,
        )

        region.metrics["covid"]["COVID deaths / day / 10Mp"] = make_metric(
            c="tab:red",
            em=1,
            ord=1.3,
            cred=jhu_credits,
            cum=df.Deaths * 1e7 / pop,
        )

        region.metrics["covid"]["cum positives / 100p"] = make_metric(
            c="tab:cyan",
            em=0,
            ord=1.6,
            cred=jhu_credits,
            v=df.Confirmed * 100 / pop,
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
