"""Function to merge variant metrics into a RegionAtlas"""

import itertools
import logging
import warnings

import matplotlib
import pycountry

import covid.fetch_covariants
from covid.region_data import make_metric


def add_metrics(session, atlas):
    logging.info("Loading and merging CoVariants data...")
    cov_credits = covid.fetch_covariants.credits()
    covar = covid.fetch_covariants.get_variants(session=session)

    totals = covar.groupby("variant")["found"].sum()
    vars = [v[0] for v in sorted(totals.items(), key=lambda v: v[1])]
    colors = dict(zip(vars, itertools.cycle(matplotlib.cm.tab20.colors)))

    region_cols = ["country", "region"]
    covar.sort_values(region_cols + ["date"], inplace=True)
    covar.set_index(keys="date", inplace=True)
    for r, rd in covar.groupby(region_cols, as_index=False, sort=False):
        if (r[0], r[1]) == ("United States", "USA"):
            continue  # Covered separately as ("USA", "").

        c_find = {
            "Curacao": "Curaçao",
            "Laos": "Lao People's Democratic Republic",
            "South Korea": "Republic Of Korea",
            "Sint Maarten": "Sint Maarten (Dutch part)",
            "Democratic Republic of the Congo": "Congo, The Democratic Republic of the",
        }.get(r[0], r[0])
        try:
            countries = [pycountry.countries.lookup(c_find)]
        except LookupError:
            try:
                countries = pycountry.countries.search_fuzzy(c_find)
            except LookupError:
                warnings.warn(f'Unknown covariant country: "{c_find}"')
                continue

        region = atlas.by_iso2.get(countries[0].alpha_2)
        if region is None:
            continue  # Valid country but not in skeleton

        r_find = {"Washington DC": "District of Columbia"}.get(r[1], r[1])
        if r_find:
            path, region = region.path(), region.subregions.get(r_find)
            if region is None:
                warnings.warn(f"Unknown covariant region: {path}/{r_find}")
                continue

        v_totals = v_others = []
        for v, vd in rd.groupby("variant", as_index=False):
            if not v:
                v_others = vd.found
                v_totals = vd.found
                continue

            if v in region.metrics["variant"]:
                warnings.warn(f"Duplicate covariant ({region.path()}): {v}")
                continue

            if len(v_totals) != len(vd):
                warnings.warn(
                    f"Bad covariant data ({region.path()}): "
                    f"len totals={len(v_totals)} len data={len(vd)}"
                )
                continue

            v_others = v_others - vd.found
            region.metrics["variant"][v] = make_metric(
                c=colors[v],
                em=1,
                ord=0,
                cred=cov_credits,
                v=vd.found * 100.0 / v_totals,
            )

        other_variants = make_metric(
            c=(0.9, 0.9, 0.9),
            em=1,
            ord=0,
            cred=cov_credits,
            v=v_others * 100.0 / v_totals,
        )

        region.metrics["variant"] = {
            "original/other": other_variants,
            **region.metrics["variant"],
        }


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
