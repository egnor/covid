"""Generate specialized plot of Cal-SuWers data"""


import numpy

import covid.build_atlas
import covid.fetch_calsuwers_wastewater
import covid.merge_covid_metrics
import covid.region_data


def get_lab_site_metrics(session):
    out = {}
    atlas = covid.build_atlas.get_atlas(session)
    covid.merge_covid_metrics.add_metrics(session, atlas)

    wet_L_div = 3000
    dry_g_div = 3000

    print("Loading Cal-SuWers wastewater data...")
    df = covid.fetch_calsuwers_wastewater.get_wastewater(session)
    for wwtp, wwtp_rows in df.groupby(level="wwtp_name", sort=False):
        wwtp_rows.reset_index("wwtp_name", drop=True, inplace=True)

        wwtp_first = wwtp_rows.iloc[0]
        site = f"{wwtp} ({wwtp_first['FACILITY NAME']})"
        fips = wwtp_first.county_names.split(",")[0].strip()
        fips = int(covid.fetch_calsuwers_wastewater.FIPS_FIX.get(fips, fips))
        region = atlas.by_fips[fips]

        covid_key = "COVID positives / day / 100Kp"
        covid_metrics = region.metrics.covid[covid_key]

        for (target, lab), lab_rows in wwtp_rows.groupby(
            level=["pcr_target", "lab_id"]
        ):
            assert target == "sars-cov-2"
            metrics = out.setdefault(lab, {}).setdefault(site, {})
            metrics[covid_key] = covid_metrics

            for (units, gene), rows in lab_rows.groupby(
                level=["pcr_gene_target", "pcr_target_units"]
            ):
                samples = rows.pcr_target_avg_conc
                if units[:6] == "log10 ":
                    samples = numpy.power(10.0, samples)
                    units = units[6:]
                if units.lower() == "copies/l wastewater":
                    samples = samples / wet_L_div
                    units = f"{units} / {wet_L_div:.0f}"
                elif units.lower() == "copies/g dry sludge":
                    samples = samples / dry_g_div
                    units = f"{units} / {dry_g_div:.0f}"

                title = f"{target}({gene}) {units}"
                metrics[title] = covid.region_data.make_metric(
                    c="tab:green",
                    em=1,
                    ord=1.0,
                    raw=samples.groupby("sample_collect_date").mean(),
                )

    return out


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    lab_site_metrics = get_lab_site_metrics(session)
    for lab, site_metrics in sorted(lab_site_metrics.items()):
        name = covid.fetch_calsuwers_wastewater.LAB_NAMES[lab]
        print(f"=== {lab} ({name}) ===")
        for site, metrics in sorted(site_metrics.items()):
            print(f"  {site}")
            for name, metric in sorted(metrics.items()):
                print(f"    {metric.debug_line()} {name}")
        print()
