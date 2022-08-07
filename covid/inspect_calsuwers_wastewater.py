"""Generate specialized plot of Cal-SuWers data"""

import dataclasses

import matplotlib
import matplotlib.pyplot
import numpy

import covid.build_atlas
import covid.fetch_calsuwers_wastewater
import covid.merge_covid_metrics
import covid.plot_metrics
import covid.region_data


@dataclasses.dataclass(frozen=True, order=True)
class LabId:
    id: str
    name: str


@dataclasses.dataclass(frozen=True, order=True)
class SiteId:
    id: str
    name: str
    pop: int


def get_lab_site_metrics(session):
    out = {}
    atlas = covid.build_atlas.get_atlas(session)
    covid.merge_covid_metrics.add_metrics(session, atlas)

    wet_L_div = 3000
    dry_g_div = 3000
    pmmov_div = 10000000

    print("Loading Cal-SuWers wastewater data...")
    df = covid.fetch_calsuwers_wastewater.get_wastewater(session)
    for wwtp, wwtp_rows in df.groupby(level="wwtp_name", sort=False):
        wwtp_rows.reset_index("wwtp_name", drop=True, inplace=True)
        wwtp_first = wwtp_rows.iloc[0]
        site = SiteId(
            id=wwtp,
            name=wwtp_first["FACILITY NAME"],
            pop=wwtp_first.population_served,
        )

        fips = wwtp_first.county_names.split(",")[0].strip()
        fips = int(covid.fetch_calsuwers_wastewater.FIPS_FIX.get(fips, fips))
        region = atlas.by_fips[fips]

        covid_key = "COVID positives / day / 100Kp"
        covid_metric = dataclasses.replace(
            region.metrics.covid[covid_key], color="tab:gray"
        )

        for (target, lab_id), lab_rows in wwtp_rows.groupby(
            level=["pcr_target", "lab_id"]
        ):
            assert target == "sars-cov-2"
            lab = LabId(
                id=lab_id,
                name=covid.fetch_calsuwers_wastewater.LAB_NAMES[lab_id]
            )

            metrics = out.setdefault(lab, {}).setdefault(site, {})
            metrics[covid_key] = covid_metric

            for (gene, units), rows in lab_rows.groupby(
                level=["pcr_gene_target", "pcr_target_units"]
            ):
                samples = rows.pcr_target_avg_conc
                if units[:6] == "log10 ":
                    samples = numpy.power(10.0, samples)
                    units = units[6:]
                if units.lower() == "copies/l wastewater":
                    samples = samples / wet_L_div
                    units = f"{units}/{wet_L_div:.0f}"
                elif units.lower() == "copies/g dry sludge":
                    samples = samples / dry_g_div
                    units = f"{units}/{dry_g_div:.0f}"

                title = f"{target}({gene}) {units}"
                metrics[title] = covid.region_data.make_metric(
                    c="tab:green",
                    em=1,
                    ord=1.0,
                    raw=samples.groupby("sample_collect_date").mean(),
                )

                flow = rows.flow_rate.groupby("sample_collect_date").mean()
                metrics["flow L/p/day"] = covid.region_data.make_metric(
                    c="tab:blue",
                    em=-1,
                    ord=1.0,
                    raw=flow * 4.54609e6 / site.pop,
                )

                tss = rows.tss.groupby("sample_collect_date").mean()
                metrics["tss mg/L"] = covid.region_data.make_metric(
                    c="tab:brown",
                    em=-1,
                    ord=1.0,
                    raw=tss,
                )

                hum = rows.hum_frac_mic_conc
                hum = hum.groupby("sample_collect_date").mean()
                if hum.count() > 1:
                    hum_target = rows.iloc[0].hum_frac_target_mic
                    hum_units = rows.iloc[0].hum_frac_mic_unit
                    if hum_target.lower() == "pepper mild mottle virus":
                        hum_target = "PMMoV"
                        hum = hum / pmmov_div
                        hum_units = f"{hum_units}/{pmmov_div:.0f}"

                    title = f"{hum_target} {hum_units}"
                    metrics[title] = covid.region_data.make_metric(
                        c="tab:purple",
                        em=-1,
                        ord=1.0,
                        raw=hum,
                    )

            for name, metric in list(metrics.items()):
                if metric.frame.value.count() < 2:
                    del metrics[name]
                

    return out


def write_plots(lab_site_metrics):
    for lab, site_metrics in lab_site_metrics.items():
        rows = len(site_metrics)
        fig = matplotlib.pyplot.figure(figsize=(10, 5 * rows), dpi=200)
        subplots = fig.subplots(nrows=rows, ncols=1, sharex=True, squeeze=False)

        for plot_i, (site, metrics) in enumerate(site_metrics.items()):
            axes = subplots[plot_i, 0]
            covid.plot_metrics.setup_yaxis(axes, ylim=(0, 500))
            covid.plot_metrics.setup_xaxis(
                axes,
                title=f"{lab.name}\n{site.name} ({site.pop:,}p)",
                titlesize=30,
                wrapchars=30,
            )

            covid.plot_metrics.plot_metrics(axes, metrics)
            covid.plot_metrics.plot_legend(axes)

        filename = urls.file(
            "wastewater_out", f"wastewater_{lab.id.lower()}.png"
        )
        print(f"Writing: {filename}")

        fig.align_ylabels()
        fig.tight_layout(pad=0, h_pad=1)
        fig.savefig(filename)
        matplotlib.pyplot.close(fig)

    print()


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    lab_site_metrics = get_lab_site_metrics(session)
    for lab, site_metrics in sorted(lab_site_metrics.items()):
        print(f"=== {lab.id} ({lab.name}) ===")
        for site, metrics in sorted(site_metrics.items()):
            print(f"  {site.id} ({site.name}) {site.pop}p")
            for name, metric in sorted(metrics.items()):
                print(f"    {metric.debug_line()} {name}")
            print()

    write_plots(lab_site_metrics)
