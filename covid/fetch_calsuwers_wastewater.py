"""Module to fetch wastewater data from Cal-SuWers (CDPH)."""

import io
import re

import pandas

DATA_URL = "https://data.ca.gov/dataset/b8c6ee3b-539d-4d62-8fa2-c7cd17c16656/resource/16bb2698-c243-4b66-a6e8-4861ee66f8bf/download/master-covid-public.csv"


def get_wastewater(session):
    """Returns a DataFrame of wastewater sampling data."""

    response = session.get(DATA_URL)
    response.raise_for_status()
    df = pandas.read_csv(
        io.StringIO(response.text),
        date_parser=lambda v: pandas.to_datetime(v, utc=True),
        parse_dates=["sample_collect_date"],
        thousands=',',
    )

    key_cols = [
        "wwtp_name",
        "sample_id",
        "lab_id",
        "pcr_target",
        "pcr_gene_target",
    ]

    df.set_index(key_cols, drop=True, inplace=True, verify_integrity=False)
    return df.sort_index()


def credits():
    return {
        "https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/CalSuWers-Dashboard.aspx":
            "Cal-SuWers Network",
    }


def abbrev(text):
    text = re.sub("\\bpepper mild mottle virus\\b", "PMMoV", text, flags=re.I)
    text = re.sub("\\bbcov vaccine\\b", "BCoV", text, flags=re.I)
    text = re.sub("\\bL wastewater\\b", "wet L", text, flags=re.I)
    text = re.sub("\\bg dry sludge\\b", "dry g", text, flags=re.I)
    text = re.sub("\\blog10 copies\\b", "log-cp", text, flags=re.I)
    text = re.sub("\\bcopies\\b", "cp", text, flags=re.I)
    return text


if __name__ == "__main__":
    import argparse

    from covid import cache_policy
    from covid import logging_policy  # noqa

    parser = argparse.ArgumentParser(parents=[cache_policy.argument_parser])
    args = parser.parse_args()
    session = cache_policy.new_session(args)

    print("Loading Cal-SuWers wastewater data...")
    df = get_wastewater(session)
    df.info(verbose=True, show_counts=True)
    print()

    for site, site_rows in df.groupby(level="wwtp_name"):
        row = site_rows.iloc[-1]
        print(
            f"=== [{row.county_names}] {site} " + (
                f"{row.sample_location} ({row.sample_location_specify}) "
                if row.sample_location != "wwtp"
                else ""
            ) + (
                f"({row.institution_type}: "
                if row.institution_type != "not institution specific"
                else "("
            ) +
            f"{row['FACILITY NAME']}) {row.population_served}p "
            f"cap={row.capacity_mgd}mgd " + (
                f"{row.sewage_travel_time:.0f}h "
                if pandas.notna(row.sewage_travel_time)
                else ""
            ) + " ==="
        )

        for (lab, target, gene), target_rows in site_rows.groupby(
            level=["lab_id", "pcr_target", "pcr_gene_target"]
        ):
            print(f"{site} / {lab}: {target} ({gene.upper()})")
            for sample, sample_rows in target_rows.groupby(level="sample_id"):
                row = sample_rows.iloc[0]
                print(
                    f"  {row.sample_collect_date.strftime('%Y-%m-%d')}" +
                    (
                        f" {sample} {row.pcr_target_avg_conc:4.1f}"
                        if "log10" in row.pcr_target_units else
                        f" {sample} {row.pcr_target_avg_conc / 1e3:4.0f}K"
                    ) +
                    f" {abbrev(row.pcr_target_units)}" +
                    (
                        (
                            f"; {row.hum_frac_mic_conc:4.1f}"
                            if "log10" in row.pcr_target_units else
                            f" {row.hum_frac_mic_conc / 1e6:4.0f}M"
                        ) +
                        f" {abbrev(row.hum_frac_target_mic)}"
                        if pandas.notna(row.hum_frac_mic_conc) else ""
                    ) + (
                        f"; {row.hum_frac_chem_conc}"
                        f" {row.hum_frac_target_chem}"
                        if pandas.notna(row.hum_frac_chem_conc) else ""
                    ) + (
                        f"; {row.other_norm_conc}"
                        f" {row.other_norm_name}"
                        if pandas.notna(row.other_norm_conc) else ""
                    ) + (
                        f" {row.flow_rate:5.1f}mgd"
                        if pandas.notna(row.flow_rate) else ""
                    ) + (
                        f" {row.ph:.1f}pH" if pandas.notna(row.ph) else ""
                    ) + (
                        f" {row.conductivity:4.0f}μS"
                        if pandas.notna(row.conductivity) else ""
                    ) + (
                        f" tss={row.tss:.0f}mg/L"
                        if pandas.notna(row.tss) else ""
                    ) + (
                        f" {row.collection_water_temp:.1f}C"
                        if pandas.notna(row.collection_water_temp) else ""

                    ) + (
                        f" amt={row.equiv_sewage_amt:.1f}"
                        if pandas.notna(row.equiv_sewage_amt) and
                           row.equiv_sewage_amt != 1.0
                        else ""
                    ) + (
                        f" rec={row.rec_eff_percent:.0f}%"
                        if pandas.notna(row.rec_eff_percent) and
                           row.rec_eff_percent >= 0
                        else ""
                    ) +
                    (" NTC-fail" if row.ntc_amplify == "yes" else "") +
                    (" inh" if row.inhibition_detect == "yes" else "") +
                    ("-adj" if row.inhibition_adjust == "yes" else "") +
                    (" !QC" if row.qc_ignore == "yes" else "") +
                    (" !dash" if row.dashboard_ignore == "yes" else "") +
                    (" !anal" if row.analysis_ignore == "yes" else "")
                )

            print()
