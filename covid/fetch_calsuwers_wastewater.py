"""Module to fetch wastewater data from Cal-SuWers (CDPH)."""

import io
import re

import pandas

DATA_URL = "https://data.ca.gov/datastore/dump/16bb2698-c243-4b66-a6e8-4861ee66f8bf?format=csv"

# Lab codes via communication from the Cal-SuWers team
LAB_NAMES = {
    "A": "Luminultra",
    "B": "Luminultra",
    "Biobot001": "Biobot",
    "CAL1": "SCCWRP",
    "CAL2": "LACSD",
    "CAL3": "Zymo Research",
    "CAL4": "UC Berkeley",
    "CAL5EURFNS": "Eurofins / HCVT",
    "DWRL": "CDPH DWRL",
    "GTM": "GT Molecular",
    "VLT": "Verily / SCAN / HCVT",
}

# Some rows incorrectly use names instead of FIPS codes
FIPS_FIX = {
    "Los Angeles": 6037,
    "Orange County": 6059,
}


def get_wastewater(session):
    """Returns a DataFrame of wastewater sampling data."""

    response = session.get(DATA_URL)
    response.raise_for_status()
    df = pandas.read_csv(
        io.StringIO(response.text),
        date_parser=lambda v: pandas.to_datetime(v, utc=True),
        parse_dates=["sample_collect_date", "test_result_date"],
        thousands=",",
        na_values=["not tested", "more than 3"],
        true_values=["t", "T", "yes", "Yes", "YES"],
        false_values=["f", "F", "no", "No", "NO"],
        low_memory=False,
        dtype={
            "analysis_ignore": "boolean",
            "collection_storage_temp": float,
            "dashboard_ignore": "boolean",
            "ext_blank": "boolean",
            "industrial_input": float,
            "influent_equilibriated": "boolean",
            "inhibition_adjust": "boolean",
            "inhibition_detect": "boolean",
            "major_lab_method": "Int32",
            "major_lab_method_desc": str,
            "num_no_target_control": "Int32",
            "other_norm_name": str,
            "other_norm_ref": str,
            "pasteurized": "boolean",
            "pcr_target_below_lod": "boolean",
            "pretreatment": "boolean",
            "quality_flag": "boolean",
            "qc_ignore": "boolean",
            "sample_collect_time": str,
            "sample_location_specify": str,
            "solids_separation": str,
            "stormwater_input": "boolean",
        },
    )

    df.pcr_gene_target = df.pcr_gene_target.str.upper()

    key_cols = [
        "wwtp_name",
        "pcr_target",
        "lab_id",
        "pcr_gene_target",
        "pcr_target_units",
        "sample_collect_date",
    ]

    df.set_index(key_cols, drop=True, inplace=True, verify_integrity=False)
    return df.sort_index()


def credits():
    return {
        "https://cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/CalSuWers-Dashboard.aspx": "Cal-SuWers Network",
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

    print("### Labs ###")
    lab_site_targets = {}
    for site, site_rows in df.groupby(level="wwtp_name"):
        for (target, lab, gene, units), target_rows in site_rows.groupby(
            ["pcr_target", "lab_id", "pcr_gene_target", "pcr_target_units"]
        ):
            targets = lab_site_targets.setdefault(lab, {}).setdefault(site, [])
            targets.append(f"{target}({gene}) {units}")

    for lab, site_targets in sorted(lab_site_targets.items()):
        print(f"{lab:<4} ({LAB_NAMES[lab]})")
        for site, targets in sorted(site_targets.items()):
            print(f"  {site}")
            for target in targets:
                print(f"    {target}")
        print()

    for site, site_rows in df.groupby(level="wwtp_name"):
        site_row = site_rows.iloc[-1]
        print(
            f"=== [{site_row.county_names}] {site} "
            + (
                f"{site_row.sample_location} "
                f"({site_row.sample_location_specify}) "
                if site_row.sample_location != "wwtp"
                else ""
            )
            + (
                f"({site_row.institution_type}: "
                if site_row.institution_type != "not institution specific"
                else "("
            )
            + f"{site_row['FACILITY NAME']}) {site_row.population_served}p "
            f"cap={site_row.capacity_mgd}mgd "
            + (
                f"{site_row.sewage_travel_time:.0f}h "
                if pandas.notna(site_row.sewage_travel_time)
                else ""
            )
            + " ==="
        )

        for (target, lab, gene, units), target_rows in site_rows.groupby(
            level=[
                "pcr_target",
                "lab_id",
                "pcr_gene_target",
                "pcr_target_units",
            ]
        ):
            print(f"{site} {target}({gene}) {lab} ({units})")
            for date, date_rows in target_rows.groupby(
                level="sample_collect_date"
            ):
                for (row_i, row) in enumerate(date_rows.itertuples()):
                    notna_true = lambda value: pandas.notna(value) and value
                    print(
                        f"{'*' if row_i else ' '} "
                        f"{date.strftime('%Y-%m-%d')} ({row.sample_id}):"
                        + (
                            f" {row.pcr_target_avg_conc:3.1f}"
                            if "log10" in units
                            else f" {row.pcr_target_avg_conc / 1e3:5.0f}K"
                        )
                        + (
                            f" {row.flow_rate:5.1f}mgd"
                            f" {row.flow_rate * 1e6 / row.population_served:3.0f}g/p"
                            if pandas.notna(row.flow_rate)
                            else ""
                        )
                        + (f" {row.ph:.1f}pH" if pandas.notna(row.ph) else "")
                        + (
                            f" {row.conductivity:4.0f}μS"
                            if pandas.notna(row.conductivity)
                            else ""
                        )
                        + (
                            f" tss={row.tss:.0f}mg/L"
                            if pandas.notna(row.tss)
                            else ""
                        )
                        + (
                            f" {row.collection_water_temp:.1f}C"
                            if pandas.notna(row.collection_water_temp)
                            else ""
                        )
                        + (
                            f" x{row.equiv_sewage_amt:.1f}"
                            if pandas.notna(row.equiv_sewage_amt)
                            and abs(row.equiv_sewage_amt - 1.0) > 1e-5
                            else ""
                        )
                        + (" NTC-fail" if notna_true(row.ntc_amplify) else "")
                        + (" inh" if notna_true(row.inhibition_detect) else "")
                        + ("-adj" if notna_true(row.inhibition_adjust) else "")
                        + (" !QC" if notna_true(row.qc_ignore) else "")
                        + (" !dash" if notna_true(row.dashboard_ignore) else "")
                        + (" !anal" if notna_true(row.analysis_ignore) else "")
                        + (
                            f" {row.rec_eff_percent:3.0f}%"
                            if pandas.notna(row.rec_eff_percent)
                            and row.rec_eff_percent >= 0
                            else ""
                        )
                        + (
                            f" {abbrev(row.hum_frac_target_mic)}="
                            + (
                                f"{row.hum_frac_mic_conc:.1f}"
                                if "log10" in units
                                else f"{row.hum_frac_mic_conc / 1e6:.0f}M"
                            )
                            if pandas.notna(row.hum_frac_mic_conc)
                            else ""
                        )
                        + (
                            f" {abbrev(row.hum_frac_target_chem)}="
                            f"{row.hum_frac_chem_conc}"
                            if pandas.notna(row.hum_frac_chem_conc)
                            else ""
                        )
                        + (
                            f" {abbrev(row.other_norm_name)}="
                            f"{row.other_norm_conc}"
                            if pandas.notna(row.other_norm_conc)
                            else ""
                        )
                    )

            print()
