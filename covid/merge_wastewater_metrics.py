"""Functions to merge wastewater sampling metrics into a RegionAtlas"""

import logging
import re
from warnings import warn

import matplotlib.cm
import numpy

import covid.fetch_biobot_wastewater
import covid.fetch_calsuwers_wastewater
import covid.fetch_scan_wastewater
from covid.region_data import make_metric

STATECITY_FIPS = {
    ("AL", "Bessemer"): 1073,
    ("AL", "Birmingham"): 1073,
    ("AL", "Fultondale"): 1073,
    ("CA", "Carson"): 6037,
    ("CA", "Richmond"): 6013,
    ("CA", "Davis"): 6113,
    ("CA", "Half Moon Bay"): 6081,
    ("CA", "Lompoc"): 6083,
    ("CA", "Los Angeles"): 6037,
    ("CA", "Martinez"): 6013,
    ("CA", "Novato"): 6041,
    ("CA", "Oakland"): 6001,
    ("CA", "Ontario"): 6071,
    ("CA", "Paso Robles"): 6079,
    ("CA", "Petaluma"): 6097,
    ("CA", "San Diego"): 6073,
    ("CA", "San Francisco"): 6075,
    ("CA", "San Mateo"): 6081,
    ("CA", "San Rafael"): 6041,
    ("CA", "Santa Cruz"): 6087,
    ("CA", "Santa Rosa"): 6097,
    ("CO", "Parker"): 8035,
    ("FL", "Orlando"): 12095,
    ("GA", "College Park"): 13121,
    ("GA", "Columbus"): 13215,
    ("GA", "Roswell"): 5049,
    ("ID", "Coeur D Alene"): 16055,
    ("IL", "Glen Ellyn"): 17043,
    ("IN", "Bloomington"): 18105,
    ("KS", "Lawrence"): 20045,
    ("KS", "Salina"): 5125,
    ("KY", "Louisville"): 21111,
    ("MI", "Ann Arbor"): 26161,
    ("MI", "Jackson"): 26075,
    ("NC", "Winston-Salem"): 37067,
    ("NJ", "Newark"): 34013,
    ("PA", "Harrisburg"): 42043,
    ("SC", "Greenville"): 45045,
    ("TX", "Garland"): 48113,
    ("TX", "Sunnyvale"): 48113,
}

SITE_RENAME = {
    re.compile(rx, flags=re.I): sub
    for rx, sub in {
        r"East Bay Municipal Utility District": "EBMUD",
        r"Central Contra Costa Sanitary District": "Central San",
        r"City of San Mateo & Estero M\.I\.D.": "San Mateo City",
        r"City of Santa Cruz WTF - County Influent": "Santa Cruz County",
        r"City of Santa Cruz WTF – City influent": "Santa Cruz City",
        r"Gilroy Santa Clara": "Gilroy",
        r"Hyperion Water Reclamation Facility": "LA City Hyperion",
        r"Joint Water Pollution Control Plant": "LA County JWPCP",
        r"Margaret H Chandler WWRF, San Bernardino": "San Bernardino City",
        r"Regional Water Recycling Plant No.1 (RP-1)": "Inland Empire RP-1",
        r"San Diego EW Blom Point Loma WWTP": "San Diego City",
        r"San Jose Santa Clara": "San Jose",
        r"Silicon Valley": "Redwood City SVCW",
        r"Sunnyvale Santa Clara": "Sunnyvale",
        r"Sewer Authority Mid-Coastside": "Half Moon Bay SAM",
        r"Southeast San Francisco": "SFPUC Southeast",
        r"West County Wastewater District": "West County",
        r"\bcity of ": "",
        r" center\b": "",
        r" control\b": "",
        r" district\b": "",
        r" facility\b": "",
        r" influent\b": "",
        r" main\b": "",
        r" plant\b": "",
        r" primary\b": "",
        r" quality\b": "",
        r" reclamation\b": "",
        r" recovery\b": "",
        r" recycling\b": "",
        r" regional\b": "",
        r" resource\b": "",
        r" rwrf\b": "",
        r" sanitation\b": "",
        r" sanitary\b": "",
        r" sewer\b": "",
        r" treatment\b": "",
        r" water\b": "",
        r" wastewater\b": "",
        r" wtf\b": "",
        r" wwtp\b": "",
    }.items()
}

UNITS_RENAME = {
    re.compile(rx, flags=re.I): sub
    for rx, sub in {
        r"copies": "cp",
        r"L wastewater": "L wet",
        r"g dry sludge": "g dry",
    }.items()
}


def _site_name(name):
    for rx, sub in SITE_RENAME.items():
        name = rx.sub(sub, name)
    return name


def _color(index):
    return matplotlib.cm.tab20b.colors[(4 + 2 * index) % 19]


def add_metrics(session, atlas):
    matplotlib.cm.tab20b.colors

    #
    # SCAN (Stanford and Verily)
    #

    logging.info("Loading and merging SCAN wastewater data...")
    df = covid.fetch_scan_wastewater.get_wastewater(session)
    dups = df.index.duplicated(keep=False)
    for city, state, site, timestamp in df.index[dups]:
        warn(
            "Duplicate SCAN wastewater data: "
            f"{city}, {state} ({site}) {timestamp.strftime('%Y-%m-%d')}"
        )

    df = df[~dups]
    index_cols = ["City", "State_Abbr", "Site_Name"]
    for (city, state, site), rows in df.groupby(
        level=index_cols, sort=False, as_index=False
    ):
        rows.reset_index(index_cols, drop=True, inplace=True)
        fips = STATECITY_FIPS.get((state, city))
        if not fips:
            warn(f"Unknown SCAN wastewater city: {city}, {state}")
            continue

        region = atlas.by_fips.get(fips)
        if not region:
            warn(f"Missing SCAN wastewater FIPS: {fips} ({plant})")
            continue

        region.credits.update(covid.fetch_scan_wastewater.credits())
        ww_metrics = region.metrics.wastewater.setdefault(_site_name(site), {})
        ww_metrics[f"Kcp/g dry (WastewaterSCAN)"] = make_metric(
            c=_color(len(ww_metrics)),
            em=1,
            ord=1.0,
            raw=rows.SC2_S_gc_g_dry_weight * 1e-3,
        )

        ww_metrics[f"Kcp/g dry BA.4/5 (WastewaterSCAN)"] = make_metric(
            c=_color(len(ww_metrics)),
            em=0,
            ord=1.0,
            raw=rows.HV_69_70_Del_gc_g_dry_weight * 1e-3,
        )

    #
    # Cal-SuWers (California Department of Public Health)
    #

    logging.info("Loading and merging Cal-SuWers wastewater data...")
    df = covid.fetch_calsuwers_wastewater.get_wastewater(session)
    for wwtp, wwtp_rows in df.groupby(level="wwtp_name", sort=False):
        wwtp_rows.reset_index("wwtp_name", drop=True, inplace=True)
        wwtp_first = wwtp_rows.iloc[0]

        fips = wwtp_first.county_names.split(",")[0].strip()
        fips = int(covid.fetch_calsuwers_wastewater.FIPS_FIX.get(fips, fips))
        region = atlas.by_fips.get(fips)
        if not region:
            warn(f"Unknown Cal-SuWers wastewater county: {fips}")
            continue

        site = _site_name(wwtp_first["FACILITY NAME"])
        region.credits.update(covid.fetch_calsuwers_wastewater.credits())
        ww_metrics = region.metrics.wastewater.setdefault(site, {})

        series_cols = ["pcr_target", "lab_id", "pcr_target_units"]
        for (target, lab, units), rows in wwtp_rows.groupby(
            level=series_cols, sort=False
        ):
            samples = rows.pcr_target_avg_conc
            if units[:6] == "log10 ":
                samples = numpy.power(10.0, samples)
                units = units[6:]
            for rx, sub in UNITS_RENAME.items():
                units = rx.sub(sub, units)

            if lab == "CAL2":
                samples = 0.01 * samples
                units = units.replace("/", "/c", 1)
            elif lab == "CAL3":
                samples = 0.1 * samples
                units = units.replace("/", "/d", 1)

            lab = covid.fetch_calsuwers_wastewater.LAB_NAMES.get(lab, lab)
            title = f"K{units} ({lab})"
            title = f"{target} {title}" if target != "sars-cov-2" else title
            ww_metrics[title] = make_metric(
                c=_color(len(ww_metrics)),
                em=1,
                ord=1.0,
                raw=samples.groupby("sample_collect_date").mean() * 1e-3,
            )

    #
    # Biobot Analytics
    #

    logging.info("Loading and merging Biobot wastewater data...")
    df = covid.fetch_biobot_wastewater.get_wastewater(session)
    for fips, rows in df.groupby(level="fipscode", sort=False, as_index=False):
        first = rows.iloc[0]
        rows.reset_index("fipscode", drop=True, inplace=True)
        region = atlas.by_fips.get(fips)
        if not region:
            warn(f"Missing Biobot wastewater FIPS: {fips} ({first['name']})")
            continue

        region.credits.update(covid.fetch_biobot_wastewater.credits())
        ww_metrics = region.metrics.wastewater.setdefault("Biobot", {})
        ww_metrics[f"Kcp/L wet"] = make_metric(
            c=_color(len(ww_metrics)),
            em=1,
            ord=1.0,
            v=rows.effective_concentration_rolling_average,
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
