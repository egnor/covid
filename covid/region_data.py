"""Basic data structures for COVID-relevant metrics."""

import collections
import re
from dataclasses import dataclass
from dataclasses import field
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import pandas
import pandas.api.types


@dataclass(frozen=True)
class Metric:
    frame: pandas.DataFrame
    color: str
    emphasis: int = 0
    order: float = 0
    increase_color: Optional[str] = None
    decrease_color: Optional[str] = None
    credits: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PolicyChange:
    date: pandas.Timestamp
    score: int
    emoji: str
    text: str
    credits: Dict[str, str]


@dataclass(eq=False)
class Region:
    name: str
    short_name: str
    iso_code: Optional[str] = None
    fips_code: Optional[int] = None
    place_id: Optional[str] = None
    lat_lon: Optional[Tuple[float, float]] = None
    parent: Optional["Region"] = field(default=None, repr=0)
    subregions: Dict[str, "Region"] = field(default_factory=dict, repr=0)
    totals: collections.Counter = field(default_factory=collections.Counter)
    map_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    covid_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    variant_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    vaccine_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    serology_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    mobility_metrics: Dict[str, Metric] = field(default_factory=dict, repr=0)
    policy_changes: List[PolicyChange] = field(default_factory=list, repr=0)

    def path(r):
        return f"{r.parent.path()}/{r.short_name}" if r.parent else r.name

    def matches_regex(r, rx):
        rx = rx if isinstance(rx, re.Pattern) else (rx and re.compile(rx, re.I))
        return bool(
            not rx
            or rx.fullmatch(r.name)
            or rx.fullmatch(r.path())
            or rx.fullmatch(r.path().replace(" ", "_"))
        )

    def debug_line(r):
        return (
            f'{r.totals["population"] or -1:9.0f}p <'
            + ".h"[any("hosp" in k for k in r.covid_metrics.keys())]
            + ".m"[bool(r.map_metrics)]
            + ".c"[bool(r.covid_metrics)]
            + ".v"[bool(r.vaccine_metrics)]
            + ".s"[bool(r.serology_metrics)]
            + ".g"[bool(r.mobility_metrics)]
            + ".p"[bool(r.policy_changes)]
            + f"> {r.path()}"
            + (f" ({r.name})" if r.name != r.short_name else "")
        )

    def debug_block(r, with_credits=False, with_data=False):
        out = r.debug_line()

        for cat, metrics in (
            ("map", r.map_metrics),
            ("cov", r.covid_metrics),
            ("var", r.variant_metrics),
            ("vax", r.vaccine_metrics),
            ("ser", r.serology_metrics),
            ("mob", r.mobility_metrics),
        ):
            for name, m in metrics.items():
                out += (
                    f"\n    {len(m.frame):3d}d =>{m.frame.index.max().date()}"
                    f" last={m.frame.value.iloc[-1]:<5.1f} "
                    f" {cat}: {name}"
                )
                if with_credits:
                    out += f'\n        {" ".join(m.credits.values())}'
                if with_data:
                    out += "\n" + str(m.frame)

        for c in r.policy_changes:
            out += (
                f"\n           {c.date.date()} {c.score:+2d}"
                f" {c.emoji} {c.text}"
            )

        return out + ("\n" if "\n" in out else "")

    def debug_tree(r, **kwargs):
        return r.debug_block() + "".join(
            "\n  " + sub.debug_tree().replace("\n", "\n  ")
            for sub in r.subregions.values()
        )


@dataclass(eq=False)
class Atlas:
    world: Region = None
    by_iso2: Dict[str, Region] = field(default_factory=dict)
    by_jhu_id: Dict[str, Region] = field(default_factory=dict)
    by_fips: Dict[int, Region] = field(default_factory=dict)
