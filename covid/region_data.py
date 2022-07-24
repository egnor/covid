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
    rollup: bool = False
    increase_color: Optional[str] = None
    decrease_color: Optional[str] = None


@dataclass(frozen=True)
class PolicyChange:
    date: pandas.Timestamp
    score: int
    emoji: str
    text: str


@dataclass(eq=False)
class Metrics:
    total: collections.Counter = field(default_factory=collections.Counter)
    policy: List[PolicyChange] = field(default_factory=list)

    covid: Dict[str, Metric] = field(default_factory=dict)
    hospital: Dict[str, Metric] = field(default_factory=dict)
    map: Dict[str, Metric] = field(default_factory=dict)
    mobility: Dict[str, Metric] = field(default_factory=dict)
    vaccine: Dict[str, Metric] = field(default_factory=dict)
    variant: Dict[str, Metric] = field(default_factory=dict)
    wastewater: Dict[str, Metric] = field(default_factory=dict)


@dataclass(eq=False)
class Region:
    name: str
    path: List[str]
    iso_code: Optional[str] = None
    fips_code: Optional[int] = None
    place_id: Optional[str] = None
    lat_lon: Optional[Tuple[float, float]] = None
    metrics: Metrics = field(default_factory=Metrics)
    credits: Dict[str, str] = field(default_factory=dict)
    subregions: Dict[str, "Region"] = field(default_factory=dict, repr=False)

    def matches_regex(r, rx):
        rx = rx if hasattr(rx, "fullmatch") else (rx and re.compile(rx, re.I))
        return bool(
            not rx
            or rx.fullmatch(r.name)
            or rx.fullmatch(r.debug_path())
            or rx.fullmatch(r.debug_path().replace(" ", "_"))
        )

    def debug_path(r):
        return "/".join(r.path)

    def debug_line(r):
        return (
            f'{r.metrics.total["population"] or -1:9.0f}p <'
            + "|".join(k[:3] for k, v in r.metrics.items() if v)
            + f"> {r.debug_path()}"
            + (f" ({r.name})" if r.name != r.path[-1] else "")
        )

    def debug_block(r, with_data=False):
        out = r.debug_line()

        for name, url in r.credits.items():
            out += f"\n    cred {name}: {url}"

        for cat, metrics in r.metrics.items():
            for name, m in metrics.items():
                out += (
                    f"\n    {m.frame.value.count():3d}d"
                    f" =>{m.frame.index.max().date()}"
                    f" last={m.frame.value.iloc[-1]:<5.1f} "
                    f" {cat[:3]}: {name}"
                )
                if with_data:
                    out += "\n" + str(m.frame)

        for c in r.metrics.policy:
            out += (
                f"\n           {c.date.date()} {c.score:+2d}"
                f" {c.emoji} {c.text}"
            )

        return out + ("\n" if "\n" in out else "")

    def debug_tree(r, **kwargs):
        return r.debug_block(**kwargs) + "".join(
            "\n  " + sub.debug_tree(**kwargs).replace("\n", "\n  ")
            for sub in r.subregions.values()
        )


@dataclass(eq=False)
class RegionAtlas:
    world: Region = None
    by_iso2: Dict[str, Region] = field(default_factory=dict)
    by_jhu_id: Dict[str, Region] = field(default_factory=dict)
    by_fips: Dict[int, Region] = field(default_factory=dict)


def make_metric(c, em, ord, v=None, raw=None, cum=None, rollup=True):
    """Returns a Metric with data massaged appropriately."""

    assert (v is not None) or (raw is not None) or (cum is not None)

    if cum is not None:
        raw = cum - cum.shift()  # Assume daily data.

    if (v is not None) and (raw is not None):
        assert v.index is raw.index
        df = pandas.DataFrame({"raw": raw, "value": v})
    elif v is not None:
        df = pandas.DataFrame({"value": v})
    elif raw is not None:
        (nonzero_is,) = (raw.values > 0).nonzero()  # Skip first nonzero.
        first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(raw)
        first_i = max(0, min(first_i, len(raw) - 14))
        smooth = raw.iloc[first_i:].clip(lower=0.0).rolling(7).mean()
        df = pandas.DataFrame({"raw": raw, "value": smooth})
    else:
        raise ValueError(f"No data for metric")

    if not pandas.api.types.is_datetime64_any_dtype(df.index.dtype):
        raise ValueError(f'Bad trend index dtype "{df.index.dtype}"')
    if df.index.duplicated().any():
        dups = df.index.duplicated(keep=False)
        raise ValueError(f"Dup trend dates: {df.index[dups]}")

    return Metric(frame=df, color=c, emphasis=em, order=ord, rollup=rollup)
