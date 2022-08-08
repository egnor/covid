"""Basic data structures for COVID-relevant metrics."""

import collections
import dataclasses
import re
from dataclasses import field
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import pandas
import pandas.api.types
import scipy.stats


@dataclasses.dataclass(frozen=True)
class Metric:
    frame: pandas.DataFrame
    color: str
    emphasis: int = 0
    order: float = 0
    increase_color: Optional[str] = None
    decrease_color: Optional[str] = None

    def debug_line(self):
        last_date = self.frame.value.last_valid_index()
        return (
            (
                f"{self.frame.value.count():3d}d =>{last_date.strftime('%Y-%m-%d')}"
                f" last={self.frame.value.loc[last_date]:<5.1f}"
            )
            if self.frame is not None
            else "[None]"
        )

    def debug_block(self, with_data=False):
        if with_data and self.frame is not None:
            return (self.debug_line() + "\n" + str(self.frame)).rstrip()
        else:
            return self.debug_line()


@dataclasses.dataclass(frozen=True)
class PolicyChange:
    date: pandas.Timestamp
    score: int
    emoji: str
    text: str


@dataclasses.dataclass(eq=False)
class Metrics:
    total: collections.Counter = field(default_factory=collections.Counter)
    policy: List[PolicyChange] = field(default_factory=list)
    covid: Dict[str, Metric] = field(default_factory=dict)
    hospital: Dict[str, Metric] = field(default_factory=dict)
    map: Dict[str, Metric] = field(default_factory=dict)
    mobility: Dict[str, Metric] = field(default_factory=dict)
    vaccine: Dict[str, Metric] = field(default_factory=dict)
    variant: Dict[str, Metric] = field(default_factory=dict)
    wastewater: Dict[str, Dict[str, Metric]] = field(default_factory=dict)

    def debug_dict(self):
        out = {}
        for f in dataclasses.fields(self):
            v = getattr(self, f.name)
            if isinstance(v, dict) and v:
                out[f.name] = v
        return out

    def debug_line(self):
        """Returns a one-line summary of the metric."""

        out = f'{self.total["population"] or -1:9.0f}p'
        cats = [k[:3] for k, v in self.debug_dict().items()]
        return out + (f' <{"+".join(cats)}>' if cats else "")

    def debug_block(self, **kwargs):
        """Returns a paragraph description of the metric data."""

        lines = []
        for cat, mdict in self.debug_dict().items():
            for name, m in mdict.items():
                if isinstance(m, Metric):
                    metric_lines = m.debug_block(**kwargs).splitlines()
                    metric_lines[0] += f" {cat[:3]}: {name}"
                    lines.extend(metric_lines)
                elif isinstance(m, dict):
                    for sub, subm in m.items():
                        subm_lines = subm.debug_block(**kwargs).splitlines()
                        subm_lines[0] += f" {cat[:3]}[{name}]: {sub}"
                        lines.extend(subm_lines)

        for c in self.policy:
            line = f"       {c.date.date()} {c.score:+2d} {c.emoji} {c.text}"
            lines.append(line)

        return "\n".join(lines)


@dataclasses.dataclass(eq=False)
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

    def subregion(r, k, name=None):
        """Finds or creates a subregion with a path key and optional name."""

        assert isinstance(k, str)
        sub = r.subregions.get(k)
        if not sub:
            sub = r.subregions[k] = Region(name=name or k, path=r.path + [k])
        return sub

    def debug_path(r):
        """Returns a string like 'US/California/Santa Clara'."""

        return "/".join(r.path)

    def debug_line(r):
        """Returns a one-line summary of the region data."""

        dataclasses.asdict(r.metrics)
        line = f"{r.metrics.debug_line()} {r.debug_path()}"
        return line + (f" ({r.name})" if r.name != r.path[-1] else "")

    def debug_block(r, **kwargs):
        """Returns a paragraph description of the region data."""

        lines = [r.debug_line()]
        for url, name in r.credits.items():
            lines.append(f"    {name} ({url})")
        for line in r.metrics.debug_block(**kwargs).splitlines():
            lines.append(f"    {line}")
        return "\n".join(lines)

    def debug_tree(r, **kwargs):
        """Returns a text description of an entire region subtree."""

        lines = r.debug_block(**kwargs).splitlines()
        if len(lines) > 1 and lines[-1] != "":
            lines.append("")

        for sub in r.subregions.values():
            sub_lines = sub.debug_tree(**kwargs).splitlines()
            for line in sub_lines:
                lines.append(f"  {line}")
            if len(sub_lines) > 1 and sub_lines[-1] != "":
                lines.append("")

        return "\n".join(lines).rstrip()


@dataclasses.dataclass(eq=False)
class RegionAtlas:
    world: Region = None
    by_iso2: Dict[str, Region] = field(default_factory=dict)
    by_jhu_id: Dict[str, Region] = field(default_factory=dict)
    by_fips: Dict[int, Region] = field(default_factory=dict)


def make_metric(c, em, ord, v=None, raw=None, cum=None):
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
        clipped = raw.iloc[first_i:].clip(lower=0.0)
        smooth = clipped.rolling(7, center=True).apply(
            lambda x: scipy.stats.trim_mean(x, 1.0 / 7.0)
        )
        df = pandas.DataFrame({"raw": raw, "value": smooth})
    else:
        raise ValueError(f"No data for metric")

    if not pandas.api.types.is_datetime64_any_dtype(df.index.dtype):
        raise ValueError(f'Bad trend index dtype "{df.index.dtype}"')
    if df.index.duplicated().any():
        dups = df.index.duplicated(keep=False)
        raise ValueError(f"Dup trend dates: {df.index[dups]}")

    return Metric(frame=df, color=c, emphasis=em, order=ord)
