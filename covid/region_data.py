# Data types and related utilities for aggregated regional stats.

import collections

import numpy
import pandas


MetricData = collections.namedtuple(
    'MetricData', 'name color importance frame')

DayData = collections.namedtuple(
    'PolicyData', 'date significance emojis events')

RegionData = collections.namedtuple(
    'RegionData', 'id name population metrics days date attribution')


def name_per_capita(name, capita):
    def number(n):
        return numpy.format_float_positional(n, precision=3, trim='-')
    return (
        f'{name} / {number(capita / 1000000)}Mp' if capita >= 1000000 else
        f'{name} / {number(capita / 1000)}Kp' if capita >= 10000 else
        f'{name} / {number(capita)}p')


def trend(name, color, importance, date, raw, capita, population):
    nonzero_is, = (raw.values > 0).nonzero()
    first_i = nonzero_is[0] + 1 if len(nonzero_is) else len(raw)
    date = date[first_i:]
    per_cap = raw[first_i:] * capita / population

    f = pandas.DataFrame(dict(
        date=date, raw=per_cap, value=per_cap.rolling(7).mean()))
    f.set_index('date', inplace=True)
    return MetricData(name_per_capita(name, capita), color, importance, f)


def threshold(name, color, importance, value, capita, population):
    f = pandas.DataFrame(dict(
        value=[value * capita / population] * 2,
        date=[pandas.to_datetime('2020-01-01'),
              pandas.to_datetime('2020-12-31')]))
    f.set_index('date', inplace=True)
    return MetricData(name_per_capita(name, capita), color, importance, f)
