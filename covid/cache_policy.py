# Common settings and defaults for requests_cache layer.

import argparse
import datetime
import pathlib

import cachecontrol
import cachecontrol.caches.file_cache
import cachecontrol.heuristics
import pandas
import requests


# Reusable command line arguments for log fetching.
argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group('data caching')
argument_group.add_argument(
    '--cache_time', type=pandas.Timedelta,
    default=pandas.Timedelta(hours=1))
argument_group.add_argument(
    '--cache_dir', type=pathlib.Path,
    default=pathlib.Path.home() / 'http_cache')


def new_session(args):
    """Returns a new CachedSession per the supplied command line args."""

    session = requests.Session()
    if args.cache_time:
        adapter = cachecontrol.CacheControlAdapter(
            cache=cachecontrol.caches.file_cache.FileCache(args.cache_dir),
            heuristic=cachecontrol.heuristics.ExpiresAfter(
                seconds=args.cache_time.total_seconds()))
        session.mount('http://', adapter)
        session.mount('https://', adapter)

    return session
