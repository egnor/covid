"""Common settings and defaults for requests_cache layer."""

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
    default=pathlib.Path.home() / 'covid_cache')


def new_session(args):
    """Returns a new Session with caching per supplied command line args."""

    session = requests.Session()
    if args.cache_time:
        adapter = cachecontrol.CacheControlAdapter(
            cache=cachecontrol.caches.file_cache.FileCache(args.cache_dir),
            heuristic=cachecontrol.heuristics.ExpiresAfter(
                seconds=args.cache_time.total_seconds()))
        session.mount('http://', adapter)
        session.mount('https://', adapter)

    return session


def cached_path(session, url_key):
    adapter = session.get_adapter('https://example.com/')
    if isinstance(adapter, cachecontrol.CacheControlAdapter):
        cache, heuristic = adapter.cache, adapter.heuristic
        if (isinstance(cache, cachecontrol.caches.file_cache.FileCache) and
                isinstance(heuristic, cachecontrol.heuristics.ExpiresAfter)):
            path = pathlib.Path(
                cachecontrol.caches.file_cache.url_to_file_path(
                    url_key, cache))
            if path.exists():
                ft = datetime.datetime.fromtimestamp(path.stat().st_mtime)
                keep_time = datetime.datetime.now() - heuristic.delta
                if ft < keep_time:
                    path.unlink()

            return path

    return None
