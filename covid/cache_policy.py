# Common settings and defaults for requests_cache layer.

import argparse
import calendar
import datetime
import pathlib

import cachecontrol
import cachecontrol.caches.file_cache
import cachecontrol.heuristics
import email.utils
import requests


# Reusable command line arguments for log fetching.
argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group('data caching')
argument_group.add_argument(
    '--no_cache', default=False, action='store_true')
argument_group.add_argument(
    '--cache_dir', type=pathlib.Path,
    default=pathlib.Path.home() / 'http_cache')
argument_group.add_argument(
    '--cache_time', type=datetime.timedelta,
    default=datetime.timedelta(hours=1))


class SimpleTimeHeuristic(cachecontrol.heuristics.BaseHeuristic):
    def __init__(self, cache_time):
        self._cache_time = cache_time

    def update_headers(self, response):
        date = email.utils.parsedate(response.headers['date'])
        et = datetime.datetime(*date[:6]) + self._cache_time
        return {
            'expires': email.utils.formatdate(calendar.timegm(et.timetuple())),
            'cache-control': 'public',
        }


def new_session(parsed_args):
    """Returns a new CachedSession per the supplied command line args."""
    return (requests.Session() if parsed_args.no_cache else
            cachecontrol.CacheControl(
                requests.Session(),
                heuristic=SimpleTimeHeuristic(parsed_args.cache_time),
                cache=cachecontrol.caches.file_cache.FileCache(
                    parsed_args.cache_dir)))
