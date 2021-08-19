"""Common settings and defaults for requests_cache layer."""

import argparse
import contextlib
import datetime
import email.utils
import logging
import pathlib

import cachecontrol
import cachecontrol.caches.file_cache
import cachecontrol.heuristics
import pandas
import requests


# Reusable command line arguments for log fetching.
argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group("data caching")
argument_group.add_argument("--debug_http", action="store_true")
argument_group.add_argument(
    "--cache_time", type=pandas.Timedelta, default=pandas.Timedelta(hours=6)
)
argument_group.add_argument(
    "--cache_dir",
    type=pathlib.Path,
    default=pathlib.Path.home() / "covid_cache",
)

logger = logging.getLogger("covid.cache_policy")


class _CacheHeuristic(cachecontrol.heuristics.BaseHeuristic):
    def __init__(self, timedelta):
        self.timedelta = timedelta

    def update_headers(self, response):
        vary = response.headers.get("vary")
        vary = (None if vary == "*" else vary) or ""
        exp_time = datetime.datetime.now() + self.timedelta
        exp = email.utils.formatdate(exp_time.timestamp(), usegmt=True)
        return {"vary": vary, "expires": exp, "cache-control": "public"}

    def warning(self, response):
        return f"110 - Automatically cached for {self.timedelta}."


def new_session(args):
    """Returns a new Session with caching per supplied command line args."""

    if args.debug_http:
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("urllib3").setLevel(logging.DEBUG)
        logging.getLogger("cachecontrol").setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    session = requests.Session()
    if args.cache_time:
        logger.debug(f"Caching results for {args.cache_time}.")
        args.cache_dir.mkdir(exist_ok=True)
        adapter = cachecontrol.CacheControlAdapter(
            cache=cachecontrol.caches.file_cache.FileCache(args.cache_dir),
            heuristic=_CacheHeuristic(args.cache_time),
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

    return session


def cached_path(session, url):
    adapter = session.get_adapter(url)
    if isinstance(adapter, cachecontrol.CacheControlAdapter):
        cache, heuristic = adapter.cache, adapter.heuristic
        if isinstance(
            cache, cachecontrol.caches.file_cache.FileCache
        ) and isinstance(heuristic, _CacheHeuristic):
            cutoff = heuristic.timedelta
            name = cachecontrol.caches.file_cache.url_to_file_path(url, cache)
            path = pathlib.Path(name)
            log = f"\n  {url}\n  {path}"
            if path.exists():
                ts = datetime.datetime.fromtimestamp(path.stat().st_mtime)
                age = datetime.datetime.now() - ts
                if age > cutoff:
                    logger.debug(f"Purge cached data ({age} > {cutoff}):{log}")
                    path.unlink()
                else:
                    logger.debug(f"Keep cached data ({age} <= {cutoff}):{log}")
            else:
                logger.debug(f"No cached data:{log}")
            return path

    return None


@contextlib.contextmanager
def temp_to_rename(path, mode=None):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / ("tmp." + path.name)
    try:
        if mode:
            with temp_path.open(mode=mode) as file:
                yield file
        else:
            yield temp_path
        temp_path.rename(path)
    finally:
        temp_path.unlink(missing_ok=True)
