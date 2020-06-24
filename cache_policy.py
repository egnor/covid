#!/usr/bin/env python3
# Common settings and defaults for requests_cache layer.

import argparse
import requests_cache


# Reusable command line arguments for log fetching.
argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group('data caching')
argument_group.add_argument('--cache_name', default='http_cache')
argument_group.add_argument('--cache_backend', default='sqlite')
argument_group.add_argument('--cache_seconds', type=float, default=3600.0)


def new_session(parsed_args):
    """Returns a new CachedSession per the supplied command line args."""
    return requests_cache.core.CachedSession(
        cache_name=parsed_args.cache_name,
        backend=parsed_args.cache_backend,
        expire_after=parsed_args.cache_seconds)
