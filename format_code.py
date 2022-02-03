#!/usr/bin/env python3

import subprocess

subprocess.run(
    [
        "autoflake",
        "--recursive",
        "--in-place",
        "--remove-all-unused-imports",
        "--remove-duplicate-keys",
        "--remove-unused-variables",
        ".",
    ]
)

subprocess.run(["black", "--line-length", "80", "."], check=True)
subprocess.run(["isort", "--force-single-line-imports", "."], check=True)
