#!/usr/bin/env python3

import subprocess

print("=== autoflake ===")
subprocess.run(
    [
        "autoflake",
        "--exclude=python_venv",
        "--recursive",
        "--in-place",
        "--remove-all-unused-imports",
        "--remove-duplicate-keys",
        "--remove-unused-variables",
        ".",
    ]
)

print("\n=== black ===")
subprocess.run(["black", "--line-length", "80", "."], check=True)

print("\n=== isort ===")
subprocess.run(
    ["isort", "--skip=python_venv", "--force-single-line-imports", "."],
    check=True,
)
