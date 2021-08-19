#!/usr/bin/env python3

import subprocess

subprocess.run(["black", "--line-length", "80", "."], check=True)
subprocess.run(["isort", "--force-single-line-imports", "."], check=True)
