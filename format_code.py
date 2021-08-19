#!/usr/bin/env python3

import subprocess

subprocess.run(["black", "-l", "80", "."], check=True)
