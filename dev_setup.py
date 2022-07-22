#!/usr/bin/env python3

import os
import signal
from pathlib import Path
from subprocess import check_call, check_output

signal.signal(signal.SIGINT, signal.SIG_DFL)
source_dir = Path(__file__).resolve().parent

print("=== System packages (sudo apt install ...) ===")
apt_install = [
    "python3", "python3-dev", "python3-pip", "python3-venv",
    "libcairo-dev", "libgeos-dev", "libproj-dev",
]
dpkg_query_command = ["dpkg-query", "--show", "--showformat=${Package}\\n"]
installed = set(check_output(dpkg_query_command).decode().split())
if not installed.issuperset(apt_install):
    check_call(["sudo", "apt", "install"] + apt_install)

import venv  # In case it just got installed above.
print()
print(f"=== Python packages (pip install ...) ===")
venv_dir = source_dir / "python_venv"
if not venv_dir.is_dir():
    print(f"Creating {venv_dir}...")
    venv.create(venv_dir, symlinks=True, with_pip=True)

pip_install = [
    "addfips", "autoflake", "beautifulsoup4", "black",
    "cachecontrol[filecache]", "cattrs", "charset-normalizer", "dominate",
    "isort", "matplotlib", "moviepy", "mplcairo", "numpy", "openpyxl",
    "pandas", "pyarrow", "pybind11", "pycountry", "pyreadr", "requests",
    "scipy", "us", "xlrd",
]

if Path("/usr/include/proj_api.h").is_file():
    pip_install.extend(["cartopy==0.19.0.post1", "--no-binary", "cartopy"])
else:
    pip_install.append("cartopy")

check_call(["direnv", "allow", source_dir])
check_call(["direnv", "exec", source_dir, "pip", "install", "wheel"])
check_call(["direnv", "exec", source_dir, "pip", "install"] + pip_install)

print()
print("::: Setup complete :::")
