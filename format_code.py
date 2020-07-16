#!/usr/bin/env python3
# Runs autopep8 on Python files.

import pathlib
import subprocess

files = list(pathlib.Path(__file__).parent.glob('**/*.py'))
command = ['autopep8', '--in-place'] + ['--aggressive'] * 2 + files
subprocess.run(command, check=True)
print(f'=== Formatted {len(files)} files -- use git diff to see changes')
