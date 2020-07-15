#!/usr/bin/env python3

import pathlib
import shutil
import subprocess

project_dir = pathlib.Path(__file__).parent
dist_dir = project_dir / 'dist'
shutil.rmtree(dist_dir)

subprocess.run(
    ['poetry', 'build', '-f', 'wheel'],
    check=True, cwd=project_dir)

files = list(pathlib.Path(__file__).parent.glob('**/*.py'))
command = ['autopep8', '--in-place'] + ['--aggressive'] * 2 + files
subprocess.run(command, check=True)
print(f'=== Formatted {len(files)} files -- use git diff to see changes ===')
