#!/usr/bin/env python3

import argparse
import datetime
import os
import pathlib
import shutil
import subprocess
import tempfile
import toml
import venv


parser = argparse.ArgumentParser()
parser.add_argument('--project_dir', type=pathlib.Path,
                    default=pathlib.Path(__file__).parent)
parser.add_argument('--builds_dir', type=pathlib.Path,
                    default=pathlib.Path.home() / 'venv_builds')

args = parser.parse_args()
project_toml = toml.load(args.project_dir / 'pyproject.toml')
project_name = project_toml['tool']['poetry']['name']
project_version = project_toml['tool']['poetry']['version']

dist_path = args.project_dir / 'dist'
if not dist_path.exists():
    dist_path.mkdir()

wheel_name = f'{project_name}-{project_version}-py3-none-any.whl'
wheel_path = args.project_dir / 'dist' / wheel_name
if wheel_path.exists():
    wheel_path.unlink()

command = ['poetry', 'build', '-f', 'wheel']
print(f'=== {" ".join(str(a) for a in command)}')
subprocess.run(command, check=True, cwd=args.project_dir)

print()
req_path = dist_path / 'requirements.txt'
command = ['poetry', 'export', '-f', 'requirements.txt', '-o', req_path]
print(f'=== {" ".join(str(a) for a in command)}')
subprocess.run(command, check=True, cwd=args.project_dir)

timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
venv_name = f'{project_name}-{timestamp}-{project_version}'
venv_path = args.builds_dir / venv_name
print(f'=== venv: {venv_path}')
venv.create(venv_path, clear=True, symlinks=True, with_pip=True)

venv_environ = dict(os.environ, VIRTUAL_ENV=venv_path)
for pip_args in (['wheel'], ['-r', req_path], [wheel_path]):
    command = [venv_path / 'bin' / 'pip3', '-q', 'install'] + pip_args
    print(f'=== (in venv) pip3 install {" ".join(str(a) for a in pip_args)}')
    subprocess.run(command, check=True, env=venv_environ)

latest_venv_path = args.builds_dir / f'{project_name}-latest'
print(f'=== symlink: {latest_venv_path} => {venv_name}')
temp_path = pathlib.Path(tempfile.mktemp(dir=args.builds_dir))
temp_path.symlink_to(venv_name, target_is_directory=True)
temp_path.replace(latest_venv_path)
