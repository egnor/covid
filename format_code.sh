#!/bin/sh
# Runs autopep8 on Python files.

cd "$(dirname $0)"
autopep8 --in-place --aggressive --aggressive *.py
echo '=== Formatted -- use git diff to see changes ==='
