#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# Cleanup pyc and pycache files, pytest does like these cached files getting mounted inside a docker container
find . -type f -name *.pyc -exec rm -r {} \+
find . -type d -name __pycache__ -exec rm -r {} \+

pip3 install -r "${DIR}/requirements-dev.txt"
pip3 install -e "${DIR}/dags"

python3 -m pytest "${DIR}/dags_test/"