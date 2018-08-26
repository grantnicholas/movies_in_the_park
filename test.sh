#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

pip3 install -e "${DIR}"

python3 -m pytest "${DIR}/dags_test/"