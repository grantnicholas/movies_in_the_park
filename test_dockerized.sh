#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker run -v "${DIR}":/mnt python:3.6.5 bash -c "./mnt/test.sh"