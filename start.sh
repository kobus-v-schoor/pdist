#! /bin/bash

SCRIPT="$( cd "$(dirname "$0")" ; pwd -P )"

source "$SCRIPT/venv/bin/activate"
python "$SCRIPT/pdist.py"
