#!/usr/bin/env bash

BINDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -L)"
FULLERITE_DIR="$(dirname "${BINDIR}")"
SRC_DIR="${FULLERITE_DIR}/src"
EXAMPLE_CONFIG="${FULLERITE_DIR}/examples/config/fullerite.conf.example"

DIAMOND_DIR="${SRC_DIR}/diamond"
if [ ! -d "${DIAMOND_DIR}" ]; then
    DIAMOND_DIR="/usr/share/fullerite/diamond"
fi
DIAMOND_SERVER="${DIAMOND_DIR}/server.py"


PYTHON="$(which python)"
PYTHONPATH="$(dirname ${DIAMOND_DIR})"
export PYTHONPATH

ARGS="$@"
if [ -z "${ARGS}" ]; then
    ARGS="-c ${EXAMPLE_CONFIG}"
fi

exec $PYTHON ${DIAMOND_SERVER} ${ARGS}
