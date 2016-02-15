#!/usr/bin/env bash

BINDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -L)"
FULLERITE_DIR="$(dirname "${BINDIR}")"
FULLERITE="${BINDIR}/fullerite"
EXAMPLE_CONFIG="${FULLERITE_DIR}/examples/config/fullerite.conf.example"

ARGS="$@"
if [ -z "${ARGS}" ]; then
    ARGS="-c ${EXAMPLE_CONFIG}"
fi

exec ${FULLERITE} ${ARGS}
