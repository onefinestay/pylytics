#!/usr/bin/env bash

export PYLYTICS_TEST=1
py.test test/integration/declarative/ -s $*
