#!/usr/bin/env bash

export PYLYTICS_TEST=1
py.test --cov pylytics test/unit/library/test_dimension.py --cov-report term-missing $*
