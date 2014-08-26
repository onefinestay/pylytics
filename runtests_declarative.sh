#!/usr/bin/env bash

export PYLYTICS_TEST=1
py.test --cov pylytics test/unit/test_declarative.py --cov-report term-missing $*

