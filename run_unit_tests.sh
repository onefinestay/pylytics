#!/usr/bin/env bash

export PYLYTICS_TEST=1
py.test --cov pylytics test/unit/ --cov-report term-missing $*
