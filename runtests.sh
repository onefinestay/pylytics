#!/usr/bin/env bash

export PYLYTICS_TEST=1
py.test --cov pylytics --cov-report term-missing $*
