#!/usr/bin/env bash

py.test --cov pylytics --cov-report term-missing $*

