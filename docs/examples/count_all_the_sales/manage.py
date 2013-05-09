#!/usr/bin/env python
"""This is the main entry point for running analytics scripts."""

import os

from pylytics.library.main import main


if __name__ == "__main__":
    os.environ.setdefault('PROJECT_PATH',
                          os.path.dirname(os.path.abspath(__file__)))
    main()
