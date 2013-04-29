#!/usr/bin/env python
import argparse
import os
from shutil import copytree, ignore_patterns
import sys

import pylytics


def create_project(project_name):
    """Create a new pylytics project."""
    pylytics_root = os.path.dirname(pylytics.__file__)
    template_root = os.path.join(pylytics_root, 'conf', 'project_template')
    copytree(template_root, project_name, ignore=ignore_patterns('*.pyc'))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Create new pylytics projects.")
    parser.add_argument(
        'project',
        help = 'The name of the new project',
        nargs = 1,
        type = str,
        )

    args = parser.parse_args().__dict__
    project = args['project'][0]
    sys.stdout.write('Creating new project - {0} ...\n'.format(project))
    create_project(project)
