#!/usr/bin/env python
import sys

import pylytics


def create_project(project_name):
    """
    Create a new pylytics project.
    
    > Get the location of pylytics ... 
    
    """
    # walk ... read ... then write ....
    pass


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
    sys.stdout.write('Creating new project - {} ...\n'.format(project))
    create_project(project)
