"""Utilities for converting to and from camelcase."""

import re


def underscore_to_camelcase(value):
    """hello_world => HelloWorld"""
    
    def camelcase():
        yield str.lower
        while True:
            yield str.capitalize
    
    c = camelcase()
    camelcase =  "".join(c.next()(x) if x else '_' for x in value.split("_"))
    return camelcase[0].capitalize() + camelcase[1:]


def camelcase_to_underscore(value):
    """HelloWorld => hello_world"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', value)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
