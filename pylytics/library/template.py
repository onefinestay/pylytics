""" Provides simple templating for Mondrian XML files.

In the future the XSD file for the Mondrian schema might be used.

"""

from inspect import getmro
import os

from jinja2 import Template


TEMPLATE_DIR = '../conf/mondrian_templates/'


def get_template(table_type, mondrian_version=3):
    """ Returns a Jinja template for the Mondrian XML.

    Args:
        table_type:
            Either 'cube' or 'dimension'.
        mondrian_version:
            Either 3 or 4.

    """
    path = os.path.join(
        os.path.dirname(__file__),
        TEMPLATE_DIR,
        'mondrian_{}'.format(mondrian_version),
        '{}.jinja'.format(table_type)
        )
    with open(path) as template:
        contents = template.read()
    return Template(contents)


class TemplateConstructor(object):

    def __init__(self, table, mondrian_version=3, *args, **kwargs):
        self.table = table
        base_names = [i.__name__ for i in getmro(table)]
        table_type = 'cube' if 'Fact' in base_names  else 'dimension'
        self.template = get_template(table_type, mondrian_version)

    @property
    def rendered(self):
        return self.template.render(table=self.table)
