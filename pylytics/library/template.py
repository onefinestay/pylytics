""" Provides simple templating for Mondrian XML files.

In the future the XSD file for the Mondrian schema might be used.

"""

from jinja2 import Environment, PackageLoader


def level_type(level_type):
    """ Custom filter for mapping the pylytics dimension type to a Mondrian
    type.
    """
    return ('Numeric' if level_type is int else 'String')


def get_template(mondrian_version=3):
    """ Returns a Jinja template for the Mondrian XML.

    Args:
        mondrian_version:
            Only 3 at the moment (version 4 coming soon).

    """
    if mondrian_version != 3:
        raise ValueError('Only Mondrian version 3 is currently supported.')
    env = Environment(
        loader=PackageLoader(
            'pylytics.conf.mondrian_templates',
            'mondrian_%i' % mondrian_version
            ),
        )
    env.filters['level_type'] = level_type
    return env.get_template('cube.jinja')


class TemplateConstructor(object):

    def __init__(self, table, mondrian_version=3, *args, **kwargs):
        self.table = table
        self.template = get_template(mondrian_version)

    @property
    def rendered(self):
        return self.template.render(table=self.table)
