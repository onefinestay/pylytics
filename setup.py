import os
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))
make_abs = lambda fn: os.path.join(here, fn)


def parse_requirements(fn, dependency_links):
    requirements = []

    if not os.path.exists(fn):
        return requirements, dependency_links

    with open(fn, 'r') as f:
        for dep in f:
            dep = dep.strip()
            # need to make github requirements work with
            # setuptools like it would work with `pip -r`
            # URLs will not work, so we transform them to
            # dependency_links and requirements
            if dep.startswith('git+'):
                dependency_links.append(dep)
                _, dep = dep.rsplit('#egg=', 1)
                dep = dep.replace('-', '==', 1)
            requirements.append(dep)

    return requirements, dependency_links


requirements, dependency_links = parse_requirements(
    make_abs('requirements.txt'), [])


setup(
    author='onefinestay',
    author_email='engineering@onefinestay.com',
    classifiers=[
      "Development Status :: 3 - Alpha",
      "Programming Language :: Python",
      "Intended Audience :: Developers",
      "Natural Language :: English",
      "Topic :: Software Development",
      "Topic :: Utilities",
      "Environment :: Console",
    ],
    description='Build and manage a star schema.',
    entry_points={
      'console_scripts': [
          'gonzo = pylytics.manage:run_command'
      ]
    },
    include_package_data=True,
    install_requires=requirements,
    long_description=open(make_abs('README.md')).read(),
    name='pylytics',
    packages=find_packages(exclude=['tests', 'tests.*']),
    url='https://github.com/onefinestay/pylytics',
    version='0.1.0',
    zip_safe=False,
    )
