import os

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages


HERE = os.path.abspath(os.path.dirname(__file__))


make_abs = lambda fn: os.path.join(HERE, fn)


def parse_requirements():
    """Extract requirements from requirements.txt file.
    """
    path = make_abs('requirements.txt')

    requirements = []

    if not os.path.exists(path):
        return requirements

    with open(path, 'r') as f:
        for dep in f:
            dep = dep.strip()

            # Lines beginning with -- are instructions to pip about how to
            # process the requirements. There are no equivalents in setuptools.
            # An example is `--allow-external some-python-package`.
            if dep.startswith('--'):
                continue

            requirements.append(dep)

    return requirements


setup(
    author='onefinestay',
    author_email='engineering@onefinestay.com',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 2.7",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Topic :: Software Development",
        "Topic :: Utilities",
        "Environment :: Console"
    ],
    description='Build and manage a star schema.',
    include_package_data=True,
    install_requires=parse_requirements(),
    long_description=open(make_abs('README.rst')).read(),
    name='pylytics',
    packages=find_packages(exclude=("test", "test.*")),
    scripts=[
        'pylytics/bin/pylytics-admin.py',
    ],
    url='https://github.com/onefinestay/pylytics',
    version='1.2.2',
    zip_safe=False,
    )
