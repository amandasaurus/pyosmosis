#! /usr/bin/env python

from setuptools import setup, find_packages

from pyosmosis import version_string

setup(
    name="pyosmosis",
    version=version_string(),
    author="Rory McCann",
    author_email="rory@technomancy.org",
    packages=['pyosmosis'],
    entry_points = {
        'console_scripts': [
            'pyosmosis = pyosmosis:main',
            ]
        },
    )
