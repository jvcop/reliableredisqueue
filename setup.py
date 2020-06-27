#!/usr/bin/env python

import os

from distutils.core import setup


def get_version(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, rel_path)) as file:
        for line in file:
            if line.startswith("__version__"):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]

    raise RuntimeError("Unable to find version string.")


setup(
    name="reliableredisqueue",
    version=get_version("reliableredisqueue/__init__.py"),
    author="Jean Vancoppenolle",
    author_email="Vancoppenolle@ferret-go.com",
    packages=[
        "reliableredisqueue",
        "reliableredisqueue._scripts",
    ],
    package_data={
        "reliableredisqueue": [
            "_scripts/*.lua",
        ]
    },
    extras_require={
        "test": [
            "fakeredis>=1.2.1,<2.0",
            "lupa>=1.9,<2.0",
            "pytest>=3.0.6,<4.0",
            "pytest-cov>=1.8.1,<2.0",
            "pytest-pep8>=1.0.6,<2.0",
            "pytest-mock>=2.0.0,<3.0",
        ]
    },
    install_requires=[
        "redis>=3.5.2,<4.0",
    ])
