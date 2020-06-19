#!/usr/bin/env python

from distutils.core import setup

exec(open("reliableredisqueue/version.py").read())

setup(
    name="reliableredisqueue",
    version=__version__,
    author="Jean Vancoppenolle",
    author_email="Vancoppenolle@ferret-go.com",
    packages=[
        "reliableredisqueue",
    ],
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
