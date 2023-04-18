#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-flexopus",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Reza Besharat",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_flexopus"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-flexopus=tap_flexopus:main
    """,
    packages=["tap_flexopus"],
    package_data = {
        "schemas": ["tap_flexopus/schemas/*.json"]
    },
    include_package_data=True,
)
