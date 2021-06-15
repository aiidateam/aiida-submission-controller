# -*- coding: utf-8 -*-
"""Define the setup for `aiida-submission-controller`."""
import json
from setuptools import setup, find_packages


def setup_package():
    """Install the `aiida-submission-controller` package."""
    filename_setup_json = 'setup.json'
    filename_description = 'README.md'

    with open(filename_setup_json, 'r') as handle:
        setup_json = json.load(handle)

    with open(filename_description, 'r') as handle:
        description = handle.read()

    setup(include_package_data=True,
          packages=find_packages(),
          long_description=description,
          long_description_content_type='text/markdown',
          **setup_json)


if __name__ == '__main__':
    setup_package()
