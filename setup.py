#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

setup(
    name='scarface-utils',
    version='1.1.6',
    packages=find_packages(),
    package_data={
        '': ['*.json', '*.xml']
    },
    install_requires=[
        'azure-batch',
        'azure-keyvault',
        'azure-storage-blob',
        'azure-storage-common',
        'bump2version',
        'pydocumentdb',
        'PyYAML',
        'python-gitlab',
        'PyGithub',
    ],
    entry_points={
        'console_scripts': [
            'bump-version = scarface_utils.common.bump_version:main',
        ]
    },
    description='Packaged utils to interact with Azure Services and auto-bump on pipeline',
    author='Massimo Luraschi',
    author_email='massimo.lura@gmail.com',
    url='https://github.com/ScarfaceIII/utils'
)
