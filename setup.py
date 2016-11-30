#!/usr/bin/env python
import re
from os import path as op

from setuptools import setup, find_packages


def _read(fname):
    try:
        return open(op.join(op.dirname(__file__), fname)).read()
    except IOError:
        return ''

install_requires = [
    l for l in _read('requirements.txt').split('\n') if l and not l.startswith('#')]

setup(
    name='watchtower.sentry',
    version="0.1",
    license="MIT",
    description=_read('DESCRIPTION'),
    long_description=_read('README.md'),
    author='Simon Zhang, Alistair King',
    author_email='charthouse-info@caida.org',
    url='http://github.com/caida/watchtower-sentry',
    packages=find_packages(),
    install_requires=install_requires,
    include_package_data=True,
    entry_points={'console_scripts': [
        'watchtower-sentry=watchtower.sentry.app:run',
        'watchtower-scanner=watchtower.sentry.scanner_app:run_scanner'
    ]},
)
