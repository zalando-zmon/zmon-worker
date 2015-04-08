#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup, find_packages


def load_req(fn):
    return [r.strip() for r in open(fn).read().splitlines() if r.strip() and not r.strip().startswith('#')]


# just in case setup.py is launched from elsewhere than the containing directory
original_dir = os.getcwd()
os.chdir(os.path.dirname(os.path.abspath(__file__)))


try:

    setup(
        name="Zmon-Worker",
        version=__import__('zmon_worker_monitor').__version__,
        description='Zmon Worker Monitor',
        url='https://github.com/zalando/zmon-worker',
        license='Apache License 2.0',

        packages=find_packages(exclude=['tests', 'tests.*']),
        setup_requires=["numpy"],  # workaround for bug in numpy+setuptools: https://github.com/numpy/numpy/issues/2434
        install_requires=load_req('requirements.txt'),
        test_suite='tests',
        tests_require=load_req('test_requirements.txt'),

        entry_points={
            'console_scripts': [
                'zmon-worker = zmon_worker_monitor.web:main',
            ]
        },

        package_data={
            'zmon_worker_monitor': ['data/*', 'builtins/plugins/*.worker_plugin'],

        },

        # more metadata for upload to PyPI
        author="Henning Jacobs",
        author_email="henning.jacobs@zalando.de",
        keywords='zalando zmon zmon2 worker component monitoring infrastructure',
        long_description=open('README.rst').read(),
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Topic :: Software Development :: Libraries :: Python Modules'],

        platforms='All',
    )

finally:
    os.chdir(original_dir)
