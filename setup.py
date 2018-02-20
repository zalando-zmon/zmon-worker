#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup, find_packages


def load_req(fn):
    return [r.strip() for r in open(fn).read().splitlines() if r.strip() and not r.strip().startswith('#')]


if __name__ == '__main__':
    # just in case setup.py is launched from elsewhere than the containing directory
    original_dir = os.getcwd()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    try:
        setup(
            name='zmon-worker',
            version=__import__('zmon_worker_monitor').__version__,
            description='ZMON Worker Monitor',
            url='https://github.com/zalando/zmon-worker',
            license='Apache License 2.0',
            packages=find_packages(exclude=['tests', 'tests.*']),
            # workaround for bug in numpy+setuptools: https://github.com/numpy/numpy/issues/2434
            setup_requires=['numpy', 'flake8', 'pytest-runner'],
            install_requires=load_req('requirements.txt'),
            dependency_links=['git+https://github.com/zalando-zmon/opentracing-utils.git#egg=opentracing_utils'],
            test_suite='tests',
            tests_require=load_req('test_requirements.txt'),

            entry_points={
                'console_scripts': [
                    'zmon-worker = zmon_worker_monitor.main:main',
                ]
            },
            include_package_data=True,  # needed to include templates (see MANIFEST.in)

            # more metadata for upload to PyPI
            author='Henning Jacobs',
            author_email='henning.jacobs@zalando.de',
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
