#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

MAIN_PACKAGE = 'zmon_worker_monitor'


class PyTest(TestCommand):

    user_options = [('cov-html=', None, 'Generate junit html report')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.cov = None
        self.pytest_args = ['--cov', MAIN_PACKAGE, '--cov-report', 'term-missing',
                            '--doctest-modules', '-s',
                            '--ignore', 'tests/plugins']
        self.cov_html = False

    def finalize_options(self):
        TestCommand.finalize_options(self)
        if self.cov_html:
            self.pytest_args.extend(['--cov-report', 'html'])

    def run_tests(self):
        try:
            import pytest
        except:
            raise RuntimeError('py.test is not installed, run: pip install pytest')

        # HACK: circumvent strange atexit error with concurrent.futures
        # https://developer.blender.org/T39399
        import concurrent.futures  # noqa

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


def load_req(fn):
    return [r.strip() for r in open(fn).read().splitlines() if r.strip() and not r.strip().startswith('#')]

if __name__ == '__main__':
    # just in case setup.py is launched from elsewhere than the containing directory
    original_dir = os.getcwd()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    try:
        cmdclass = {}
        cmdclass['test'] = PyTest

        setup(
            name="zmon-worker",
            version=__import__('zmon_worker_monitor').__version__,
            description='ZMON Worker Monitor',
            url='https://github.com/zalando/zmon-worker',
            license='Apache License 2.0',
            packages=find_packages(exclude=['tests', 'tests.*']),
            setup_requires=['numpy', 'flake8'],  # workaround for bug in numpy+setuptools: https://github.com/numpy/numpy/issues/2434
            install_requires=load_req('requirements.txt'),
            cmdclass=cmdclass,
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
