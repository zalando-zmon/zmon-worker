===========
ZMON Worker
===========

.. image:: https://travis-ci.org/zalando-zmon/zmon-worker.svg?branch=master
   :target: https://travis-ci.org/zalando-zmon/zmon-worker
   :alt: Build Status

.. image:: https://codecov.io/gh/zalando-zmon/zmon-worker/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/zalando-zmon/zmon-worker
   :alt: Coverage Status

.. image:: https://img.shields.io/pypi/v/zmon-worker.svg
   :target: https://pypi.python.org/pypi/zmon-worker/
   :alt: Latest PyPI version

.. image:: https://img.shields.io/badge/OpenTracing-enabled-blue.svg
   :target: http://opentracing.io
   :alt: OpenTracing enabled

.. image:: https://img.shields.io/pypi/l/zmon-worker.svg
   :target: https://pypi.python.org/pypi/zmon-worker/
   :alt: License


ZMON's Python worker is doing the heavy lifting of executing tasks against entities, and evaluating all alerts assigned to check.
Tasks are picked up from Redis and the resulting check values plus alert state changes are written back to Redis.

Local Development
=================

Start Redis on localhost:6379:

.. code-block:: bash

    $ docker run -p 6379:6379 -it redis

Install the required development libraries:

Ubuntu/Debian:

.. code-block:: bash

    $ sudo apt-get install build-essential python2.7-dev libpq-dev libldap2-dev libsasl2-dev libsnappy-dev
    $ sudo pip2 install -r requirements.txt

macOS:

.. code-block:: bash

    $ brew install python snappy
    $ sudo pip install -r requirements.txt

Start the ZMON worker process:

.. code-block:: bash

    $ python2 -m zmon_worker_monitor

You can query the worker monitor via the REST API:

.. code-block:: bash

    $ curl http://localhost:8080/status

You can also query the worker monitor via RPC:

.. code-block:: bash

    $ python2 -m zmon_worker_monitor.rpc_client http://localhost:23500/zmon_rpc list_stats

Running Unit Tests
==================

Run tests via `Tox <http://tox.readthedocs.io/en/latest/install.html>`_.

.. code-block:: bash

  $ tox

You can also pass args to ``pytest`` via ``tox``, for instance to run specific test case:

.. code-block:: bash

  $ tox tests/test_kairosdb.py::test_kairosdb_query

Alternative way of running unit tests within Docker:

.. code-block:: bash

    $ export WORKER_IMAGE=registry.opensource.zalan.do/stups/zmon-worker:cd166
    $ docker run -it -u $(id -u) -v $(pwd):/workdir -w /workdir $WORKER_IMAGE python setup.py flake8
    $ docker run -it -u $(id -u) -v $(pwd):/workdir -w /workdir $WORKER_IMAGE python setup.py test

Building the Docker Image
=========================

.. code-block:: bash

    $ docker build -t zmon-worker .
    $ docker run -it zmon-worker

Running the Docker image
========================

The Docker image supports many configuration options via environment variables.
Configuration options are explained in the `ZMON Documentation <http://zmon.readthedocs.org/en/latest/installation/configuration.html#worker>`_.
