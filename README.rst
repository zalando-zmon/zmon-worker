===========
ZMON Worker
===========

.. image:: https://travis-ci.org/zalando/zmon-worker.svg?branch=master
   :target: https://travis-ci.org/zalando/zmon-worker
   :alt: Build Status

.. image:: https://coveralls.io/repos/zalando/zmon-worker/badge.svg
   :target: https://coveralls.io/r/zalando/zmon-worker
   :alt: Coverage Status

ZMON's Python worker is doing the heavy lifting of executing tasks against entities, and evaluating all alerts assigned to check.
Tasks are picked up from Redis and the resulting check values plus alert state changes are written back to Redis.

Local Development
=================

Start Redis on localhost:6379:

.. code-block:: bash

    $ docker run -p 6379:6379 -it redis

Install the required development libraries:

.. code-block:: bash

    $ sudo apt-get install build-essential python2.7-dev libpq-dev libldap2-dev libsasl2-dev libsnappy-dev
    $ sudo pip2 install -r requirements.txt

Start the ZMON worker process:

.. code-block:: bash

    $ python2 -m zmon_worker_monitor

You can query the worker monitor via RPC:

.. code-block:: bash

    $ python2 -m zmon_worker_monitor.rpc_client http://localhost:23500/zmon_rpc list_stats

Running Unit Tests
==================

.. code-block:: bash

    $ sudo pip2 install -r test_requirements.txt
    $ python2 setup.py test


Building the Docker Image
=========================

.. code-block:: bash

    $ docker build -t zmon-worker .
    $ docker run -it zmon-worker

Running the Docker image
========================

The Docker image supports many configuration options via environment variables.
Configuration options are explained in the `ZMON Documentation <http://zmon.readthedocs.org/en/latest/installation/configuration.html#worker>`_.
