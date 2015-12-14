Running and building with Docker
================================

.. image:: https://travis-ci.org/zalando/zmon-worker.svg?branch=master
   :target: https://travis-ci.org/zalando/zmon-worker
   :alt: Build Status

.. image:: https://coveralls.io/repos/zalando/zmon-worker/badge.svg
   :target: https://coveralls.io/r/zalando/zmon-worker
   :alt: Coverage Status

.. code-block:: bash

    $ sudo apt-get install libldap2-dev libsasl2-dev libsnappy-dev
    $ sudo pip2 install -r requirements.txt
    $ python2 -m zmon_worker_monitor

.. code-block:: bash

    $ docker build -t zmon-worker .
    $ docker run -it zmon-worker
