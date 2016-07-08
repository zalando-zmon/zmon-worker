#!/bin/bash
export WORKER_IMAGE=registry.opensource.zalan.do/stups/zmon-worker:cd166
docker run -it -u $(id -u) -v $(pwd):/workdir -w /workdir $WORKER_IMAGE python setup.py test
