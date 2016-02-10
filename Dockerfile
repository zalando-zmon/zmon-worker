FROM zalando/python:15.01.03

#making this a cachable point as compile takes forever without -j

RUN apt-get install -y libsnappy-dev libev4 libev-dev && \
    pip install -U pip setuptools urllib3

ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

ADD ./ /app/

RUN cd /app && python setup.py install

CMD ["zmon-worker", "-c", "/app/config.yaml"]

COPY scm-source.json /
