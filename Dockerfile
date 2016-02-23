FROM registry.opensource.zalan.do/stups/ubuntu:15.10-18

#making this a cachable point as compile takes forever without -j

RUN apt-get update && apt-get -y install python-pip python-dev libev4 libev-dev python-psycopg2 libpq-dev libldap2-dev libsasl2-dev libssl-dev libsnappy-dev && \
    pip2 install -U pip setuptools urllib3

ADD requirements.txt /app/requirements.txt
RUN pip2 install -r /app/requirements.txt

ADD ./ /app/

RUN cd /app && python2 setup.py install

CMD ["zmon-worker", "-c", "/app/config.yaml"]

COPY scm-source.json /
