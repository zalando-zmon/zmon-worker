FROM registry.opensource.zalan.do/stups/ubuntu:latest

#making this a cachable point as compile takes forever without -j

RUN apt-get update && apt-get -y install python-pip python-dev libev4 libev-dev python-psycopg2 libpq-dev libldap2-dev libsasl2-dev libssl-dev libsnappy-dev iputils-ping freetds-dev git && \
    pip2 install -U pip setuptools urllib3 Cython

# make requests library use the Debian CA bundle (includes Zalando CA)
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

RUN pip2 install -U -e git+https://github.com/zalando-zmon/opentracing-utils.git#egg=opentracing-utils

ADD requirements.txt /app/requirements.txt

RUN pip2 install --upgrade -r /app/requirements.txt

ADD ./ /app/

RUN cd /app && python2 setup.py install

COPY zmon_worker_extras/ /app/zmon_worker_extras

ENV ZMON_PLUGINS "$ZMON_PLUGINS:/app/zmon_worker_extras/check_plugins"

CMD ["zmon-worker", "-c", "/app/config.yaml"]
