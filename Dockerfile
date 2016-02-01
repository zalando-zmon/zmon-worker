FROM zalando/python:15.01.03

#making this a cachable point as compile takes forever without -j

RUN apt-get install -y libsnappy-dev libev4 libev-dev

RUN mkdir -p /app/zmon_worker_data
RUN chmod 777 /app/zmon_worker_data
VOLUME /app/zmon_worker_data
WORKDIR /app/zmon_worker_data

ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

ADD ./ /app/

RUN cd /app && python setup.py install

CMD ["zmon-worker", "-c", "/app/config.yaml"]

COPY scm-source.json /
