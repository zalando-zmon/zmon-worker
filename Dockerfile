FROM zalando/python:15.01.03

#making this a cachable point as compile takes forever without -j

RUN apt-get install -y libsnappy-dev libev4 libev-dev

RUN mkdir -p /app/zmon_worker_data
RUN chmod 777 /app/zmon_worker_data
VOLUME /app/zmon_worker_data
WORKDIR /app/zmon_worker_data

ADD requirements.txt /app/requirements.txt
ADD test_requirements.txt /app/test_requirements.txt
RUN pip install -r /app/requirements.txt

ADD README.rst /app/README.rst
ADD setup.py /app/setup.py
ADD zmon_worker_monitor /app/zmon_worker_monitor
ADD config.yaml /app/config.yaml
ADD app.py /app/app.py

RUN cd /app && python setup.py install

CMD ["python", "/app/app.py"]

COPY scm-source.json /
