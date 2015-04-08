FROM zalando/python:15.01.03

#making this a cachable point as compile takes forever without -j

RUN mkdir -p /app
WORKDIR /app

ADD requirements.txt /app/requirements.txt
ADD test_requirements.txt /app/test_requirements.txt
RUN pip install -r /app/requirements.txt

ADD README.rst /app/README.rst
ADD setup.py /app/setup.py
ADD zmon_worker_monitor /app/zmon_worker_monitor
ADD zmon_worker_monitor/data /app/data
ADD web.conf /app/web.conf

RUN cd /app && python setup.py install

RUN mkdir -p /app/logs

CMD ["python", "/app/zmon_worker_monitor/web.py", "-c", "/app/web.conf"]
