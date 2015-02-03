FROM zalando/python:15.01.03

#making this a cachable point as compile takes forever without -j

RUN mkdir -p /app
WORKDIR /app

ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

ADD src /app/src
ADD web.conf /app/web.conf

CMD ["python", "/app/src/web.py"]
