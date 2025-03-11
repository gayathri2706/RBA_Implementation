FROM python:3.8

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

WORKDIR /rba_etl

COPY requirement_new.txt .

RUN pip3 install -r requirement_new.txt

COPY . .

RUN mv Config config

RUN ls -la

CMD ["python", "./ETL.py"]