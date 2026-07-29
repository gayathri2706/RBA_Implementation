FROM python:3.8

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV PYTHONUNBUFFERED=1

WORKDIR /rba_etl

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .

RUN mv Config config

RUN ls -la

CMD ["python", "./ETL.py"]
