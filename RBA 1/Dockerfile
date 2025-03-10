FROM python:3.8

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

WORKDIR /rba_etl

COPY requirements_new.txt .

RUN pip3 install -r requirements_new.txt

COPY . .

CMD [“python”, “./ETL.py”]



