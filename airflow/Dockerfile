FROM apache/airflow:2.7.3-python3.10

WORKDIR /opt/airflow

COPY requirements.txt .
COPY build.sh .

RUN sh build.sh
