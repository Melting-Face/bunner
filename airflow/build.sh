#!/bin/sh

pip install --no-cache-dir -r requirements.txt

airflow db migrate

airflow users create \
  -r Admin \
  -u admin \
  -e admin@example.com \
  -f admin \
  -l user \
  -p admin1234
