FROM apache/superset

WORKDIR /app

USER root

COPY . .
#
RUN sh build.sh

USER superset
