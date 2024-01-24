FROM apache/superset:latest-py310

WORKDIR /app

USER root

COPY . .
#
RUN sh build.sh

USER superset
