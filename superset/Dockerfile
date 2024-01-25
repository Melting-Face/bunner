FROM apache/superset:2.1.0

WORKDIR /app

USER root

COPY . .
#
RUN sh build.sh

USER superset
