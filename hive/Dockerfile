FROM apache/hive:4.0.0-beta-1

USER 0

WORKDIR /opt/hive

COPY ./build.sh ./build.sh

COPY ./dependencies.txt ./dependencies.txt

RUN sh ./build.sh

COPY ./hive-site.xml /opt/hive/conf/hive-site.xml

USER hive
