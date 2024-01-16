#!/bin/sh

while read -r line
do
  wget "https://repo1.maven.org/maven2/$line" -P /opt/spark/jars
done < dependencies.txt

mv conf /opt/spark
