#!/bin/sh

while read -r line
do
  wget "$line" -P /opt/spark/jars
done < dependencies.txt

mv conf /opt/spark
