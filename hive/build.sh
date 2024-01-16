#!/bin/sh

apt-get upgrade && apt-get update -y

apt-get install wget -y

while read -r line
do
  wget "https://repo1.maven.org/maven2/$line" -P /opt/hive/lib
done < dependencies.txt
