# syntax=docker/dockerfile:1
FROM python:3.9
# FROM ubuntu:20.04
# RUN apt-get update && apt-get install -y python3.9 python3.9-dev
RUN pip install --upgrade pip
RUN apt-get update && apt-get -y dist-upgrade
# RUN apt-get update && apt-get -y upgrade
# RUN apk add gcc musl-dev python3-dev libffi-dev openssl-dev
# RUN sudo apt-get update && apt-get install -y libmysqlclient
RUN apt-get -y install mariadb-client
WORKDIR /
COPY . .

RUN pip install -r ../../requirements.txt
CMD ["python", "/jobs/streaming/apacheBeam/runnerMain.py"]