# syntax=docker/dockerfile:1
FROM python:3.9
RUN pip install --upgrade pip
RUN apt-get update && apt-get -y dist-upgrade
RUN apt-get -y install mariadb-client
WORKDIR /
COPY . .
RUN pip install -r requirements.txt
WORKDIR /jobs/streaming/beamRunner/
CMD ["python", "runnerMain.py"]