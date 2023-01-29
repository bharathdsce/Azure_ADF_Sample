# syntax=docker/dockerfile:1
FROM python:3.9
RUN pip install --upgrade pip
WORKDIR /
COPY . .
RUN pip install -r requirements.txt
WORKDIR /jobs/streaming/twitterFeedToPubSub/
CMD ["python", "twitterFeedsToPubsub.py"]