FROM python:3.11-slim

ENV PYTHONENV="/etl"

RUN apt-get update && \
    apt-get install -y \
    sqlite3

WORKDIR /etl
COPY . .

RUN pip install -r requirements.txt
