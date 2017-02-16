FROM python:3.6

RUN apt-get update && apt-get install -y python3-pip

RUN pip3 install PyYAML
