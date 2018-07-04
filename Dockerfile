FROM python:3.6

RUN apt-get update && apt-get install -y \
  python-virtualenv \
  python3-pip

RUN pip3 install --upgrade pip
RUN pip3 install --upgrade virtualenvwrapper
RUN pip3 install PyYAML
