FROM python:3.6-alpine

RUN apk add --no-cache --virtual .build-deps py3-yaml
