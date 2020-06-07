FROM python:3.7-slim-buster
LABEL maintainer="andyafter@gmail.com"

RUN apk --no-cache add ca-certificates gcompat libc6-compat
WORKDIR /root/