FROM python:3.7-slim-buster
LABEL maintainer="andyafter@gmail.com"

RUN apk --no-cache add ca-certificates gcompat libc6-compat
# adding the source code
RUN mkdir -p /merlin/
WORKDIR /merlin/
ADD . /merlin/