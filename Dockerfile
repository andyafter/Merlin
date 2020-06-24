FROM python:3.7-slim-buster
LABEL maintainer="andyafter@gmail.com"

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# adding the source code
RUN mkdir -p /merlin/
WORKDIR /merlin/
ADD . /merlin/
