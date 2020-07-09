FROM python:3.7.8-stretch
RUN mkdir -p /merlin
COPY requirements.txt /merlin/requirements.txt
WORKDIR /merlin
RUN pip install -r requirements.txt
COPY artifacts /merlin
ENTRYPOINT /bin/bash
