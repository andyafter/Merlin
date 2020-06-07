FROM alpine:latest AS builder
LABEL maintainer="andyafter@gmail.com"

RUN apk --no-cache add ca-certificates gcompat libc6-compat
WORKDIR /root/

CMD ["echo 'yes!'"]