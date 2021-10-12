FROM debian:buster as build
RUN apt-get update && apt-get install -y make git gcc
WORKDIR /tmp/install
COPY Makefile* ./
COPY build/install-dep.sh build/install-dep.sh
RUN make install-dep-nonsudo

FROM golang:1.14.7
COPY --from=build /usr/lib/libopensea-* /usr/lib/
