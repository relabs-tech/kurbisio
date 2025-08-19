FROM golang:1.24
ENV CGO_ENABLED=1

WORKDIR /app

COPY ./reaper.yaml ./reaper.yaml

RUN apt-get update && apt-get install -y git bash tzdata ca-certificates gcc
RUN update-ca-certificates

RUN go install github.com/obsidiandynamics/goharvest/cmd/reaper@latest

ENV PATH ${GOPATH}/bin:/usr/local/go/bin:$PATH

ENTRYPOINT ["reaper", "-f", "./reaper.yaml"]