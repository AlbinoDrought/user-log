FROM golang:1.19-alpine as builder
WORKDIR /app
COPY . /app
RUN apk add --no-cache build-base && go get && go test && go build -o /discord-user-log

FROM alpine:3.14

RUN apk add --update --no-cache tini ca-certificates
USER 1000
COPY --from=builder /discord-user-log /discord-user-log
ENTRYPOINT ["tini", "--"]
CMD ["/discord-user-log"]
