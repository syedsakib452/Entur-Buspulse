# Build stage: compile the Go program
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /uploader .

# Run stage: just the program, no Go toolchain
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /uploader /usr/local/bin/uploader
# Runs as root on purpose: the collectors' files in /data are owned by root,
# and the uploader has to compress and delete them.
ENTRYPOINT ["uploader"]
