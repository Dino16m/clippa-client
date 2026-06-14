#!/bin/sh
set -e
echo "Building clippa-client for linux/amd64..."
export GOOS=linux
export GOARCH=amd64
export CGO_ENABLED=1
go build -o ./dist/clippa ./cmd/main.go
echo "Done: ./dist/clippa"
