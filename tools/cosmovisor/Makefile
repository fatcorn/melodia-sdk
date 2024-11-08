#!/usr/bin/make -f

all: cosmovisor test install

cosmovisor:
	go build -mod=readonly ./cmd/cosmovisor

test:
	go test -mod=readonly -race ./...

install:
	@echo Installing binary...
	@go install ./...
	@echo Installed successfully

.PHONY: all cosmovisor test
