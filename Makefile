.PHONY: build docker test lint

build:
	go build -o bin/redisbetween .

docker:
	docker-compose up

test:
	go test -count 1 -race ./...

lint:
	GOGC=75 golangci-lint run --timeout 10m --concurrency 32 -v -E golint ./...

ruby-test:
	cd ruby; rake

ruby-setup:
	cd ruby; bin/setup

proxy-test: build
	bin/redisbetween -unlink -network unix -loglevel debug redis://$$REDIS_HOST:7000?readonly=true redis://$$REDIS_HOST:7000?label=cluster redis://$$REDIS_HOST:7006?label=standalone
