.PHONY: build run test vet lint tidy clean

build:
	go build -o bin/processor ./cmd/processor

run:
	go run ./cmd/processor

test:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -rf bin
