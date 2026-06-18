.PHONY: build run test tidy clean fmt vet

BINARY_NAME=cache-proxy
CMD_PATH=./cmd/server

build:
	go build -o $(BINARY_NAME) $(CMD_PATH)

build-linux:
	GOOS=linux GOARCH=amd64 go build -o $(BINARY_NAME)-linux $(CMD_PATH)

build-windows:
	GOOS=windows GOARCH=amd64 go build -o $(BINARY_NAME).exe $(CMD_PATH)

run:
	go run $(CMD_PATH)

run-config:
	go run $(CMD_PATH) configs/config.yaml

test:
	go test -v ./...

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

tidy:
	go mod tidy

fmt:
	go fmt ./...

vet:
	go vet ./...

clean:
	rm -f $(BINARY_NAME) $(BINARY_NAME).exe $(BINARY_NAME)-linux
	rm -f coverage.out
	rm -f *.log

deps:
	go mod download

all: fmt vet build
