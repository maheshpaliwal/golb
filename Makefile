BIN := bin/golb
PKGS := ./...
CONFIG ?= configs/golb.example.yaml

.PHONY: build test vet fmt bench check run clean

build:
	go build -o $(BIN) ./cmd/golb

test:
	go test -race -count=1 $(PKGS)

vet:
	go vet $(PKGS)

fmt:
	@test -z "$$(gofmt -l cmd internal)" || (gofmt -l cmd internal; echo "run gofmt -w on the files above"; exit 1)

bench:
	go test -run '^$$' -bench . -benchmem ./internal/balancer ./internal/metrics ./internal/proxy

check: build
	$(BIN) -check -config $(CONFIG)

run: build
	$(BIN) -config $(CONFIG)

clean:
	rm -rf bin
