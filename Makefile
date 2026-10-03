.PHONY: all fmt vet test race fuzz benchmark examples check clean

all: check

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./...

# The required validation command. -count=1 defeats the test cache so that
# concurrency tests are actually re-run.
race:
	go test -race -count=1 ./...

fuzz:
	go test -fuzz=FuzzSubscribeValidation -fuzztime=30s ./pubsub/

benchmark:
	go test -run '^$$' -bench=. -benchmem ./pubsub/

examples:
	go run ./examples/basic
	go run ./examples/fanout
	go run ./examples/slow-consumer

# What CI runs.
check: vet
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	go test -race -count=1 ./...

clean:
	go clean -testcache
