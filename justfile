# Run tests
test:
	go test ./...
	go test -bench=. -benchmem ./...

lint:
	golangci-lint run ./...

run:
	go run ./cmd/bgp-viewer \
		-capture-interval 1h \
		-capture-dir ./archive \
		-height 2160 \
		-width 3840 \
		-scale 760.0 \
		-hide-window-controls

# Watch BGP updates for a specific prefix
debug-prefix prefix="146.66.28.0/22":
	go run ./cmd/debug-prefix -prefix {{prefix}}
