# justfile for bgp-stream

# Default recipe: build the binaries
build:
	go build -o bgp-streamer ./cmd/bgp-streamer/main.go
	go build -o bgp-viewer ./cmd/bgp-viewer/main.go

# Run tests
test:
	go test ./...

# Clean up built binaries
clean:
	rm -f bgp-streamer bgp-viewer

# Build the Docker image locally using buildx
docker-build:
	docker buildx build -t bgp-streamer:latest .

# Run the Docker image locally for testing (requires YOUTUBE_STREAM_KEY)
docker-run:
	docker run -it --rm \
		-e YOUTUBE_STREAM_KEY \
		-v $(pwd)/audio:/app/audio \
		-v $(pwd)/data:/app/data \
		bgp-streamer:latest
