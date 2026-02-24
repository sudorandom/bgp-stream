# Run tests
test:
	go test ./...

build:
	go build -o bgp-viewer ./cmd/bgp-viewer/main.go

run:
	go run ./cmd/bgp-viewer

# Clean up built binaries
clean:
	rm -f bgp-viewer

# Build the Docker image locally using buildx
docker-build:
	docker buildx build -t bgp-viewer:latest .

# Run the Docker image locally for testing (requires YOUTUBE_STREAM_KEY)
docker-run:
	docker run -it --rm \
		-e YOUTUBE_STREAM_KEY \
		-v $(pwd)/audio:/app/audio \
		-v $(pwd)/data:/app/data \
		bgp-viewer:latest
