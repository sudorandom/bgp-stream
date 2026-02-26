# Run tests
test:
	go test ./...

run:
	go run ./cmd/bgp-viewer -capture-interval 1h -capture-dir ./archive

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
