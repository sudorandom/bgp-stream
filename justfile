# Run tests
test:
	go test ./...
	go test -bench=. -benchmem ./...

lint:
	golangci-lint run ./...

run:
	go run ./cmd/bgp-viewer \
		-capture-interval 1h \
		-capture-dir ./archive

record-video:
	go run ./cmd/bgp-viewer -hide-ui -video archive/screen-recording.mov

# Watch BGP updates for a specific prefix
debug-prefix prefix="146.66.28.0/22":
	go run ./cmd/bgp-cli debug-prefix --prefix {{prefix}}

# Fetch and process all required geolocation data
fetch-data:
	go run ./cmd/bgp-cli fetch

# Force a re-download of all source data files
refresh-data:
	go run ./cmd/bgp-cli fetch --fresh

# Mixes the end of an MP3 with a MOV file. Usage: just mix-video vid.mov aud.mp3
mix-video vid aud:
    #!/usr/bin/env bash
    set -euo pipefail

    if [ ! -f "{{vid}}" ] || [ ! -f "{{aud}}" ]; then
        echo "Error: One or both files not found."
        exit 1
    fi

    # Grab exact durations in seconds
    vid_len=$(ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{{vid}}")
    aud_len=$(ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{{aud}}")

    # Calculate start time (audio length minus video length) using awk
    start_time=$(awk -v a="$aud_len" -v v="$vid_len" 'BEGIN { print (a >= v ? a - v : "error") }')

    if [ "$start_time" == "error" ]; then
        echo "Error: The MP3 is shorter than the video."
        exit 1
    fi

    out_name="mixed_$(basename "{{vid}}")"

    # Combine them
    ffmpeg -y -i "{{vid}}" -ss "$start_time" -i "{{aud}}" -c:v copy -c:a aac -map 0:v:0 -map 1:a:0 -shortest "$out_name"

# Extracts a portion of a MOV to a scaled animated WEBP. Usage: just extract-webp vid.mov 33 53
extract-webp vid start end:
    #!/usr/bin/env bash
    set -euo pipefail

    if [ ! -f "{{vid}}" ]; then
        echo "Error: File '{{vid}}' not found."
        exit 1
    fi

    input="{{vid}}"
    out_name="${input%.*}_snippet.webp"

    # Added a video filter (-vf) to drop the framerate and scale the width down
    ffmpeg -y -ss "{{start}}" -to "{{end}}" -i "$input" -vf "fps=15,scale=800:-1:flags=lanczos" -vcodec libwebp -loop 0 -an -q:v 80 -compression_level 6 "$out_name"
    
    echo "Success: Created $out_name"
