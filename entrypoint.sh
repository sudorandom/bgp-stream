#!/bin/bash
set -x

# Clean up stale files
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99 /tmp/audio.pipe

# Create a named pipe for audio
mkfifo /tmp/audio.pipe

# Default settings (1080p)
WIDTH=1920
HEIGHT=1080
SCALE=380.0
BITRATE="6000k"
MAXRATE="9000k"

# Check if 4k is requested via environment or arguments
if [[ "$*" == *"4k"* ]] || [ "$QUALITY" == "4k" ]; then
    WIDTH=3840
    HEIGHT=2160
    SCALE=760.0
    BITRATE="18000k"
    MAXRATE="25000k"
fi

# Override with specific environment variables if provided
WIDTH=${RENDER_WIDTH:-$WIDTH}
HEIGHT=${RENDER_HEIGHT:-$HEIGHT}
SCALE=${RENDER_SCALE:-$SCALE}

# Filter out "4k", "1080p", and "-headless" from the arguments
ARGS=()
for arg in "$@"; do
    if [[ "$arg" != "4k" && "$arg" != "1080p" && "$arg" != "-headless" ]]; then
        ARGS+=("$arg")
    fi
done

# Start Xvfb in the background with the determined resolution
Xvfb :99 -ac -screen 0 "${WIDTH}x${HEIGHT}x24" > /tmp/xvfb.log 2>&1 &
XVFB_PID=$!

# Ensure Xvfb and audio pipe are cleaned up on exit
trap "kill $XVFB_PID; rm -f /tmp/audio.pipe" EXIT

# Wait for Xvfb to be ready
echo "Waiting for Xvfb (PID $XVFB_PID) to start..."
timeout 10s bash -c 'until xset -q > /dev/null 2>&1; do sleep 0.1; done'

if [ $? -ne 0 ]; then
    echo "Xvfb failed to start within 10 seconds"
    cat /tmp/xvfb.log
    exit 1
fi

echo "Xvfb is ready. Starting bgp-viewer and FFmpeg..."
export DISPLAY=:99

# Determine video codec (VA-API if available)
VCODEC="libx264"
FFMPEG_GLOBAL_ARGS=""
FFMPEG_OUTPUT_ARGS="-pix_fmt yuv420p"

if [ -c "/dev/dri/renderD128" ]; then
    VCODEC="h264_vaapi"
    FFMPEG_GLOBAL_ARGS="-vaapi_device /dev/dri/renderD128"
    FFMPEG_OUTPUT_ARGS="-vf format=nv12,hwupload"
fi

# Run the viewer in the background.
# We open the pipe for both read and write (3<>) to avoid blocking
# and ensure the pipe stays open.
exec 3<> /tmp/audio.pipe
stdbuf -oL -eL ./bgp-viewer -width "$WIDTH" -height "$HEIGHT" -scale "$SCALE" -audio-fd 3 "${ARGS[@]}" 2>&1 &
STREAMER_PID=$!

# Start FFmpeg for capture
# We use a silence generator (anullsrc) mixed with the pipe audio to ensure 
# that FFmpeg always has an audio stream. This prevents "Preparing stream" hangs.
if [ -n "$YOUTUBE_STREAM_KEY" ]; then
    echo "Starting FFmpeg capture for YouTube stream (${WIDTH}x${HEIGHT})..."
    ffmpeg $FFMPEG_GLOBAL_ARGS \
        -f x11grab -draw_mouse 0 -video_size "${WIDTH}x${HEIGHT}" -framerate 30 -thread_queue_size 1024 -i :99.0 \
        -f s16le -ar 44100 -ac 2 -thread_queue_size 1024 -i /tmp/audio.pipe \
        -f lavfi -i anullsrc=channel_layout=stereo:sample_rate=44100 \
        -filter_complex "[1:a][2:a]amix=inputs=2:duration=longest[a]" \
        -map 0:v -map "[a]" \
        -c:v $VCODEC -b:v $BITRATE -maxrate $MAXRATE -bufsize 30000k -g 60 \
        $FFMPEG_OUTPUT_ARGS \
        -c:a aac -b:a 128k \
        -f flv "rtmp://a.rtmp.youtube.com/live2/$YOUTUBE_STREAM_KEY"
else
    echo "YOUTUBE_STREAM_KEY not set. Running in local mode only."
    wait $STREAMER_PID
fi
