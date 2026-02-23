#!/bin/bash
set -x

# Clean up stale Xvfb lock files if they exist
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99

# Start Xvfb in the background
Xvfb :99 -ac -screen 0 1920x1080x24 > /tmp/xvfb.log 2>&1 &
XVFB_PID=$!

# Ensure Xvfb is cleaned up on exit
trap "kill $XVFB_PID" EXIT

# Wait for Xvfb to be ready
echo "Waiting for Xvfb (PID $XVFB_PID) to start..."
timeout 10s bash -c 'until xset -q > /dev/null 2>&1; do sleep 0.1; done'

if [ $? -ne 0 ]; then
    echo "Xvfb failed to start within 10 seconds"
    cat /tmp/xvfb.log
    exit 1
fi

echo "Xvfb is ready. Starting bgp-streamer..."
export DISPLAY=:99

# Check if DISPLAY is available
if ! xset -q > /dev/null 2>&1; then
    echo "ERROR: DISPLAY :99 not found"
    exit 1
fi

# Run the streamer and make sure stdout/stderr are unbuffered
stdbuf -oL -eL ./bgp-streamer "$@" 2>&1
