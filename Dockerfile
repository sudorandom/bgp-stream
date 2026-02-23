# --- Build Stage ---
FROM golang:1.24-bookworm AS builder

# Install build dependencies for Ebitengine on Linux
RUN apt-get update && apt-get install -y \
    libgl1-mesa-dev \
    libxcursor-dev \
    libxrandr-dev \
    libxinerama-dev \
    libxi-dev \
    libxxf86vm-dev \
    libasound2-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN go build -o bgp-streamer ./cmd/bgp-streamer/main.go

# --- Final Stage ---
FROM debian:bookworm-slim

# Install runtime dependencies
RUN sed -i 's/main/main contrib non-free non-free-firmware/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y \
    ffmpeg \
    xvfb \
    x11-xserver-utils \
    libgl1-mesa-dri \
    mesa-va-drivers \
    intel-media-va-driver-non-free \
    vainfo \
    libva-drm2 \
    libx11-6 \
    libxcursor1 \
    libxrandr2 \
    libxinerama1 \
    libxi6 \
    libasound2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bgp-streamer .

# Copy assets and entrypoint
COPY audio/ ./audio/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Add dummy audio device configuration
RUN printf 'pcm.!default {\n    type plug\n    slave.pcm "null"\n}' > /etc/asound.conf

# Set environment variables
ENV DISPLAY=:99
ENV LIBGL_ALWAYS_SOFTWARE=1

# The entrypoint script handles starting Xvfb
ENTRYPOINT ["./entrypoint.sh"]
CMD ["-headless"]
