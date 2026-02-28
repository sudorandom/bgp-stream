// Package main provides the entry point for the BGP Real-Time Map Viewer.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
)

var (
	renderWidth        = flag.Int("width", 1920, "Internal rendering width")
	renderHeight       = flag.Int("height", 1080, "Internal rendering height")
	renderScale        = flag.Float64("scale", 380.0, "Internal rendering scale")
	windowWidth        = flag.Int("window-width", 0, "Initial window width (defaults to render width)")
	windowHeight       = flag.Int("window-height", 0, "Initial window height (defaults to render height)")
	tpsFlag            = flag.Int("tps", 30, "Ticks per second (engine updates)")
	audioFd            = flag.Int("audio-fd", -1, "File descriptor to write raw PCM audio data (streaming only)")
	hideWindowControls = flag.Bool("hide-window-controls", false, "Whether to hide window decorations (title bar, etc.)")
	floating           = flag.Bool("floating", false, "Whether to keep the window always on top")
	captureInterval    = flag.Duration("capture-interval", 0, "Interval to periodically capture high-quality frames (e.g., 1m, 1h). 0 to disable.")
	captureDir         = flag.String("capture-dir", "captures", "Directory to store captured frames")
	minimalUI          = flag.Bool("minimal-ui", false, "Start with only the map and now-playing panel visible")
)

func main() {
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	engine := bgpengine.NewEngine(*renderWidth, *renderHeight, *renderScale)
	engine.FrameCaptureInterval = *captureInterval
	engine.FrameCaptureDir = *captureDir
	engine.MinimalUI = *minimalUI

	// If audio-fd is provided, use it for streaming audio
	if *audioFd != -1 {
		log.Printf("Attaching audio to file descriptor: %d", *audioFd)
		engine.SetAudioWriter(os.NewFile(uintptr(*audioFd), "audio-pipe"))
	}

	engine.InitPulseTexture()

	// Start all data loading in the background
	go func() {
		// 1. Generate initial map background
		if err := engine.GenerateInitialBackground(); err != nil {
			log.Printf("Warning: Failed to generate background: %v", err)
		}

		// 2. Load the rest of the data
		if err := engine.LoadRemainingData(); err != nil {
			log.Printf("Fatal: failed to load remaining data: %v", err)
			os.Exit(1)
		}

		if engine.GetProcessor() != nil {
			go engine.GetProcessor().Listen()
		}
		if engine.GetAudioPlayer() != nil {
			go engine.GetAudioPlayer().Start()
		}
		go engine.StartBufferLoop()
		go engine.StartMetricsLoop()
	}()

	go engine.StartMemoryWatcher()

	// Handle graceful shutdown for audio fade-out
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received termination signal...")
		if ap := engine.GetAudioPlayer(); ap != nil {
			ap.Shutdown()
		}
		if engine.SeenDB != nil {
			if err := engine.SeenDB.Close(); err != nil {
				log.Printf("Error closing database: %v", err)
			}
		}
		os.Exit(0)
	}()

	ebiten.SetTPS(*tpsFlag)
	ebiten.SetWindowTitle("BGP Real-Time Map Viewer")
	ebiten.SetWindowDecorated(!*hideWindowControls)
	ebiten.SetWindowFloating(*floating)

	w, h := *windowWidth, *windowHeight
	if w == 0 {
		w = *renderWidth
	}
	if h == 0 {
		h = *renderHeight
	}

	ebiten.SetWindowSize(w, h)
	ebiten.SetWindowResizingMode(ebiten.WindowResizingModeEnabled)
	log.Println("Starting ebiten game loop...")
	if err := ebiten.RunGame(engine); err != nil {
		log.Fatal(err)
	}

	if engine.SeenDB != nil {
		if err := engine.SeenDB.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}
}
