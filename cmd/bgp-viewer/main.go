package main

import (
	"flag"
	"log"
	"os"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
)

var (
	headlessFlag = flag.Bool("headless", false, "Run without a local window (Xvfb rendering active)")
	renderWidth  = flag.Int("width", 1920, "Internal rendering width")
	renderHeight = flag.Int("height", 1080, "Internal rendering height")
	renderScale  = flag.Float64("scale", 300.0, "Internal rendering scale")
	windowWidth  = flag.Int("window-width", 1280, "Initial window width (non-headless only)")
	windowHeight = flag.Int("window-height", 720, "Initial window height (non-headless only)")
	tpsFlag      = flag.Int("tps", 30, "Ticks per second (engine updates)")
	audioFd      = flag.Int("audio-fd", -1, "File descriptor to write raw PCM audio data (streaming only)")
)

func main() {
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	engine := bgpengine.NewEngine(*renderWidth, *renderHeight, *renderScale)

	// If audio-fd is provided, use it for streaming audio
	if *audioFd != -1 {
		log.Printf("Attaching audio to file descriptor: %d", *audioFd)
		engine.AudioWriter = os.NewFile(uintptr(*audioFd), "audio-pipe")
	}

	engine.InitPulseTexture()
	if err := engine.LoadData(); err != nil {
		log.Fatalf("Failed to initialize engine data: %v", err)
	}

	go engine.ListenToBGP()
	go engine.StartAudioPlayer()
	go engine.StartBufferLoop()
	go engine.StartMetricsLoop()
	go engine.StartMemoryWatcher()

	ebiten.SetTPS(*tpsFlag)
	if *headlessFlag {
		log.Println("Running in HEADLESS mode (Rendering active).")
		if err := ebiten.RunGame(engine); err != nil {
			log.Fatal(err)
		}
	} else {
		ebiten.SetWindowSize(*windowWidth, *windowHeight)
		ebiten.SetWindowTitle("BGP Real-Time Map Viewer")
		if err := ebiten.RunGame(engine); err != nil {
			log.Fatal(err)
		}
	}
}
