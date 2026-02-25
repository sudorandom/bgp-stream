package main

import (
	"flag"
	"log"
	"os"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
)

var (
	renderWidth  = flag.Int("width", 1920, "Internal rendering width")
	renderHeight = flag.Int("height", 1080, "Internal rendering height")
	renderScale  = flag.Float64("scale", 380.0, "Internal rendering scale")
	windowWidth  = flag.Int("window-width", 0, "Initial window width (defaults to render width)")
	windowHeight = flag.Int("window-height", 0, "Initial window height (defaults to render height)")
	tpsFlag      = flag.Int("tps", 30, "Ticks per second (engine updates)")
	audioFd      = flag.Int("audio-fd", -1, "File descriptor to write raw PCM audio data (streaming only)")
	hideWindowControls = flag.Bool("hide-window-controls", false, "Whether to hide window decorations (title bar, etc.)")
	floating     = flag.Bool("floating", false, "Whether to keep the window always on top")
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
	if err := ebiten.RunGame(engine); err != nil {
		log.Fatal(err)
	}

	if engine.SeenDB != nil {
		engine.SeenDB.Close()
	}
}
