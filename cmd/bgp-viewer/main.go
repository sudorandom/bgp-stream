package main

import (
	"log"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
)

func main() {
	// Standard 1080p local view
	width, height, scale := 1920, 1080, 300.0
	engine := bgpengine.NewEngine(width, height, scale)

	engine.InitPulseTexture()
	if err := engine.LoadData(); err != nil {
		log.Fatalf("Failed to initialize engine data: %v", err)
	}

	engine.InitAudio()
	go engine.ListenToBGP()
	go engine.StartAudioPlayer()
	go engine.StartBufferLoop()
	go engine.StartMetricsLoop()

	ebiten.SetWindowSize(1280, 720)
	ebiten.SetWindowTitle("BGP Real-Time Map Viewer")
	ebiten.SetTPS(30)
	if err := ebiten.RunGame(engine); err != nil {
		log.Fatal(err)
	}
}
