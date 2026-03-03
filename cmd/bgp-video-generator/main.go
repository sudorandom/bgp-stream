package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
)

var (
	renderWidth  = flag.Int("width", 3840, "Internal rendering width")
	renderHeight = flag.Int("height", 2160, "Internal rendering height")
	renderScale  = flag.Float64("scale", 760.0, "Internal rendering scale")
	fps          = flag.Int("fps", 60, "Frames per second for output video")
	speed        = flag.Float64("speed", 10.0, "Simulation speed multiplier")
	outputFile   = flag.String("output", "output.mp4", "Output video file")
)

type multiFlag []string

func (f *multiFlag) String() string {
	return ""
}

func (f *multiFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

var mmdbFiles multiFlag

func main() {
	flag.Var(&mmdbFiles, "mmdb", "Path to an additional .mmdb file (can be specified multiple times)")
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if flag.NArg() == 0 {
		log.Fatal("Please specify an input MRT file")
	}
	inputFile := flag.Arg(0)

	fmt.Printf("Generating video %s from %s at %dx%d %d fps (%.1fx speed)\n", *outputFile, inputFile, *renderWidth, *renderHeight, *fps, *speed)

	engine := bgpengine.NewEngine(*renderWidth, *renderHeight, *renderScale)
	engine.MinimalUI = true
	engine.AnimationSpeed = *speed
	engine.MMDBFiles = mmdbFiles

	engine.InitPulseTexture()
	engine.InitFlareTexture()
	engine.InitTrendlineTexture()

	// Preload data
	if err := engine.GenerateInitialBackground(); err != nil {
		log.Printf("Warning: Failed to generate background: %v", err)
	}

	if err := engine.LoadRemainingData(); err != nil {
		log.Printf("Fatal: failed to load remaining data: %v", err)
		os.Exit(1)
	}

	vc := &bgpengine.VideoCapture{}
	err := vc.Start(*outputFile, *fps, *renderWidth, *renderHeight)
	if err != nil {
		log.Fatalf("Failed to start video capture: %v", err)
	}
	defer vc.Close()

	mrtProcessor := bgpengine.NewMRTProcessor(engine.GetProcessor(), inputFile, *speed, *fps)

	ctx := context.Background()
	err = mrtProcessor.Process(ctx, engine, vc)
	if err != nil {
		log.Fatalf("Failed to process MRT file: %v", err)
	}

	fmt.Println("Done")
}
