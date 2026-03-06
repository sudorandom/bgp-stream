// Package main provides the entry point for the BGP Real-Time Map Viewer.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	_ "github.com/silbinarywolf/preferdiscretegpu"
	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
)

type multiFlag []string

func (f *multiFlag) String() string {
	return ""
}

func (f *multiFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

var (
	renderWidth                = flag.Int("width", 3840, "Internal rendering width")
	renderHeight               = flag.Int("height", 2160, "Internal rendering height")
	renderScale                = flag.Float64("scale", 760.0, "Internal rendering scale")
	windowWidth                = flag.Int("window-width", 0, "Initial window width (defaults to render width)")
	windowHeight               = flag.Int("window-height", 0, "Initial window height (defaults to render height)")
	tpsFlag                    = flag.Int("tps", 30, "Ticks per second (engine updates)")
	audioFd                    = flag.Int("audio-fd", -1, "File descriptor to write raw PCM audio data (streaming only)")
	hideWindowControls         = flag.Bool("hide-window-controls", false, "Whether to hide window decorations (title bar, etc.)")
	floating                   = flag.Bool("floating", false, "Whether to keep the window always on top")
	captureInterval            = flag.Duration("capture-interval", 0, "Interval to periodically capture high-quality frames (e.g., 1m, 1h). 0 to disable.")
	captureDir         *string = flag.String("capture-dir", "captures", "Directory to store captured frames")
	minimalUI          *bool   = flag.Bool("minimal-ui", false, "Start with only the map and now-playing panel visible")
	hideUI             *bool   = flag.Bool("hide-ui", false, "Hide all UI elements")
	videoPath          *string = flag.String("video", "", "Path to save recorded video (implies -hide-ui and -tps 30)")
	videoDelay                 = flag.Duration("video-delay", 8*time.Second, "Delay before starting video recording")
	audioDir           *string = flag.String("audio-dir", "", "Directory containing MP3 files for background music")
	mmdbFiles          multiFlag
)

func main() {
	flag.Var(&mmdbFiles, "mmdb", "Path to an additional .mmdb file (can be specified multiple times)")
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if *videoPath != "" {
		*hideUI = true
		*tpsFlag = 30 // Ensure consistent FPS for recording
	}

	engine := initEngine()

	startBackgroundTasks(engine)
	setupSignalHandler(engine)
	runWindowLoop(engine)
}

func initEngine() *bgpengine.Engine {
	engine := bgpengine.NewEngine(*renderWidth, *renderHeight, *renderScale)
	engine.FrameCaptureInterval = *captureInterval
	engine.FrameCaptureDir = *captureDir
	engine.MinimalUI = *minimalUI
	engine.HideUI = *hideUI
	engine.VideoPath = *videoPath
	engine.VideoStartDelay = *videoDelay
	engine.MMDBFiles = mmdbFiles
	engine.AudioDir = *audioDir

	// Initialize video writer if requested
	if engine.VideoPath != "" {
		if err := engine.InitVideoWriter(); err != nil {
			log.Fatalf("Fatal: Failed to initialize video writer: %v", err)
		}
	}

	// Initialize audio if a directory is provided
	engine.InitAudio()

	// If audio-fd is provided, use it for streaming audio
	if *audioFd != -1 {
		log.Printf("Attaching audio to file descriptor: %d", *audioFd)
		engine.SetAudioWriter(os.NewFile(uintptr(*audioFd), "audio-pipe"))
	}

	engine.InitPulseTexture()
	engine.InitFlareTexture()
	engine.InitSquareTexture()
	engine.InitTrendlineTexture()
	return engine
}

func startBackgroundTasks(engine *bgpengine.Engine) {
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
}

func setupSignalHandler(engine *bgpengine.Engine) {
	// Handle graceful shutdown for audio fade-out
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received termination signal...")
		closeEngineResources(engine)
		os.Exit(0)
	}()
}

func closeEngineResources(engine *bgpengine.Engine) {
	engine.Stop()
	if engine.SeenDB != nil {
		if err := engine.SeenDB.Close(); err != nil {
			log.Printf("Error closing SeenDB: %v", err)
		}
	}
	if engine.StateDB != nil {
		if err := engine.StateDB.Close(); err != nil {
			log.Printf("Error closing StateDB: %v", err)
		}
	}
}

func runWindowLoop(engine *bgpengine.Engine) {
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

	closeEngineResources(engine)
}
