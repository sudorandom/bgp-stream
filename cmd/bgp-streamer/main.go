package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
)

var (
	qualityFlag     = flag.String("quality", "1080p", "Stream quality: 1080p or 4k")
	headlessFlag    = flag.Bool("headless", false, "Run without a local window (more stable for 24/7 streams)")
	outputFlag      = flag.String("output", "", "Output destination (file path or RTMP URL). Overrides YouTube stream key.")
	softwareFlag    = flag.Bool("software", false, "Force software encoding (libx264) even if hardware acceleration is available")
	deviceFlag      = flag.String("device", "/dev/dri/renderD128", "VA-API render device path (Linux only)")
	vaapiDriverFlag = flag.String("vaapi-driver", "", "Force a specific VA-API driver (e.g., iHD, i965, radeonsi)")
	debugFlag       = flag.Bool("debug", false, "Enable verbose logging for debugging")
	streamKey       = os.Getenv("YOUTUBE_STREAM_KEY")
	ffmpegStdin     *os.File
	pixelBuffer     []byte
)

func main() {
	flag.Parse()
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	width, height, scale := 1920, 1080, 300.0
	bitrate, maxBitrate := "9000k", "15000k"

	if *qualityFlag == "4k" {
		width, height, scale = 3840, 2160, 600.0
		bitrate, maxBitrate = "18000k", "25000k"
	}

	engine := bgpengine.NewEngine(width, height, scale)
	pixelBuffer = make([]byte, width*height*4)

	// Use a pool of buffers to avoid constant allocations
	bufferPool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, width*height*4)
		},
	}

	// Configure Stream Output with a non-blocking buffer
	frameChan := make(chan []byte, 2)
	engine.OnFrame = func(screen *ebiten.Image) {
		if ffmpegStdin != nil {
			buf := bufferPool.Get().([]byte)
			screen.ReadPixels(buf)
			select {
			case frameChan <- buf:
			default:
				// Skip frame if FFmpeg is falling behind and return buffer to pool
				bufferPool.Put(buf)
			}
		}
	}

	go func() {
		for buf := range frameChan {
			if ffmpegStdin != nil {
				ffmpegStdin.Write(buf)
			}
			bufferPool.Put(buf)
		}
	}()

	engine.InitPulseTexture()
	if err := engine.LoadData(); err != nil {
		log.Fatalf("Failed to initialize engine data: %v", err)
	}

	initFFmpeg(engine, width, height, bitrate, maxBitrate)

	go engine.ListenToBGP()
	go engine.StartAudioPlayer()
	go engine.StartBufferLoop()
	go engine.StartMetricsLoop()
	go engine.StartMemoryWatcher()

	if *headlessFlag {
		log.Println("Running in HEADLESS mode (Rendering active).")
		if err := ebiten.RunGame(engine); err != nil {
			log.Fatal(err)
		}
	} else {
		ebiten.SetWindowSize(1280, 720)
		ebiten.SetWindowTitle("BGP Streamer (YouTube LIVE)")
		ebiten.SetTPS(30)
		if err := ebiten.RunGame(engine); err != nil {
			log.Fatal(err)
		}
	}
}

func initFFmpeg(engine *bgpengine.Engine, width, height int, bitrate, maxBitrate string) {
	vcodec := "libx264"
	var globalHWArgs []string
	var outputHWArgs []string

	if !*softwareFlag {
		switch runtime.GOOS {
		case "darwin":
			vcodec = "h264_videotoolbox"
			outputHWArgs = []string{"-realtime", "true", "-q:v", "65", "-color_range", "1"}
		case "linux":
			if _, err := os.Stat(*deviceFlag); err == nil {
				// Check for read/write permissions
				f, err := os.OpenFile(*deviceFlag, os.O_RDWR, 0)
				if err != nil {
					log.Printf("WARNING: Device %s exists but cannot be opened for RW: %v. Using software encoding.", *deviceFlag, err)
				} else {
					f.Close()
					vcodec = "h264_vaapi"
					globalHWArgs = []string{"-vaapi_device", *deviceFlag}
					outputHWArgs = []string{
						"-vf", "format=nv12,hwupload",
						"-color_range", "1",
					}
				}
			} else if *debugFlag {
				log.Printf("DEBUG: Render device %s NOT found.", *deviceFlag)
			}
		}
	}

	var ffmpegArgs []string
	if *debugFlag {
		ffmpegArgs = append(ffmpegArgs, "-loglevel", "debug")
	}

	ffmpegArgs = append(ffmpegArgs, globalHWArgs...)
	ffmpegArgs = append(ffmpegArgs,
		"-thread_queue_size", "1024",
		"-f", "rawvideo", "-pixel_format", "rgba", "-video_size", fmt.Sprintf("%dx%d", width, height),
		"-framerate", "30", "-i", "pipe:0",
		"-f", "s16le", "-ar", "44100", "-ac", "2", "-i", "pipe:3",
	)
	ffmpegArgs = append(ffmpegArgs,
		"-c:v", vcodec,
		"-b:v", bitrate,
		"-maxrate", maxBitrate,
		"-bufsize", "30000k",
		"-g", "60",
	)

	if vcodec != "h264_vaapi" {
		ffmpegArgs = append(ffmpegArgs, "-pix_fmt", "yuv420p")
	}

	if vcodec == "libx264" {
		ffmpegArgs = append(ffmpegArgs, "-preset", "veryfast", "-crf", "18", "-x264-params", "keyint=60:min-keyint=60:scenecut=0:bframes=2", "-color_range", "1")
	}

	ffmpegArgs = append(ffmpegArgs, outputHWArgs...)
	ffmpegArgs = append(ffmpegArgs, "-c:a", "aac", "-b:a", "128k")

	output := *outputFlag
	if output == "" {
		if streamKey != "" {
			output = "rtmp://a.rtmp.youtube.com/live2/" + streamKey
			log.Printf("YouTube Stream Key detected. Preparing to go LIVE in %s.", *qualityFlag)
		} else {
			output = "test.flv"
		}
	}

	if strings.HasPrefix(output, "rtmp://") || strings.HasPrefix(output, "rtmps://") || strings.HasSuffix(output, ".flv") {
		ffmpegArgs = append(ffmpegArgs, "-f", "flv")
	}

	ffmpegArgs = append(ffmpegArgs, output)
	cmd := exec.Command("ffmpeg", ffmpegArgs...)

	// Pass environment variables for VA-API debugging and driver selection
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "LIBVA_MESSAGES=1")
	if *vaapiDriverFlag != "" {
		cmd.Env = append(cmd.Env, "LIBVA_DRIVER_NAME="+*vaapiDriverFlag)
	}

	pipe, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	ffmpegStdin = pipe.(*os.File)

	// Create a pipe for audio (FD 3)
	audioReader, audioWriter, err := os.Pipe()
	if err != nil {
		log.Fatal(err)
	}
	cmd.ExtraFiles = []*os.File{audioReader}
	engine.AudioWriter = audioWriter

	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := cmd.Wait(); err != nil {
			log.Printf("ffmpeg process exited with error: %v", err)
		} else {
			log.Println("ffmpeg process exited normally")
		}
		
		// Close pipes to signal other goroutines
		if ffmpegStdin != nil {
			ffmpegStdin.Close()
			ffmpegStdin = nil
		}
		if engine.AudioWriter != nil {
			if closer, ok := engine.AudioWriter.(io.Closer); ok {
				closer.Close()
			}
			engine.AudioWriter = nil
		}
		
		log.Println("Stream connection lost. Exiting in 10s...")
		time.Sleep(10*time.Second)
		os.Exit(1)
	}()

	log.Printf("Warming up connection using %s (5s)...", vcodec)
	time.Sleep(5 * time.Second)
}
