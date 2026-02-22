package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
)

var (
	qualityFlag  = flag.String("quality", "1080p", "Stream quality: 1080p or 4k")
	headlessFlag = flag.Bool("headless", false, "Run without a local window (more stable for 24/7 streams)")
	streamKey    = os.Getenv("YOUTUBE_STREAM_KEY")
	ffmpegStdin  *os.File
	pixelBuffer  []byte
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

	// Configure Stream Output
	engine.OnFrame = func(screen *ebiten.Image) {
		if ffmpegStdin != nil {
			screen.ReadPixels(pixelBuffer)
			ffmpegStdin.Write(pixelBuffer)
		}
	}

	engine.InitPulseTexture()
	if err := engine.LoadData(); err != nil {
		log.Fatalf("Failed to initialize engine data: %v", err)
	}

	initFFmpeg(engine, width, height, bitrate, maxBitrate)

	go engine.ListenToBGP()
	go engine.StartAudioPlayer()
	go engine.StartBufferLoop()
	go engine.StartMetricsLoop()

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
	var hwArgs []string
	switch runtime.GOOS {
	case "darwin":
		vcodec = "h264_videotoolbox"
		hwArgs = []string{"-realtime", "true", "-q:v", "65", "-color_range", "1"}
	case "linux":
		if _, err := os.Stat("/dev/dri/renderD128"); err == nil {
			vcodec = "h264_vaapi"
			hwArgs = []string{"-vaapi_device", "/dev/dri/renderD128", "-vf", "format=nv12,hwupload", "-color_range", "1"}
		}
	}

	ffmpegArgs := []string{
		"-thread_queue_size", "1024",
		"-f", "rawvideo", "-pixel_format", "rgba", "-video_size", fmt.Sprintf("%dx%d", width, height),
		"-framerate", "30", "-i", "pipe:0",
		"-f", "s16le", "-ar", "44100", "-ac", "2", "-i", "pipe:3",
	}
	ffmpegArgs = append(ffmpegArgs, hwArgs...)
	ffmpegArgs = append(ffmpegArgs,
		"-c:v", vcodec,
		"-b:v", bitrate,
		"-maxrate", maxBitrate,
		"-bufsize", "30000k",
		"-g", "60",
		"-pix_fmt", "yuv420p",
	)
	if vcodec == "libx264" {
		ffmpegArgs = append(ffmpegArgs, "-preset", "veryfast", "-crf", "18", "-x264-params", "keyint=60:min-keyint=60:scenecut=0:bframes=2", "-color_range", "1")
	}
	ffmpegArgs = append(ffmpegArgs, "-c:a", "aac", "-b:a", "128k", "-f", "flv")

	output := "test.flv"
	if streamKey != "" {
		output = "rtmp://a.rtmp.youtube.com/live2/" + streamKey
		log.Printf("YouTube Stream Key detected. Preparing to go LIVE in %s.", *qualityFlag)
	}

	ffmpegArgs = append(ffmpegArgs, output)
	cmd := exec.Command("ffmpeg", ffmpegArgs...)
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
			log.Fatalf("ffmpeg process exited with error: %v", err)
		}
		log.Fatal("ffmpeg process exited unexpectedly")
	}()

	log.Printf("Warming up connection using %s (5s)...", vcodec)
	time.Sleep(5 * time.Second)
}
