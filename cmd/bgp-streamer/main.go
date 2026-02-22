package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
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

	// Try hardware acceleration first
	if err := runFFmpeg(engine, width, height, bitrate, maxBitrate, false); err != nil {
		log.Printf("Hardware accelerated ffmpeg failed: %v. Falling back to software encoding.", err)
		// Try software encoding
		if err := runFFmpeg(engine, width, height, bitrate, maxBitrate, true); err != nil {
			log.Fatalf("Software encoding also failed: %v", err)
		}
	}

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

func runFFmpeg(engine *bgpengine.Engine, width, height int, bitrate, maxBitrate string, forceSoftware bool) error {
	cmd, videoPipe, audioReader, audioWriter, vcodec, err := prepareFFmpeg(width, height, bitrate, maxBitrate, forceSoftware)
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		audioReader.Close()
		audioWriter.Close()
		return err
	}
	// Close the read end of the pipe in the parent process.
	// The child process has its own copy.
	audioReader.Close()

	// Assign pipes for use
	ffmpegStdin = videoPipe
	engine.AudioWriter = audioWriter

	// Monitor process for early failure
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	log.Printf("Warming up connection using %s (5s)...", vcodec)
	select {
	case err := <-done:
		// Process exited early
		// Clean up writer as it's no longer useful
		audioWriter.Close()
		ffmpegStdin = nil
		engine.AudioWriter = nil
		if err != nil {
			return fmt.Errorf("ffmpeg exited early with error: %v", err)
		}
		return fmt.Errorf("ffmpeg exited early unexpectedly")
	case <-time.After(5 * time.Second):
		// Success! Continue monitoring in background
		log.Println("Connection established successfully.")
		go func() {
			err := <-done
			if err != nil {
				log.Fatalf("ffmpeg process exited with error: %v", err)
			}
			log.Fatal("ffmpeg process exited unexpectedly")
		}()
		return nil
	}
}

func prepareFFmpeg(width, height int, bitrate, maxBitrate string, forceSoftware bool) (*exec.Cmd, *os.File, *os.File, *os.File, string, error) {
	vcodec := "libx264"
	var hwArgs []string
	if !forceSoftware {
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
	}

	ffmpegArgs = append(ffmpegArgs, output)
	cmd := exec.Command("ffmpeg", ffmpegArgs...)

	// Unset DISPLAY to prevent libva from trying to use X11/Xvfb
	env := os.Environ()
	filteredEnv := make([]string, 0, len(env))
	for _, e := range env {
		if !strings.HasPrefix(e, "DISPLAY=") {
			filteredEnv = append(filteredEnv, e)
		}
	}
	cmd.Env = filteredEnv

	videoPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	videoPipeFile := videoPipe.(*os.File)

	audioReader, audioWriter, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	cmd.ExtraFiles = []*os.File{audioReader}
	cmd.Stderr = os.Stderr

	return cmd, videoPipeFile, audioReader, audioWriter, vcodec, nil
}
