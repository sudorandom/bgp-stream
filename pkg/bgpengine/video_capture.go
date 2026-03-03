package bgpengine

import (
	"fmt"
	"image"
	"io"
	"os/exec"
)

type VideoCapture struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	width  int
	height int
}

func (v *VideoCapture) Start(filename string, fps int, width int, height int) error {
	v.width = width
	v.height = height

	v.cmd = exec.Command("ffmpeg",
		"-y", // Overwrite output files without asking
		"-f", "rawvideo",
		"-vcodec", "rawvideo",
		"-s", fmt.Sprintf("%dx%d", width, height),
		"-pix_fmt", "rgba",
		"-r", fmt.Sprintf("%d", fps),
		"-i", "-", // Read from stdin
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-crf", "18", // High quality
		"-pix_fmt", "yuv420p",
		filename,
	)

	stdin, err := v.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdin pipe: %v", err)
	}
	v.stdin = stdin

	if err := v.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start ffmpeg: %v", err)
	}

	return nil
}

func (v *VideoCapture) WriteFrame(img *image.RGBA) error {
	if v.stdin == nil {
		return fmt.Errorf("video capture not started")
	}

	// Make sure the image is the correct size
	if img.Rect.Dx() != v.width || img.Rect.Dy() != v.height {
		return fmt.Errorf("image dimensions (%dx%d) do not match capture dimensions (%dx%d)", img.Rect.Dx(), img.Rect.Dy(), v.width, v.height)
	}

	_, err := v.stdin.Write(img.Pix)
	if err != nil {
		return fmt.Errorf("failed to write frame to ffmpeg: %v", err)
	}

	return nil
}

func (v *VideoCapture) Close() error {
	if v.stdin != nil {
		v.stdin.Close()
	}

	if v.cmd != nil {
		if err := v.cmd.Wait(); err != nil {
			return fmt.Errorf("ffmpeg finished with error: %v", err)
		}
	}

	return nil
}
