// Package bgpengine provides the core logic for the BGP stream engine, including frame capture.
package bgpengine

import (
	"fmt"
	"image"
	"image/png"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
)

func (e *Engine) captureFrame(img *ebiten.Image, suffix string, timestamp time.Time) {
	if e.FrameCaptureDir == "" {
		return
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(e.FrameCaptureDir, 0o755); err != nil {
		log.Printf("Error creating capture directory: %v", err)
		return
	}

	filename := fmt.Sprintf("bgp-%s-%s.png", timestamp.Format("20060102-150405"), suffix)
	path := filepath.Join(e.FrameCaptureDir, filename)

	// Clone the image data. ebiten.Image.SubImage is not enough as it's a view.
	// We need to create a new ebiten.Image and draw the current image into it,
	// OR convert to a standard image.RGBA.
	// Actually, converting to image.RGBA is better for saving to disk in a goroutine.

	bounds := img.Bounds()
	rgba := image.NewRGBA(bounds)
	img.ReadPixels(rgba.Pix)

	go func() {
		f, err := os.Create(path)
		if err != nil {
			log.Printf("Error creating capture file: %v", err)
			return
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Printf("Error closing capture file: %v", err)
			}
		}()

		if err := png.Encode(f, rgba); err != nil {
			log.Printf("Error encoding capture: %v", err)
		}
		log.Printf("Captured frame: %s", path)
	}()
}
