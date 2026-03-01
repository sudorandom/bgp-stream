// Package bgpengine provides the core logic for the BGP stream engine, including audio playback.
package bgpengine

import (
	"encoding/binary"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dhowden/tag"
	"github.com/hajimehoshi/ebiten/v2/audio"
	"github.com/hajimehoshi/go-mp3"
)

type AudioMetadataCallback func(song, artist, extra string)

type AudioPlayer struct {
	audioContext *audio.Context
	AudioWriter  io.Writer
	OnMetadata   AudioMetadataCallback
	AudioDir     string
	stopChan     chan struct{}
	stoppedChan  chan struct{}
	stopOnce     sync.Once
	isStopping   bool
}

func NewAudioPlayer(writer io.Writer, onMetadata AudioMetadataCallback) *AudioPlayer {
	return &AudioPlayer{
		AudioWriter: writer,
		OnMetadata:  onMetadata,
		AudioDir:    "audio",
		stopChan:    make(chan struct{}),
		stoppedChan: make(chan struct{}),
	}
}

func (p *AudioPlayer) Shutdown() {
	log.Println("Audio player shutting down with fade-out...")
	p.isStopping = true
	p.stopOnce.Do(func() {
		close(p.stopChan)
	})
	<-p.stoppedChan
	log.Println("Audio player stopped.")
}

func (p *AudioPlayer) Start() {
	go func() {
		defer close(p.stoppedChan)
		for {
			select {
			case <-p.stopChan:
				return
			default:
			}

			playlists, err := p.findPlaylists()
			if err != nil {
				log.Printf("Failed to read audio directory: %v", err)
				if p.waitForRetry() {
					return
				}
				continue
			}

			if len(playlists) == 0 {
				log.Println("No MP3 files found in audio directory.")
				if p.waitForRetry() {
					return
				}
				continue
			}

			path, extra := p.pickRandomTrack(playlists)
			if err := p.playTrack(path, extra); err != nil {
				log.Printf("Failed to play track %s: %v", path, err)
				if p.waitForRetry() {
					return
				}
			}

			if p.isStopping {
				return
			}
		}
	}()
}

func (p *AudioPlayer) findPlaylists() ([]string, error) {
	var playlists []string
	err := filepath.Walk(p.AudioDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".mp3") {
			playlists = append(playlists, path)
		}
		return nil
	})
	return playlists, err
}

func (p *AudioPlayer) pickRandomTrack(playlists []string) (trackPath, extra string) {
	trackPath = playlists[rand.Intn(len(playlists))]
	extra = ""
	parent := filepath.Dir(trackPath)
	if parent != p.AudioDir && parent != "." {
		extra = filepath.Base(parent)
	}
	return trackPath, extra
}

func (p *AudioPlayer) waitForRetry() bool {
	select {
	case <-time.After(5 * time.Second):
		return false
	case <-p.stopChan:
		return true
	}
}

func (p *AudioPlayer) playTrack(path, extra string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Error closing audio file: %v", err)
		}
	}()

	p.handleMetadata(f, path, extra)

	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	d, err := mp3.NewDecoder(f)
	if err != nil {
		return err
	}

	if p.AudioWriter != nil {
		return p.streamTrack(d, path)
	}

	return p.playTrackLocally(d, path)
}

func (p *AudioPlayer) handleMetadata(f *os.File, path, extra string) {
	var artist, song string
	if m, err := tag.ReadFrom(f); err == nil {
		artist = m.Artist()
		song = m.Title()
	}

	if song == "" {
		fullTitle := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		artist, song = "", fullTitle
		if parts := strings.SplitN(fullTitle, " - ", 2); len(parts) == 2 {
			song = parts[0]
			artist = parts[1]
		}
	}

	if p.OnMetadata != nil {
		p.OnMetadata(song, artist, extra)
	}
}

func (p *AudioPlayer) streamTrack(d *mp3.Decoder, path string) error {
	log.Printf("Streaming audio: %s", path)
	fadeDuration := 5 * time.Second
	totalBytes := d.Length()
	duration := time.Duration(totalBytes) * time.Second / time.Duration(d.SampleRate()*4)

	buf := make([]byte, 8192)
	startTime := time.Now()
	var stoppingAt time.Time

	for {
		if p.isStopping && stoppingAt.IsZero() {
			stoppingAt = time.Now()
		}

		n, err := d.Read(buf)
		if n > 0 {
			vol := p.calculateVolume(startTime, duration, stoppingAt, fadeDuration)
			if vol <= 0 && !stoppingAt.IsZero() {
				return nil
			}

			if vol < 1.0 {
				p.applyVolume(buf[:n], vol)
			}

			if _, err := p.AudioWriter.Write(buf[:n]); err != nil {
				log.Printf("Stream write error: %v", err)
				return err
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	return nil
}

func (p *AudioPlayer) playTrackLocally(d *mp3.Decoder, path string) error {
	if p.audioContext == nil {
		p.audioContext = audio.NewContext(44100)
	}
	player, err := p.audioContext.NewPlayer(d)
	if err != nil {
		return err
	}
	defer func() {
		if err := player.Close(); err != nil {
			log.Printf("Error closing audio player: %v", err)
		}
	}()

	player.Play()
	log.Printf("Playing: %s", path)

	fadeDuration := 5 * time.Second
	totalBytes := d.Length()
	duration := time.Duration(totalBytes) * time.Second / time.Duration(d.SampleRate()*4)
	startTime := time.Now()
	var stoppingAt time.Time
	for player.IsPlaying() {
		if p.isStopping && stoppingAt.IsZero() {
			stoppingAt = time.Now()
		}

		vol := p.calculateVolume(startTime, duration, stoppingAt, fadeDuration)
		player.SetVolume(vol)

		if vol <= 0 && (!stoppingAt.IsZero() || time.Since(startTime) >= duration) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (p *AudioPlayer) calculateVolume(startTime time.Time, duration time.Duration, stoppingAt time.Time, fadeDuration time.Duration) float64 {
	elapsed := time.Since(startTime)
	remaining := duration - elapsed

	vol := 1.0
	if remaining <= fadeDuration {
		vol = float64(remaining) / float64(fadeDuration)
	}

	if !stoppingAt.IsZero() {
		stopElapsed := time.Since(stoppingAt)
		stopVol := 1.0 - (float64(stopElapsed) / float64(fadeDuration))
		if stopVol < vol {
			vol = stopVol
		}
	}

	if vol < 0 {
		vol = 0
	}
	return vol
}

func (p *AudioPlayer) applyVolume(buf []byte, vol float64) {
	for i := 0; i < len(buf); i += 2 {
		sample := int16(binary.LittleEndian.Uint16(buf[i:]))
		sample = int16(float64(sample) * vol)
		binary.LittleEndian.PutUint16(buf[i:], uint16(sample))
	}
}
