package bgpengine

import (
	"encoding/binary"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
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
	close(p.stopChan)
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

			if err != nil {
				log.Printf("Failed to read audio directory: %v", err)
				select {
				case <-time.After(5 * time.Second):
				case <-p.stopChan:
					return
				}
				continue
			}

			if len(playlists) == 0 {
				log.Println("No MP3 files found in audio directory.")
				select {
				case <-time.After(5 * time.Second):
				case <-p.stopChan:
					return
				}
				continue
			}

			// Pick a random track
			path := playlists[rand.Intn(len(playlists))]

			// Extract extra credit from parent directory
			extra := ""
			parent := filepath.Dir(path)
			if parent != p.AudioDir && parent != "." {
				extra = filepath.Base(parent)
			}

			if err := p.playTrack(path, extra); err != nil {
				log.Printf("Failed to play track %s: %v", path, err)
				select {
				case <-time.After(5 * time.Second):
				case <-p.stopChan:
					return
				}
			}

			if p.isStopping {
				return
			}
		}
	}()
}

func (p *AudioPlayer) playTrack(path string, extra string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

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

	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	d, err := mp3.NewDecoder(f)
	if err != nil {
		return err
	}

	fadeDuration := 5 * time.Second
	if p.AudioWriter != nil {
		log.Printf("Streaming audio: %s", path)
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
					if stopVol <= 0 {
						return nil
					}
				}

				if vol < 0 { vol = 0 }
				if vol < 1.0 {
					for i := 0; i < n; i += 2 {
						sample := int16(binary.LittleEndian.Uint16(buf[i:]))
						sample = int16(float64(sample) * vol)
						binary.LittleEndian.PutUint16(buf[i:], uint16(sample))
					}
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

	if p.audioContext == nil {
		p.audioContext = audio.NewContext(44100)
	}
	player, err := p.audioContext.NewPlayer(d)
	if err != nil {
		return err
	}
	player.Play()
	log.Printf("Playing: %s", path)

	totalBytes := d.Length()
	duration := time.Duration(totalBytes) * time.Second / time.Duration(d.SampleRate()*4)
	startTime := time.Now()
	var stoppingAt time.Time
	for player.IsPlaying() {
		if p.isStopping && stoppingAt.IsZero() {
			stoppingAt = time.Now()
		}

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
			if stopVol <= 0 {
				break
			}
		}

		if vol < 0 { vol = 0 }
		player.SetVolume(vol)

		if remaining <= 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	player.Close()
	return nil
}
