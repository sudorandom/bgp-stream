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

	"github.com/hajimehoshi/go-mp3"
)

func (e *Engine) StartAudioPlayer() {
	go func() {
		for {
			files, err := os.ReadDir("audio")
			if err != nil {
				log.Printf("Failed to read audio directory: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			var playlists []string
			for _, f := range files {
				if !f.IsDir() && strings.HasSuffix(strings.ToLower(f.Name()), ".mp3") {
					playlists = append(playlists, filepath.Join("audio", f.Name()))
				}
			}

			if len(playlists) == 0 {
				log.Println("No MP3 files found in audio directory.")
				time.Sleep(5 * time.Second)
				continue
			}

			// Pick a random track
			path := playlists[rand.Intn(len(playlists))]
			if err := e.playTrack(path); err != nil {
				log.Printf("Failed to play track %s: %v", path, err)
				time.Sleep(5 * time.Second) // Wait before retrying to avoid hammering the system on failures
			}
		}
	}()
}

func (e *Engine) playTrack(path string) error {
	fullTitle := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	artist, song := "", fullTitle
	if parts := strings.SplitN(fullTitle, " - ", 2); len(parts) == 2 {
		artist = parts[0]
		song = parts[1]
	}

	// If this is the first song, set it immediately
	if e.CurrentSong == "" {
		e.CurrentSong = song
		e.CurrentArtist = artist
		e.songChangedAt = time.Now()
	} else {
		e.NextSong = song
		e.NextArtist = artist
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	d, err := mp3.NewDecoder(f)
	if err != nil {
		return err
	}

	// Case 1: Stream to Pipe (Headless/Streaming mode)
	if e.AudioWriter != nil {
		log.Printf("Streaming audio: %s", path)
		totalBytes := d.Length()
		duration := time.Duration(totalBytes) * time.Second / time.Duration(d.SampleRate()*4)
		fadeDuration := 5 * time.Second
		
		buf := make([]byte, 8192)
		startTime := time.Now()
		for {
			n, err := d.Read(buf)
			if n > 0 {
				elapsed := time.Since(startTime)
				remaining := duration - elapsed
				
				if remaining <= fadeDuration {
					vol := float64(remaining) / float64(fadeDuration)
					if vol < 0 { vol = 0 }
					// Apply volume to s16le samples
					for i := 0; i < n; i += 2 {
						sample := int16(binary.LittleEndian.Uint16(buf[i:]))
						sample = int16(float64(sample) * vol)
						binary.LittleEndian.PutUint16(buf[i:], uint16(sample))
					}
				}
				
				if _, err := e.AudioWriter.Write(buf[:n]); err != nil {
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

		// Update CurrentSong at the end of the track (after the fade)
		if e.NextSong != "" {
			e.CurrentSong = e.NextSong
			e.CurrentArtist = e.NextArtist
			e.NextSong = ""
			e.NextArtist = ""
			e.songChangedAt = time.Now()
		}
		return nil
	}

	// Case 2: Play to local audio device (Viewer mode)
	e.InitAudio()
	player, err := e.audioContext.NewPlayer(d)
	if err != nil {
		return err
	}
	player.Play()
	log.Printf("Playing: %s", path)

	totalBytes := d.Length()
	duration := time.Duration(totalBytes) * time.Second / time.Duration(d.SampleRate()*4)
	fadeDuration := 5 * time.Second
	startTime := time.Now()
	for player.IsPlaying() {
		remaining := duration - time.Since(startTime)
		if remaining <= fadeDuration {
			vol := float64(remaining) / float64(fadeDuration)
			if vol < 0 {
				vol = 0
			}
			player.SetVolume(vol)
		}
		if remaining <= 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	player.Close() // Ensure it is closed

	// Update CurrentSong at the end of the track (after the fade)
	if e.NextSong != "" {
		e.CurrentSong = e.NextSong
		e.CurrentArtist = e.NextArtist
		e.NextSong = ""
		e.NextArtist = ""
		e.songChangedAt = time.Now()
	}
	return nil
}
