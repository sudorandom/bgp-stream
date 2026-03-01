// Package bgpengine provides the core logic for the BGP stream engine, including data processing and visualization.
package bgpengine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/text/v2"
	"github.com/hajimehoshi/ebiten/v2/vector"
	"github.com/oschwald/maxminddb-golang"
	geojson "github.com/paulmach/go.geojson"
	"github.com/sudorandom/bgp-stream/pkg/sources"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type Location []interface{}

type EventType int

const (
	EventUnknown EventType = iota
	EventNew
	EventUpdate
	EventWithdrawal
	EventGossip
)

func (t EventType) String() string {
	switch t {
	case EventNew:
		return "new"
	case EventUpdate:
		return "upd"
	case EventWithdrawal:
		return "with"
	case EventGossip:
		return "gossip"
	default:
		return "unknown"
	}
}

type PrefixData struct {
	L []Location `json:"l"`
	R []uint32   `json:"r"`
}

type CityHub struct {
	Lat, Lng         float64
	CumulativeWeight float64
}

type Pulse struct {
	X, Y      float64
	StartTime time.Time
	Color     color.RGBA
	MaxRadius float64
	IsFlare   bool
}

type QueuedPulse struct {
	Lat, Lng      float64
	Type          EventType
	Color         color.RGBA
	Count         int
	ScheduledTime time.Time
	IsFlare       bool
}

type BufferedCity struct {
	Lat, Lng float64
	Counts   map[color.RGBA]int
}

var (
	ColorGossip = color.RGBA{0, 191, 255, 255} // Deep Sky Blue (Discovery)
	ColorNew    = color.RGBA{57, 255, 20, 255} // Hacker Green
	ColorUpd    = color.RGBA{148, 0, 211, 255} // Deep Violet/Purple (Policy Churn)
	ColorWith   = color.RGBA{255, 50, 50, 255} // Red (Withdrawal / Outage)

	// Level 2 - Cool/Neutral (Good/Normalish)
	ColorDiscovery = color.RGBA{0, 191, 255, 255} // Deep Sky Blue (Normal)
	ColorPolicy    = color.RGBA{148, 0, 211, 255} // Deep Violet/Purple (Normal)
	ColorBad       = color.RGBA{255, 127, 0, 255} // Orange (Bad)
	ColorCritical  = color.RGBA{255, 0, 0, 255}   // Pure Red (Critical)

	// Keep specific pulse colors for variety but group by tier color in legend
	ColorLinkFlap = color.RGBA{255, 127, 0, 255}
	ColorBabbling = color.RGBA{255, 165, 0, 255}
	ColorOutage   = color.RGBA{255, 50, 50, 255}
	ColorLeak     = color.RGBA{255, 0, 0, 255}
	ColorNextHop  = color.RGBA{218, 165, 32, 255}
	ColorAggFlap  = color.RGBA{255, 140, 0, 255}
	ColorOscill   = color.RGBA{148, 0, 211, 255}
	ColorHunting  = color.RGBA{148, 0, 211, 255}

	// Lighter versions for UI text and trendlines
	ColorGossipUI = color.RGBA{135, 206, 250, 255} // Light Sky Blue
	ColorNewUI    = color.RGBA{152, 255, 152, 255} // Light Green
	ColorUpdUI    = color.RGBA{218, 112, 214, 255} // Orchid (Lighter Purple)
	ColorWithUI   = color.RGBA{255, 127, 127, 255} // Light Red

	ColorNote = color.RGBA{255, 255, 255, 255} // White
	ColorPeer = color.RGBA{255, 255, 0, 255}   // Yellow
	ColorOpen = color.RGBA{0, 100, 255, 255}   // Blue
)

const (
	MaxActivePulses      = 4500
	MaxVisualQueueSize   = 15000
	DefaultPulsesPerTick = 60
	BurstPulsesPerTick   = 300
	VisualQueueThreshold = 3000
	VisualQueueCull      = 6000
)

type cacheEntry struct {
	Lat, Lng float64
	CC       string
}

type Engine struct {
	Width, Height int
	FPS           int
	Scale         float64

	pulses   []*Pulse
	pulsesMu sync.Mutex

	geo *GeoService

	cityBuffer         map[uint64]*BufferedCity
	cityBufferPool     sync.Pool
	seenBuffer         map[string]uint32
	bufferMu           sync.Mutex
	visualQueue        []*QueuedPulse
	queueMu            sync.Mutex
	nextPulseEmittedAt time.Time

	bgImage        *ebiten.Image
	pulseImage     *ebiten.Image
	flareImage     *ebiten.Image
	trendLineImg   *ebiten.Image
	trendCircleImg *ebiten.Image
	whitePixel     *ebiten.Image
	fadeMask       *ebiten.Image
	fontSource     *text.GoTextFaceSource
	monoSource     *text.GoTextFaceSource

	// Metrics (Windowed for Rate calculation)
	windowNew, windowUpd, windowWith, windowGossip int64
	windowNote, windowPeer, windowOpen             int64
	windowBeacon                                   int64

	windowLinkFlap, windowAggFlap, windowOscill, windowBabbling int64
	windowHunting, windowTE, windowNextHop, windowOutage        int64
	windowLeak, windowGlobal                                    int64

	rateNew, rateUpd, rateWith, rateGossip float64
	rateNote, ratePeer, rateOpen           float64
	rateBeacon                             float64
	displayBeaconPercent                   float64

	countryActivity map[string]int

	// History for trendlines (last 60 snapshots, 2s each = 2 mins)
	history   []MetricSnapshot
	metricsMu sync.Mutex

	CurrentSong      string
	CurrentArtist    string
	CurrentExtra     string
	songChangedAt    time.Time
	songBuffer       *ebiten.Image
	artistBuffer     *ebiten.Image
	extraBuffer      *ebiten.Image
	hubsBuffer       *ebiten.Image
	impactBuffer     *ebiten.Image
	trendLinesBuffer *ebiten.Image
	nowPlayingBuffer *ebiten.Image

	hubChangedAt      map[string]time.Time
	lastHubs          map[string]int
	hubPosition       map[string]int
	lastMetricsUpdate time.Time
	hubUpdatedAt      time.Time
	impactUpdatedAt   time.Time
	prefixCounts      []PrefixCount

	VisualHubs map[string]*VisualHub
	ActiveHubs []*VisualHub

	prefixImpactHistory []map[string]int
	prefixToASN         map[string]uint32
	prefixToLevel2      map[string]Level2EventType
	currentAnomalies    map[Level2EventType]map[string]int
	VisualImpact        map[string]*VisualImpact
	ActiveImpacts       []*VisualImpact

	SeenDB  *utils.DiskTrie
	StateDB *utils.DiskTrie

	audioPlayer *AudioPlayer

	processor *BGPProcessor

	asnMapping *utils.ASNMapping

	MinimalUI           bool
	minimalUIKeyPressed bool

	FrameCaptureInterval time.Duration
	FrameCaptureDir      string
	lastFrameCapturedAt  time.Time
	mapImage             *ebiten.Image

	// Reusable rendering resources
	face, monoFace, titleFace, titleMonoFace    *text.GoTextFace
	subFace, subMonoFace, extraFace, artistFace *text.GoTextFace
	titleFace09, titleFace05                    *text.GoTextFace
	drawOp                                      *ebiten.DrawImageOptions
	textOp                                      *text.DrawOptions
	legendRows                                  []legendRow
	vectorDrawPathOp                            vector.DrawPathOptions
	vectorFillOp                                vector.FillOptions
	vectorStrokeOp                              vector.StrokeOptions
}

type VisualHub struct {
	CC          string
	CountryStr  string
	Rate        float64
	RateStr     string
	RateWidth   float64
	DisplayY    float64
	TargetY     float64
	Alpha       float32
	TargetAlpha float32
	Active      bool
}

type PrefixCount struct {
	Name     string
	Count    int
	Color    color.RGBA
	Priority int
}

type VisualImpact struct {
	Prefix                     string
	MaskLen                    int
	ASN                        uint32
	NetworkName                string
	ClassificationName         string
	ClassificationColor        color.RGBA
	DisplayClassificationName  string
	DisplayClassificationColor color.RGBA
	asnStr                     string
	asnLines                   []string
	Count                      float64
	RateStr                    string
	RateWidth                  float64
	DisplayY                   float64
	TargetY                    float64
	Alpha                      float32
	TargetAlpha                float32
	Active                     bool
}

type MetricSnapshot struct {
	New, Upd, With, Gossip, Note, Peer, Open int
	Beacon                                   int

	LinkFlap, AggFlap, Oscill, Babbling int
	Hunting, TE, NextHop, Outage        int
	Leak, Attr, Global, Dedupe, Uncat   int
}

func NewEngine(width, height int, scale float64) *Engine {
	s, err := text.NewGoTextFaceSource(bytes.NewReader(fontInter))
	if err != nil {
		log.Printf("Fatal: failed to load Inter font: %v", err)
	}
	m, err := text.NewGoTextFaceSource(bytes.NewReader(fontMono))
	if err != nil {
		log.Printf("Fatal: failed to load Mono font: %v", err)
	}

	e := &Engine{
		Width:      width,
		Height:     height,
		FPS:        30,
		Scale:      scale,
		geo:        NewGeoService(width, height, scale),
		cityBuffer: make(map[uint64]*BufferedCity),
		cityBufferPool: sync.Pool{
			New: func() interface{} {
				return &BufferedCity{}
			},
		},
		seenBuffer:          make(map[string]uint32),
		nextPulseEmittedAt:  time.Now(),
		fontSource:          s,
		monoSource:          m,
		countryActivity:     make(map[string]int),
		history:             make([]MetricSnapshot, 60),
		hubChangedAt:        make(map[string]time.Time),
		lastHubs:            make(map[string]int),
		hubPosition:         make(map[string]int),
		lastMetricsUpdate:   time.Now(),
		VisualHubs:          make(map[string]*VisualHub),
		prefixImpactHistory: make([]map[string]int, 30), // 30 buckets * 20s = 10 mins
		prefixToLevel2:      make(map[string]Level2EventType),
		currentAnomalies:    make(map[Level2EventType]map[string]int),
		VisualImpact:        make(map[string]*VisualImpact),
		lastFrameCapturedAt: time.Now(),
		drawOp:              &ebiten.DrawImageOptions{},
		textOp:              &text.DrawOptions{},
		vectorDrawPathOp:    vector.DrawPathOptions{AntiAlias: true},
		vectorStrokeOp:      vector.StrokeOptions{Width: 3, LineJoin: vector.LineJoinBevel, LineCap: vector.LineCapButt},
	}

	e.whitePixel = ebiten.NewImage(1, 1)
	e.whitePixel.Fill(color.White)

	e.fadeMask = ebiten.NewImage(256, 1)
	pix := make([]byte, 256*4)
	for i := 0; i < 256; i++ {
		pix[i*4] = 255
		pix[i*4+1] = 255
		pix[i*4+2] = 255
		pix[i*4+3] = uint8(i)
	}
	e.fadeMask.WritePixels(pix)

	fontSize := 18.0
	if width > 2000 {
		fontSize = 36.0
	}
	e.face = &text.GoTextFace{Source: s, Size: fontSize}
	e.monoFace = &text.GoTextFace{Source: m, Size: fontSize}
	e.titleFace = &text.GoTextFace{Source: s, Size: fontSize * 0.8}
	e.titleMonoFace = &text.GoTextFace{Source: m, Size: fontSize * 0.8}
	e.subFace = &text.GoTextFace{Source: s, Size: fontSize * 0.6}
	e.subMonoFace = &text.GoTextFace{Source: m, Size: fontSize * 0.6}
	e.extraFace = &text.GoTextFace{Source: s, Size: fontSize * 0.6}
	e.artistFace = &text.GoTextFace{Source: s, Size: fontSize * 0.7}
	e.titleFace09 = &text.GoTextFace{Source: s, Size: fontSize * 0.9}
	e.titleFace05 = &text.GoTextFace{Source: s, Size: fontSize * 0.5}

	e.legendRows = []legendRow{
		// Column 1: Normal (Blue/Purple)
		{"DISCOVERY", 0, ColorDiscovery, ColorGossipUI, func(s MetricSnapshot) int { return s.Global }},
		{"POLICY CHURN", 0, ColorPolicy, ColorUpdUI, func(s MetricSnapshot) int { return s.TE }},
		{"PATH HUNTING", 0, ColorPolicy, ColorUpdUI, func(s MetricSnapshot) int { return s.Hunting }},
		{"PATH OSCILLATION", 0, ColorPolicy, ColorUpdUI, func(s MetricSnapshot) int { return s.Oscill }},

		// Column 2: Bad (Orange)
		{"BABBLING", 0, ColorBad, ColorBad, func(s MetricSnapshot) int { return s.Babbling }},
		{"AGGREGATOR FLAP", 0, ColorBad, ColorBad, func(s MetricSnapshot) int { return s.AggFlap }},
		{"NEXT-HOP FLAP", 0, ColorBad, ColorBad, func(s MetricSnapshot) int { return s.NextHop }},
		{"LINK FLAP", 0, ColorBad, ColorBad, func(s MetricSnapshot) int { return s.LinkFlap }},

		// Column 3: Critical (Red)
		{"ROUTE LEAK", 0, ColorCritical, ColorCritical, func(s MetricSnapshot) int { return s.Leak }},
		{"OUTAGE", 0, ColorCritical, ColorCritical, func(s MetricSnapshot) int { return s.Outage }},
	}

	e.audioPlayer = NewAudioPlayer(nil, func(song, artist, extra string) {
		e.CurrentSong = song
		e.CurrentArtist = artist
		e.CurrentExtra = extra
		e.songChangedAt = time.Now()
	})

	return e
}

func (e *Engine) SetAudioWriter(w io.Writer) {
	if e.audioPlayer != nil {
		e.audioPlayer.AudioWriter = w
	} else {
		e.audioPlayer = NewAudioPlayer(w, func(song, artist, extra string) {
			e.CurrentSong = song
			e.CurrentArtist = artist
			e.CurrentExtra = extra
			e.songChangedAt = time.Now()
		})
	}
}

func (e *Engine) GetAudioPlayer() *AudioPlayer {
	return e.audioPlayer
}

func (e *Engine) StartMemoryWatcher() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for range ticker.C {
			debug.FreeOSMemory()
		}
	}()
}

func (e *Engine) Update() error {
	now := time.Now()
	e.queueMu.Lock()
	added := 0
	maxAdded := DefaultPulsesPerTick
	if len(e.visualQueue) > VisualQueueThreshold {
		maxAdded = BurstPulsesPerTick
	}
	for len(e.visualQueue) > 0 && (e.visualQueue[0].ScheduledTime.Before(now) || len(e.visualQueue) > VisualQueueCull) && added < maxAdded {
		p := e.visualQueue[0]
		e.visualQueue = e.visualQueue[1:]
		added++
		if now.Sub(p.ScheduledTime) < 2*time.Second {
			e.AddPulse(p.Lat, p.Lng, p.Color, p.Count, p.IsFlare)
		}
	}
	e.queueMu.Unlock()

	if ebiten.IsKeyPressed(ebiten.KeyM) {
		if !e.minimalUIKeyPressed {
			e.MinimalUI = !e.MinimalUI
			e.minimalUIKeyPressed = true
		}
	} else {
		e.minimalUIKeyPressed = false
	}

	e.metricsMu.Lock()
	for cc, vh := range e.VisualHubs {
		// Snap Y position
		vh.DisplayY = vh.TargetY

		// Interpolate Alpha
		vh.Alpha += (vh.TargetAlpha - vh.Alpha) * 0.2

		// Cleanup inactive or invisible hubs instantly
		if !vh.Active || vh.Alpha < 0.01 {
			delete(e.VisualHubs, cc)
		}
	}

	for p, vi := range e.VisualImpact {
		// Snap Y position
		vi.DisplayY = vi.TargetY
		// Interpolate Alpha
		vi.Alpha += (vi.TargetAlpha - vi.Alpha) * 0.2

		// Cleanup inactive or invisible items instantly
		if !vi.Active || vi.Alpha < 0.01 {
			delete(e.VisualImpact, p)
		}
	}

	// Calculate 10-second rolling average for beacon percentage
	// Using a wider window prevents the donut chart from jittering too much
	// during small BGP bursts while still reacting to sustained changes.
	sumTotal := 0
	sumBeacon := 0
	hLen := len(e.history)
	window := 10
	if hLen < window {
		window = hLen
	}
	for i := hLen - window; i < hLen; i++ {
		s := e.history[i]
		sumTotal += s.New + s.Upd + s.With + s.Gossip
		sumBeacon += s.Beacon
	}

	targetPercent := 0.0
	if sumTotal > 0 {
		targetPercent = (float64(sumBeacon) / float64(sumTotal)) * 100
	}
	if targetPercent > 100 {
		targetPercent = 100
	}
	e.displayBeaconPercent += (targetPercent - e.displayBeaconPercent) * 0.1

	e.metricsMu.Unlock()

	e.pulsesMu.Lock()
	active := e.pulses[:0]
	for _, p := range e.pulses {
		duration := 1500 * time.Millisecond
		if now.Sub(p.StartTime) < duration {
			active = append(active, p)
		}
	}
	e.pulses = active
	e.pulsesMu.Unlock()
	return nil
}

func (e *Engine) drawGlitchImage(screen, img *ebiten.Image, tx, ty float64, baseAlpha float32, intensity float64, isGlitching bool) {
	if img == nil {
		return
	}
	if isGlitching && rand.Float64() < intensity {
		// More aggressive chromatic aberration
		offset := 8.0 * intensity
		jx := (rand.Float64() - 0.5) * 12.0 * intensity
		jy := (rand.Float64() - 0.5) * 4.0 * intensity

		e.drawOp.GeoM.Reset()
		e.drawOp.GeoM.Translate(tx+jx+offset, ty+jy)
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.Scale(1, 0, 0, baseAlpha*0.6)
		screen.DrawImage(img, e.drawOp)

		e.drawOp.GeoM.Reset()
		e.drawOp.GeoM.Translate(tx+jx-offset, ty+jy)
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.Scale(0, 1, 1, baseAlpha*0.6)
		screen.DrawImage(img, e.drawOp)

		// Occasional white flash
		if rand.Float64() < 0.2*intensity {
			e.drawOp.GeoM.Reset()
			e.drawOp.GeoM.Translate(tx+jx, ty+jy)
			e.drawOp.ColorScale.Reset()
			e.drawOp.ColorScale.Scale(1, 1, 1, baseAlpha*0.3)
			e.drawOp.Blend = ebiten.BlendLighter
			screen.DrawImage(img, e.drawOp)
		}
	}

	jx, jy := 0.0, 0.0
	alpha := baseAlpha
	if isGlitching && rand.Float64() < intensity {
		jx = (rand.Float64() - 0.5) * 6.0 * intensity
		jy = (rand.Float64() - 0.5) * 3.0 * intensity
		alpha = float32((0.4 + rand.Float64()*0.6) * float64(baseAlpha))
	}
	e.drawOp.GeoM.Reset()
	e.drawOp.GeoM.Translate(tx+jx, ty+jy)
	e.drawOp.ColorScale.Reset()
	e.drawOp.ColorScale.Scale(1, 1, 1, alpha)
	e.drawOp.Blend = ebiten.BlendSourceOver
	screen.DrawImage(img, e.drawOp)
}

func (e *Engine) Draw(screen *ebiten.Image) {
	if e.mapImage == nil || e.mapImage.Bounds().Dx() != e.Width || e.mapImage.Bounds().Dy() != e.Height {
		e.mapImage = ebiten.NewImage(e.Width, e.Height)
	}

	if e.bgImage != nil {
		e.mapImage.DrawImage(e.bgImage, nil)
	} else {
		e.mapImage.Fill(color.RGBA{8, 10, 15, 255})
	}
	e.pulsesMu.Lock()
	now := time.Now()
	e.drawOp.GeoM.Reset()
	e.drawOp.ColorScale.Reset()
	e.drawOp.Filter = ebiten.FilterLinear // Use linear for smooth scaling
	e.drawOp.Blend = ebiten.BlendLighter
	for _, p := range e.pulses {
		elapsed := now.Sub(p.StartTime).Seconds()
		totalDuration := 1.5
		progress := elapsed / totalDuration
		if progress > 1.0 {
			continue
		}

		baseAlpha := 0.5

		alpha := (1.0 - progress) * baseAlpha
		maxRadiusMultiplier := 1.0

		e.drawOp.GeoM.Reset()

		imgW := float64(e.pulseImage.Bounds().Dx())
		halfW := imgW / 2
		imgToDraw := e.pulseImage

		// Test: enable flares for all pulses
		if p.IsFlare {
			imgW = float64(e.flareImage.Bounds().Dx())
			halfW = imgW / 2
			imgToDraw = e.flareImage

			maxRadiusMultiplier = 3.0

			// Use sin curve: starts dim, peaks at middle, fades out
			flareIntensity := math.Sin(progress * math.Pi)       // 0 -> 1 -> 0
			flareIntensity = math.Pow(flareIntensity, 1.5) * 2.5 // Power curve for dramatic peak, massive boost
			alpha = flareIntensity                               // Full dramatic pulse effect
		}

		// Flares expand much more dramatically
		scale := (1 + progress*p.MaxRadius*maxRadiusMultiplier) / imgW * 2.0

		e.drawOp.GeoM.Translate(-halfW, -halfW)
		e.drawOp.GeoM.Scale(scale, scale)
		e.drawOp.GeoM.Translate(p.X, p.Y)

		r, g, b := float32(p.Color.R)/255.0, float32(p.Color.G)/255.0, float32(p.Color.B)/255.0
		e.drawOp.ColorScale.Reset()
		// Re-apply alpha multiplication for premultiplied alpha blending
		e.drawOp.ColorScale.Scale(r*float32(alpha), g*float32(alpha), b*float32(alpha), float32(alpha))
		e.mapImage.DrawImage(imgToDraw, e.drawOp)
	}
	e.pulsesMu.Unlock()

	shouldCapture := e.FrameCaptureInterval > 0 && now.Sub(e.lastFrameCapturedAt) >= e.FrameCaptureInterval
	if shouldCapture {
		e.lastFrameCapturedAt = now
		e.captureFrame(e.mapImage, "map", now)
	}

	screen.DrawImage(e.mapImage, nil)
	e.DrawBGPStatus(screen)

	if shouldCapture {
		e.captureFrame(screen, "full", now)
	}
}

func (e *Engine) Layout(w, h int) (width, height int) { return e.Width, e.Height }

func (e *Engine) InitPulseTexture() {
	size := 256
	e.pulseImage = ebiten.NewImage(size, size)
	pixels := make([]byte, size*size*4)
	center, maxDist := float64(size)/2.0, float64(size)/2.0
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			dx, dy := float64(x)-center, float64(y)-center
			dist := math.Sqrt(dx*dx + dy*dy)
			if dist < maxDist {
				val, outer, inner := 0.0, 0.9, 0.8
				if e.Width > 2000 {
					outer, inner = 0.94, 0.88
				}
				if dist > maxDist*outer {
					val = math.Cos(((dist - maxDist*(outer+((1-outer)/2))) / (maxDist * ((1 - outer) / 2))) * (math.Pi / 2))
				} else if dist > maxDist*inner {
					val = math.Sin(((dist - maxDist*inner) / (maxDist * (outer - inner))) * (math.Pi / 2))
				}
				pixels[(y*size+x)*4+3] = uint8(val * 255)
				pixels[(y*size+x)*4+0], pixels[(y*size+x)*4+1], pixels[(y*size+x)*4+2] = 255, 255, 255
			}
		}
	}
	e.pulseImage.WritePixels(pixels)
}

//nolint:gocognit // not important to clean up right now
func (e *Engine) InitFlareTexture() {
	// Generate lens flare texture for route leaks
	size := 256
	e.flareImage = ebiten.NewImage(size, size)
	flarePixels := make([]byte, size*size*4)

	// Initialize ALL pixels to BLACK and transparent (important for BlendLighter!)
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			idx := (y*size + x) * 4
			flarePixels[idx+0] = 0 // R = 0 for transparent pixels!
			flarePixels[idx+1] = 0 // G = 0 for transparent pixels!
			flarePixels[idx+2] = 0 // B = 0 for transparent pixels!
			flarePixels[idx+3] = 0 // Transparent
		}
	}

	// Generate realistic lens flare texture - DRAMATIC for route leaks!
	centerX, centerY := float64(size)/2.0, float64(size)/2.0
	rayThickness := float64(size) / 20.0    // THICKER rays for more visibility
	rotationAngle := 15.0 * math.Pi / 180.0 // Rotate 15 degrees

	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			fx, fy := float64(x), float64(y)
			dx, dy := fx-centerX, fy-centerY

			// Apply rotation to the coordinates
			cosA, sinA := math.Cos(rotationAngle), math.Sin(rotationAngle)
			rdx := dx*cosA - dy*sinA
			rdy := dx*sinA + dy*cosA

			dist := math.Sqrt(rdx*rdx + rdy*rdy)
			maxDist := centerX

			brightness := 0.0

			// 1. LARGE bright central core
			if dist < maxDist*0.15 {
				brightness = 1.0
			}

			// 2. Main horizontal ray (full width, BRIGHTER) - now rotated
			if math.Abs(rdy) < rayThickness {
				// Slower falloff for longer-reaching rays
				rayIntensity := 1.0 - (math.Abs(rdx) / (maxDist * 1.2))
				if rayIntensity > 0 {
					edgeFalloff := 1.0 - (math.Abs(rdy) / rayThickness)
					brightness = math.Max(brightness, rayIntensity*edgeFalloff)
				}
			}

			// 3. Main vertical ray (full height, BRIGHTER) - now rotated
			if math.Abs(rdx) < rayThickness {
				rayIntensity := 1.0 - (math.Abs(rdy) / (maxDist * 1.2))
				if rayIntensity > 0 {
					edgeFalloff := 1.0 - (math.Abs(rdx) / rayThickness)
					brightness = math.Max(brightness, rayIntensity*edgeFalloff)
				}
			}

			// 4. Diagonal rays (4 rays forming an X, BRIGHTER) - now rotated
			diagDist1 := math.Abs(rdx-rdy) / math.Sqrt(2) // Top-left to bottom-right
			diagDist2 := math.Abs(rdx+rdy) / math.Sqrt(2) // Top-right to bottom-left

			if diagDist1 < rayThickness*0.85 {
				diagLen := math.Abs(rdx+rdy) / math.Sqrt(2)
				rayIntensity := 1.0 - (diagLen / (maxDist * 1.6))
				if rayIntensity > 0 {
					edgeFalloff := 1.0 - (diagDist1 / (rayThickness * 0.85))
					brightness = math.Max(brightness, rayIntensity*edgeFalloff*0.9)
				}
			}

			if diagDist2 < rayThickness*0.85 {
				diagLen := math.Abs(rdx-rdy) / math.Sqrt(2)
				rayIntensity := 1.0 - (diagLen / (maxDist * 1.6))
				if rayIntensity > 0 {
					edgeFalloff := 1.0 - (diagDist2 / (rayThickness * 0.85))
					brightness = math.Max(brightness, rayIntensity*edgeFalloff*0.9)
				}
			}

			// Clamp and apply
			if brightness > 1.0 {
				brightness = 1.0
			}
			if brightness > 0 {
				idx := (y*size + x) * 4
				flarePixels[idx+0] = uint8(brightness * 255) // R
				flarePixels[idx+1] = uint8(brightness * 255) // G
				flarePixels[idx+2] = uint8(brightness * 255) // B
				flarePixels[idx+3] = uint8(brightness * 255) // A
			}
		}
	}

	log.Printf("Lens flare generated: %dx%d with 6-ray star pattern", size, size)

	e.flareImage.WritePixels(flarePixels)
}

func (e *Engine) InitTrendlineTexture() {
	// Create a 1x1 white image for trendlines
	e.trendLineImg = ebiten.NewImage(1, 1)
	e.trendLineImg.Fill(color.White)

	// Create a simple circular texture for trendline joints
	size := 64
	e.trendCircleImg = ebiten.NewImage(size, size)
	pixels := make([]byte, size*size*4)
	center, maxDist := float64(size)/2.0, float64(size)/2.0
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			dx, dy := float64(x)-center, float64(y)-center
			if math.Sqrt(dx*dx+dy*dy) < maxDist {
				pixels[(y*size+x)*4+3] = 255
				pixels[(y*size+x)*4+0], pixels[(y*size+x)*4+1], pixels[(y*size+x)*4+2] = 255, 255, 255
			}
		}
	}
	e.trendCircleImg.WritePixels(pixels)
}

func (e *Engine) GenerateInitialBackground() error {
	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Printf("Warning: Failed to create data directory: %v", err)
	}

	// Generate basic map background immediately
	if err := e.generateBackground(); err != nil {
		return fmt.Errorf("failed to generate background: %w", err)
	}
	return nil
}

func (e *Engine) LoadRemainingData() error {
	// 1. Open databases
	var err error
	e.SeenDB, err = utils.OpenDiskTrie("data/seen-prefixes.db")
	if err != nil {
		log.Printf("Warning: Failed to open seen prefixes database: %v. Persistent state will be disabled.", err)
	}
	e.StateDB, err = utils.OpenDiskTrie("data/prefix-state.db")
	if err != nil {
		log.Printf("Warning: Failed to open prefix state database: %v. Persistent state will be disabled.", err)
	}

	// 2. Load prefix data (this maps prefixes to coordinates)
	// This is fast if cached, slow if it has to download from RIRs
	if err := e.loadPrefixData(); err != nil {
		return err
	}

	// 3. Render historical activity if we have both data sources
	if e.SeenDB != nil {
		go e.renderHistoricalData()
	}

	// 4. Download worldcities.csv if missing
	citiesPath := "data/worldcities.csv"
	if _, err := os.Stat(citiesPath); err != nil && os.IsNotExist(err) {
		log.Printf("Downloading world cities database from %s...", sources.WorldCitiesURL)
		if err := sources.DownloadWorldCities(citiesPath); err != nil {
			log.Printf("Error downloading cities: %v", err)
		}
	}

	// 5. Load cloud data from sources of truth
	e.loadCloudData()

	if err := e.loadRemoteCityData(); err != nil {
		return err
	}

	e.asnMapping = utils.NewASNMapping()
	if err := e.asnMapping.Load(); err != nil {
		log.Printf("Warning: Failed to load ASN mapping: %v", err)
	}

	e.processor = NewBGPProcessor(e.geo.GetIPCoords, e.SeenDB, e.StateDB, e.asnMapping, e.prefixToIP, e.recordEvent)

	return nil
}

func (e *Engine) renderHistoricalData() {
	if e.bgImage == nil {
		return
	}

	// Create a copy of the background to draw on
	bounds := e.bgImage.Bounds()

	// Note: We can't easily read back from ebiten.Image in a background thread efficiently,
	// but we can just re-generate the base background quickly since it's already done once.
	// For simplicity, let's just draw the dots on top of what's already there by creating
	// a transparent overlay or just re-running the background generation.
	// Actually, let's just draw dots on a new transparent image and composite it.

	overlay := image.NewRGBA(bounds)
	dotCol := color.RGBA{100, 100, 100, 40} // Very subtle gray dots

	count := 0
	if err := e.SeenDB.ForEach(func(k, v []byte) error {
		// Key is 5 bytes: 4 bytes IP + 1 byte mask
		if len(k) != 5 {
			return nil
		}
		ip := binary.BigEndian.Uint32(k[:4])
		lat, lng, _ := e.geo.GetIPCoords(ip)
		if lat != 0 || lng != 0 {
			x, y := e.geo.Project(lat, lng)
			ix, iy := int(x), int(y)
			if ix >= 0 && ix < bounds.Dx() && iy >= 0 && iy < bounds.Dy() {
				overlay.Set(ix, iy, dotCol)
				count++
			}
		}
		return nil
	}); err != nil {
		log.Printf("Error iterating over historical prefixes: %v", err)
	}

	if count > 0 {
		log.Printf("Rendered %d historical prefixes onto the map", count)
		// Composite the historical data onto the background
		overlayImg := ebiten.NewImageFromImage(overlay)
		e.bgImage.DrawImage(overlayImg, nil)
	}
}

func (e *Engine) loadRemoteCityData() error {
	cities, err := sources.FetchCityDominance()
	if err != nil {
		return err
	}
	for _, c := range cities {
		hubs := e.geo.countryHubs[c.Country]
		weight := c.LogicalDominanceIPs
		if weight <= 0 {
			weight = 1
		}
		last := 0.0
		if len(hubs) > 0 {
			last = hubs[len(hubs)-1].CumulativeWeight
		}
		e.geo.countryHubs[c.Country] = append(hubs, CityHub{Lat: c.Coordinates[1], Lng: c.Coordinates[0], CumulativeWeight: last + weight})
	}
	return nil
}

func (e *Engine) loadCloudData() {
	var allPrefixes []sources.CloudPrefix

	// 1. Google Cloud (Geofeed - Source of Truth)
	log.Println("Fetching Google Cloud Geofeed...")
	goog, err := sources.FetchGoogleGeofeed()
	if err == nil {
		allPrefixes = append(allPrefixes, goog...)
	} else {
		log.Printf("Warning: Failed to fetch GCP geofeed: %v", err)
	}

	// 2. AWS IP Ranges
	log.Println("Fetching AWS IP Ranges...")
	aws, err := sources.FetchAWSRanges()
	if err == nil {
		allPrefixes = append(allPrefixes, aws...)
	} else {
		log.Printf("Warning: Failed to fetch AWS ranges: %v", err)
	}

	if len(allPrefixes) > 0 {
		e.geo.cloudTrie = sources.NewCloudTrie(allPrefixes)
		log.Printf("Loaded %d cloud prefixes into CloudTrie", len(allPrefixes))
	}

}

func (e *Engine) drawGrid(img *image.RGBA) {
	// Draw subtle latitude/longitude grid (Cyber-grid)
	gridColor := color.RGBA{30, 35, 45, 255}
	// Longitude lines
	for lng := -180.0; lng <= 180.0; lng += 15.0 {
		var points [][]float64
		for lat := -90.0; lat <= 90.0; lat += 2.0 {
			points = append(points, []float64{lng, lat})
		}
		e.drawRingFast(img, points, gridColor)
	}
	// Latitude lines
	for lat := -90.0; lat <= 90.0; lat += 15.0 {
		var points [][]float64
		for lng := -180.0; lng <= 180.0; lng += 2.0 {
			points = append(points, []float64{lng, lat})
		}
		e.drawRingFast(img, points, gridColor)
	}
}

func (e *Engine) generateBackground() error {
	cacheDir := "data/cache"
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		log.Printf("Warning: Failed to create cache directory: %v", err)
	}
	cacheFile := fmt.Sprintf("%s/bg_%dx%d_s%.1f.png", cacheDir, e.Width, e.Height, e.Scale)

	if img, err := e.loadCachedBackground(cacheFile); err == nil {
		e.bgImage = img
		return nil
	}

	log.Println("Generating background map...")
	start := time.Now()
	cpuImg := image.NewRGBA(image.Rect(0, 0, e.Width, e.Height))
	draw.Draw(cpuImg, cpuImg.Bounds(), &image.Uniform{color.RGBA{8, 10, 15, 255}}, image.Point{}, draw.Src)

	e.drawGrid(cpuImg)

	if err := e.drawFeatures(cpuImg); err != nil {
		return err
	}

	e.bgImage = ebiten.NewImageFromImage(cpuImg)
	log.Printf("Background map generated in %v", time.Since(start))

	go e.cacheBackground(cacheFile, cpuImg)

	return nil
}

func (e *Engine) loadCachedBackground(cacheFile string) (*ebiten.Image, error) {
	if _, err := os.Stat(cacheFile); err != nil {
		return nil, err
	}
	log.Printf("Loading cached background map from %s...", cacheFile)
	f, err := os.Open(cacheFile)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Error closing cache file: %v", err)
		}
	}()
	img, err := png.Decode(f)
	if err != nil {
		return nil, err
	}
	log.Println("Cached background map loaded successfully")
	return ebiten.NewImageFromImage(img), nil
}

func (e *Engine) drawFeatures(cpuImg *image.RGBA) error {
	fc, err := geojson.UnmarshalFeatureCollection(worldGeoJSON)
	if err != nil {
		return err
	}
	landColor, outlineColor := color.RGBA{26, 29, 35, 255}, color.RGBA{36, 42, 53, 255}
	for _, f := range fc.Features {
		if f.Geometry.IsPolygon() {
			e.fillPolygon(cpuImg, f.Geometry.Polygon, landColor)
			for _, ring := range f.Geometry.Polygon {
				e.drawRingFast(cpuImg, ring, outlineColor)
			}
		} else if f.Geometry.IsMultiPolygon() {
			for _, poly := range f.Geometry.MultiPolygon {
				e.fillPolygon(cpuImg, poly, landColor)
				for _, ring := range poly {
					e.drawRingFast(cpuImg, ring, outlineColor)
				}
			}
		}
	}
	return nil
}

func (e *Engine) cacheBackground(cacheFile string, cpuImg *image.RGBA) {
	f, err := os.Create(cacheFile)
	if err != nil {
		log.Printf("Warning: Failed to create background cache file: %v", err)
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("Error closing background cache file: %v", err)
		}
	}()
	if err := png.Encode(f, cpuImg); err != nil {
		log.Printf("Warning: Failed to encode background cache: %v", err)
	} else {
		log.Printf("Background map cached to %s", cacheFile)
	}
}

type ipRange struct {
	Start, End uint32
	CC, City   string
	Lat, Lng   float32
	Priority   int
}

func (e *Engine) loadPrefixData() error {
	log.Println("Prefix data loading started...")
	cachePath := "data/prefix-dump-cache.json"
	if data, err := os.ReadFile(cachePath); err == nil {
		if err := json.Unmarshal(data, &e.geo.prefixData); err == nil {
			debug.FreeOSMemory()
			return nil
		}
	}

	e.loadWorldCities()

	geoReader, err := e.getGeoIPReader()
	if err != nil {
		return err
	}
	defer func() {
		if err := geoReader.Close(); err != nil {
			log.Printf("Error closing GeoIP database: %v", err)
		}
	}()

	allRanges := e.fetchRIRData(geoReader)
	segments := e.flattenPrefixData(allRanges)
	e.indexPrefixData(segments)

	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Printf("Warning: Failed to create data directory: %v", err)
	}
	if f, err := os.Create(cachePath); err == nil {
		if err := json.NewEncoder(f).Encode(e.geo.prefixData); err != nil {
			log.Printf("Warning: Failed to encode prefix cache: %v", err)
		}
		if err := f.Close(); err != nil {
			log.Printf("Error closing prefix cache file: %v", err)
		}
	}
	debug.FreeOSMemory()
	return nil
}

func (e *Engine) loadWorldCities() {
	var citiesReader io.Reader
	if f, err := os.Open("data/worldcities.csv"); err == nil {
		defer func() {
			if err := f.Close(); err != nil {
				log.Printf("Error closing world cities file: %v", err)
			}
		}()
		citiesReader = f
		log.Println("Using worldcities.csv from disk")
	} else if len(worldCitiesCSV) > 0 {
		citiesReader = bytes.NewReader(worldCitiesCSV)
		log.Println("Using embedded worldcities.csv")
	}

	if citiesReader != nil {
		csvReader := csv.NewReader(citiesReader)
		if _, err := csvReader.Read(); err != nil {
			log.Printf("Warning: failed to read CSV header: %v", err)
		}
		for {
			rec, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			// Supported Format 1 (SimpleMaps): city, city_ascii, lat, lng, country, iso2, iso3, admin_name, capital, population, id
			if len(rec) >= 6 && (strings.Contains(rec[2], ".") || strings.Contains(rec[2], ",")) {
				lat, _ := strconv.ParseFloat(rec[2], 64)
				lng, _ := strconv.ParseFloat(rec[3], 64)
				e.geo.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(rec[1]), strings.ToUpper(rec[5]))] = [2]float32{float32(lat), float32(lng)}
			} else if len(rec) >= 10 {
				// Supported Format 2 (dr5hn): id, name, state_id, state_code, state_name, country_id, country_code, country_name, latitude, longitude, wikiDataId
				lat, _ := strconv.ParseFloat(rec[8], 64)
				lng, _ := strconv.ParseFloat(rec[9], 64)
				e.geo.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(rec[1]), strings.ToUpper(rec[6]))] = [2]float32{float32(lat), float32(lng)}
			}
		}
	} else {
		log.Println("Warning: No world cities data available.")
	}
}

func (e *Engine) getGeoIPReader() (*maxminddb.Reader, error) {
	var geoReader *maxminddb.Reader
	if f, err := os.ReadFile("data/ipinfo_lite.mmdb"); err == nil {
		geoReader, _ = maxminddb.FromBytes(f)
		if geoReader != nil {
			log.Println("Using ipinfo_lite.mmdb from disk")
		}
	}
	if geoReader == nil && len(geoIPDB) > 0 {
		geoReader, _ = maxminddb.FromBytes(geoIPDB)
		if geoReader != nil {
			log.Println("Using embedded ipinfo_lite.mmdb")
		}
	}

	if geoReader == nil {
		return nil, fmt.Errorf("no GeoIP database available (ipinfo_lite.mmdb)")
	}
	return geoReader, nil
}

func (e *Engine) fetchRIRData(geoReader *maxminddb.Reader) []ipRange {
	var mu sync.Mutex
	var allRanges []ipRange
	var wg sync.WaitGroup

	rirNames := []string{"APNIC", "RIPE", "AFRINIC", "LACNIC", "ARIN"}

	for _, name := range rirNames {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			e.processRIRData(n, geoReader, &mu, &allRanges)
		}(name)
	}
	wg.Wait()
	return allRanges
}

func (e *Engine) processRIRData(name string, geoReader *maxminddb.Reader, mu *sync.Mutex, allRanges *[]ipRange) {
	r, err := sources.GetRIRReader(name)
	if err != nil {
		log.Printf("[RIR-%s] Error fetching data: %v", name, err)
		return
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("Error closing RIR reader: %v", err)
		}
	}()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "|")
		if len(parts) < 7 || parts[2] != "ipv4" {
			continue
		}
		count, _ := strconv.ParseUint(parts[4], 10, 32)
		startIP := net.ParseIP(parts[3]).To4()
		if startIP != nil {
			start := binary.BigEndian.Uint32(startIP)
			p := 32
			for c := uint32(count); c > 1; c >>= 1 {
				p--
			}
			e.handleRIRRange(start, start+uint32(count)-1, strings.ToUpper(parts[1]), p, geoReader, mu, allRanges)
		}
	}
}

func (e *Engine) handleRIRRange(start, end uint32, cc string, priority int, geoReader *maxminddb.Reader, mu *sync.Mutex, allRanges *[]ipRange) {
	var lat, lng float32
	var city string

	var record struct {
		City struct {
			Names map[string]string `maxminddb:"names"`
		} `maxminddb:"city"`
	}
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, start)
	if err := geoReader.Lookup(ip, &record); err == nil {
		cityName := record.City.Names["en"]
		if c, ok := e.geo.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(cityName), strings.ToUpper(cc))]; ok {
			lat, lng = c[0], c[1]
			city = cityName
		}
	}

	if cc != "" {
		mu.Lock()
		*allRanges = append(*allRanges, ipRange{Start: start, End: end, City: city, CC: cc, Lat: lat, Lng: lng, Priority: priority})
		mu.Unlock()
	}
}

type prefixSegment struct {
	start, end uint32
	r          *ipRange
}

func (e *Engine) flattenPrefixData(allRanges []ipRange) []prefixSegment {
	// Proper Flattening (Sweep-line)
	type event struct {
		pos   uint32
		isEnd bool
		r     *ipRange
	}
	events := make([]event, 0, len(allRanges)*2)
	for i := range allRanges {
		events = append(events,
			event{allRanges[i].Start, false, &allRanges[i]},
			event{allRanges[i].End, true, &allRanges[i]},
		)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].pos != events[j].pos {
			return events[i].pos < events[j].pos
		}
		return !events[i].isEnd
	})

	var segments []prefixSegment
	activeStacks := make([][]*ipRange, 161)
	getBest := func() *ipRange {
		for p := 160; p >= 0; p-- {
			if len(activeStacks[p]) > 0 {
				return activeStacks[p][len(activeStacks[p])-1]
			}
		}
		return nil
	}
	var lastPos uint32
	var hasActive bool
	for i := 0; i < len(events); {
		pos := events[i].pos
		best := getBest()
		if hasActive && pos > lastPos {
			segments = append(segments, prefixSegment{lastPos, pos - 1, best})
		}
		for i < len(events) && events[i].pos == pos {
			r := events[i].r
			if events[i].isEnd {
				stack := activeStacks[r.Priority]
				for idx, val := range stack {
					if val == r {
						activeStacks[r.Priority] = append(stack[:idx], stack[idx+1:]...)
						break
					}
				}
			} else {
				activeStacks[r.Priority] = append(activeStacks[r.Priority], r)
			}
			i++
		}
		hasActive = getBest() != nil
		lastPos = pos
	}
	return segments
}

func (e *Engine) indexPrefixData(segments []prefixSegment) {
	// Indexing
	locToIdx := make(map[string]int)
	var locations []Location
	var flatRanges []uint32
	for _, seg := range segments {
		key := fmt.Sprintf("%s|%s|%f|%f", seg.r.CC, seg.r.City, seg.r.Lat, seg.r.Lng)
		idx, ok := locToIdx[key]
		if !ok {
			idx = len(locations)
			locations = append(locations, Location{seg.r.Lat, seg.r.Lng, seg.r.CC, seg.r.City})
			locToIdx[key] = idx
		}
		if len(flatRanges) >= 2 && flatRanges[len(flatRanges)-1] == uint32(idx) {
			continue
		} // Merge adjacent same-loc segments
		flatRanges = append(flatRanges, seg.start, uint32(idx))
	}
	e.geo.prefixData = PrefixData{L: locations, R: flatRanges}
}

// StartBufferLoop runs a background loop that periodically processes buffered BGP events.
// It aggregates high-frequency events into batches, shuffles them to prevent visual
// clustering, and paces their release into the visual queue to ensure smooth animations.
func (e *Engine) StartBufferLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		e.processSeenBuffer()
		nextBatch := e.drainCityBuffer()

		if len(nextBatch) == 0 {
			continue
		}

		e.scheduleVisualPulses(nextBatch)
	}
}

func (e *Engine) processSeenBuffer() {
	e.bufferMu.Lock()
	defer e.bufferMu.Unlock()
	// 1. Batch persist seen prefixes
	if len(e.seenBuffer) > 0 && e.SeenDB != nil {
		batch := make(map[string][]byte)
		for p, asn := range e.seenBuffer {
			asnBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(asnBytes, asn)
			batch[p] = asnBytes
		}
		e.seenBuffer = make(map[string]uint32)

		// Execute write in a separate goroutine to avoid blocking the visual queue
		go func(b map[string][]byte) {
			if err := e.SeenDB.BatchInsertRaw(b); err != nil {
				// Only log if it's not a "closing" error to reduce shutdown noise
				if !strings.Contains(err.Error(), "blocked") && !strings.Contains(err.Error(), "closed") {
					log.Printf("Warning: Failed to update seen database: %v", err)
				}
			}
		}(batch)
	}
}

func (e *Engine) drainCityBuffer() []*QueuedPulse {
	e.bufferMu.Lock()
	defer e.bufferMu.Unlock()
	var nextBatch []*QueuedPulse
	// 2. Convert buffered city activity into discrete pulse events for each color
	for key, d := range e.cityBuffer {
		for c, count := range d.Counts {
			if count > 0 {
				isFlare := (c == ColorLeak)
				nextBatch = append(nextBatch, &QueuedPulse{Lat: d.Lat, Lng: d.Lng, Color: c, Count: count, IsFlare: isFlare})
			}
		}
		// Reset and return to pool
		d.Counts = nil
		*d = BufferedCity{}
		e.cityBufferPool.Put(d)
		delete(e.cityBuffer, key)
	}
	return nextBatch
}

func (e *Engine) scheduleVisualPulses(nextBatch []*QueuedPulse) {
	// Shuffle the batch so events from different geographic locations are interleaved
	rand.Shuffle(len(nextBatch), func(i, j int) { nextBatch[i], nextBatch[j] = nextBatch[j], nextBatch[i] })

	// Spread the batch evenly across the next 500ms interval
	spacing := 500 * time.Millisecond / time.Duration(len(nextBatch))
	now := time.Now()
	if e.nextPulseEmittedAt.Before(now) {
		e.nextPulseEmittedAt = now
	}

	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	// Cap the visual backlog to prevent memory exhaustion during massive BGP spikes
	maxQueueSize := MaxVisualQueueSize
	currentSize := len(e.visualQueue)

	if currentSize < maxQueueSize {
		if currentSize+len(nextBatch) > maxQueueSize {
			log.Printf("Truncating batch of %d pulses to fit queue (Current: %d, Max: %d)", len(nextBatch), currentSize, maxQueueSize)
			nextBatch = nextBatch[:maxQueueSize-currentSize]
			if len(nextBatch) > 0 {
				spacing = 500 * time.Millisecond / time.Duration(len(nextBatch))
			}
		}

		for i, p := range nextBatch {
			// Schedule the pulse to be processed by the Update() loop at a specific time
			p.ScheduledTime = e.nextPulseEmittedAt.Add(time.Duration(i) * spacing)
			e.visualQueue = append(e.visualQueue, p)
		}
	} else {
		log.Printf("Dropping batch of %d pulses (Queue size: %d)", len(nextBatch), len(e.visualQueue))
	}

	// Advance the next emission baseline, capping the visual backlog to 2 seconds
	// to prevent the visualization from falling too far behind real-time spikes.
	e.nextPulseEmittedAt = e.nextPulseEmittedAt.Add(500 * time.Millisecond)
	if e.nextPulseEmittedAt.After(now.Add(2 * time.Second)) {
		e.nextPulseEmittedAt = now.Add(2 * time.Second)
	}
}

func (e *Engine) recordEvent(lat, lng float64, cc string, eventType EventType, level2Type Level2EventType, prefix string, asn uint32) {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()

	// 1. Track prefix impact (latest bucket)
	if prefix != "" {
		if len(e.prefixImpactHistory) > 0 {
			bucket := e.prefixImpactHistory[len(e.prefixImpactHistory)-1]
			if bucket == nil {
				bucket = make(map[string]int)
				e.prefixImpactHistory[len(e.prefixImpactHistory)-1] = bucket
			}
			bucket[prefix]++
		}
		if e.prefixToASN == nil {
			e.prefixToASN = make(map[string]uint32)
		}
		if asn != 0 {
			e.prefixToASN[prefix] = asn
		}
		if utils.IsBeaconPrefix(prefix) {
			e.windowBeacon++
		}

		// Track bad/critical anomalies specifically
		prio := e.GetPriority(level2Type.String())
		if prio >= 2 {
			e.prefixToLevel2[prefix] = level2Type
		}

		if actualType, ok := e.prefixToLevel2[prefix]; ok {
			if e.GetPriority(actualType.String()) >= 2 {
				if e.currentAnomalies[actualType] == nil {
					e.currentAnomalies[actualType] = make(map[string]int)
				}
				e.currentAnomalies[actualType][prefix]++
			}
		}
	}

	// 2. Buffer city activity
	b := e.getOrCreateCityBuffer(lat, lng)

	if cc != "" {
		e.countryActivity[cc]++
	}

	if b.Counts == nil {
		b.Counts = make(map[color.RGBA]int)
	}

	// 3. Determine color and name based on Level 2 type
	c, name := e.getLevel2Visuals(level2Type)

	// 4. Increment counts only if a Level 2 color was chosen
	if c != (color.RGBA{}) {
		b.Counts[c]++
	}

	// 5. Update Visual Impact metadata
	if prefix != "" {
		e.updateHierarchicalRates(prefix, name, c)
	}

	// 6. Update windowed metrics (this drives the dashboard numbers)
	e.updateWindowedMetrics(eventType, level2Type, prefix, asn)
}

func (e *Engine) getOrCreateCityBuffer(lat, lng float64) *BufferedCity {
	key := math.Float64bits(lat) ^ (math.Float64bits(lng) << 1)
	e.bufferMu.Lock()
	defer e.bufferMu.Unlock()
	b, ok := e.cityBuffer[key]
	if !ok {
		b = e.cityBufferPool.Get().(*BufferedCity)
		b.Lat = lat
		b.Lng = lng
		e.cityBuffer[key] = b
	}
	return b
}

func (e *Engine) getLevel2Visuals(level2Type Level2EventType) (visualColor color.RGBA, classificationName string) {
	switch level2Type {
	case Level2Discovery:
		return ColorDiscovery, nameDiscovery
	case Level2PolicyChurn:
		return ColorPolicy, namePolicyChurn
	case Level2PathLengthOscillation:
		return ColorPolicy, namePathOscillation
	case Level2PathHunting:
		return ColorPolicy, namePathHunting
	case Level2LinkFlap:
		return ColorBad, nameLinkFlap
	case Level2Babbling:
		return ColorBad, nameBabbling
	case Level2AggFlap:
		return ColorBad, nameAggFlap
	case Level2NextHopOscillation:
		return ColorBad, nameNextHopFlap
	case Level2Outage:
		return ColorOutage, nameHardOutage
	case Level2RouteLeak:
		return ColorCritical, nameRouteLeak
	default:
		return color.RGBA{}, ""
	}
}

func (e *Engine) GetPriority(name string) int {
	switch name {
	case nameRouteLeak, nameHardOutage:
		return 3 // Critical (Red)
	case nameLinkFlap, nameBabbling, nameNextHopFlap, nameAggFlap:
		return 2 // Bad (Orange)
	case namePolicyChurn, namePathOscillation, namePathHunting:
		return 1 // Normalish (Purple)
	default:
		return 0 // Discovery (Blue)
	}
}

func (e *Engine) updateHierarchicalRates(prefix, name string, c color.RGBA) {
	vi, ok := e.VisualImpact[prefix]
	if !ok {
		vi = &VisualImpact{Prefix: prefix}
		e.VisualImpact[prefix] = vi
	}
	if name != "" {
		// Only update classification if it's higher priority than what we have
		if e.GetPriority(name) >= e.GetPriority(vi.ClassificationName) {
			vi.ClassificationName = name
			vi.ClassificationColor = c
		}
	}
}

func (e *Engine) updateWindowedMetrics(eventType EventType, level2Type Level2EventType, prefix string, asn uint32) {
	switch level2Type {
	case Level2LinkFlap:
		e.windowLinkFlap++
	case Level2AggFlap:
		e.windowAggFlap++
	case Level2PathLengthOscillation:
		e.windowOscill++
	case Level2Babbling:
		e.windowBabbling++
	case Level2PathHunting:
		e.windowHunting++
	case Level2PolicyChurn:
		e.windowTE++
	case Level2NextHopOscillation:
		e.windowNextHop++
	case Level2Outage:
		e.windowOutage++
	case Level2RouteLeak:
		e.windowLeak++
	case Level2Discovery:
		e.windowGlobal++
	}

	switch eventType {
	case EventNew:
		e.windowNew++
		if prefix != "" {
			e.seenBuffer[prefix] = asn
		}
	case EventUpdate:
		e.windowUpd++
	case EventWithdrawal:
		e.windowWith++
	case EventGossip:
		e.windowGossip++
	}
}

type point struct{ x, y float64 }

func (e *Engine) projectRings(rings [][][]float64) (projectedRings [][]point, minY, maxY float64) {
	projectedRings = make([][]point, len(rings))
	minY, maxY = float64(e.Height), 0.0
	for i, ring := range rings {
		projectedRings[i] = make([]point, 0, len(ring))
		for _, p := range ring {
			x, y := e.geo.Project(p[1], p[0])
			if math.IsNaN(x) || math.IsNaN(y) {
				continue
			}
			projectedRings[i] = append(projectedRings[i], point{x, y})
			if y < minY {
				minY = y
			}
			if y > maxY {
				maxY = y
			}
		}
	}
	return projectedRings, minY, maxY
}

func (e *Engine) scanlineFill(img *image.RGBA, projectedRings [][]point, minY, maxY float64, c color.RGBA) {
	for y := int(minY); y <= int(maxY); y++ {
		if y < 0 || y >= e.Height {
			continue
		}
		var nodes []int
		fy := float64(y)
		for _, ring := range projectedRings {
			for i := 0; i < len(ring); i++ {
				j := (i + 1) % len(ring)
				if (ring[i].y < fy && ring[j].y >= fy) || (ring[j].y < fy && ring[i].y >= fy) {
					nodeX := ring[i].x + (fy-ring[i].y)/(ring[j].y-ring[i].y)*(ring[j].x-ring[i].x)
					nodes = append(nodes, int(nodeX))
				}
			}
		}
		sort.Ints(nodes)
		for i := 0; i < len(nodes)-1; i += 2 {
			xs, xe := nodes[i], nodes[i+1]
			if xs < 0 {
				xs = 0
			}
			if xe >= e.Width {
				xe = e.Width - 1
			}
			for x := xs; x < xe; x++ {
				off := y*img.Stride + x*4
				img.Pix[off], img.Pix[off+1], img.Pix[off+2], img.Pix[off+3] = c.R, c.G, c.B, 255
			}
		}
	}
}

func (e *Engine) fillPolygon(img *image.RGBA, rings [][][]float64, c color.RGBA) {
	if len(rings) == 0 {
		return
	}
	projectedRings, minY, maxY := e.projectRings(rings)
	e.scanlineFill(img, projectedRings, minY, maxY, c)
}

func (e *Engine) drawRingFast(img *image.RGBA, coords [][]float64, c color.RGBA) {
	for i := 0; i < len(coords)-1; i++ {
		x1, y1 := e.geo.Project(coords[i][1], coords[i][0])
		x2, y2 := e.geo.Project(coords[i+1][1], coords[i+1][0])
		if math.IsNaN(x1) || math.IsNaN(y1) || math.IsNaN(x2) || math.IsNaN(y2) {
			continue
		}
		e.drawLineFast(img, int(x1), int(y1), int(x2), int(y2), c)
	}
}

func (e *Engine) drawLineFast(img *image.RGBA, x1, y1, x2, y2 int, c color.RGBA) {
	dx, dy := math.Abs(float64(x2-x1)), math.Abs(float64(y2-y1))
	sx, sy := -1, -1
	if x1 < x2 {
		sx = 1
	}
	if y1 < y2 {
		sy = 1
	}
	err := dx - dy
	for {
		if x1 >= 0 && x1 < e.Width && y1 >= 0 && y1 < e.Height {
			off := y1*img.Stride + x1*4
			img.Pix[off], img.Pix[off+1], img.Pix[off+2], img.Pix[off+3] = c.R, c.G, c.B, 255
		}
		if x1 == x2 && y1 == y2 {
			break
		}
		e2 := 2 * err
		if e2 > -dy {
			err -= dy
			x1 += sx
		}
		if e2 < dx {
			err += dx
			y1 += sy
		}
	}
}

func (e *Engine) AddPulse(lat, lng float64, c color.RGBA, count int, isFlare ...bool) {
	flare := false
	if len(isFlare) > 0 {
		flare = isFlare[0]
	} else {
		flare = (c == ColorLeak)
	}

	lat += (rand.Float64() - 0.5) * 0.8
	lng += (rand.Float64() - 0.5) * 0.8
	x, y := e.geo.Project(lat, lng)
	e.pulsesMu.Lock()
	defer e.pulsesMu.Unlock()
	if len(e.pulses) < MaxActivePulses {
		baseRad := 6.0
		if e.Width > 2000 {
			baseRad = 12.0
		}
		// Use natural log (ln) for slower growth at high counts
		growth := baseRad * 1.2
		radius := baseRad + math.Log(float64(count))*growth

		if radius > 240 {
			radius = 240
		}
		e.pulses = append(e.pulses, &Pulse{X: x, Y: y, StartTime: time.Now(), Color: c, MaxRadius: radius, IsFlare: flare})
	}
}

func (e *Engine) GetProcessor() *BGPProcessor {
	return e.processor
}

func (e *Engine) prefixToIP(p string) uint32 {
	if strings.Contains(p, ":") {
		return 0 // Ignore IPv6 for now
	}
	parts := strings.Split(p, "/")
	ipStr := parts[0]
	parsedIP := net.ParseIP(ipStr).To4()
	if parsedIP == nil {
		return 0
	}
	return binary.BigEndian.Uint32(parsedIP)
}
