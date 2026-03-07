// Package bgpengine provides the core logic for the BGP stream engine, including data processing and visualization.
package bgpengine

import (
	"bytes"
	"encoding/binary"
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
	"os/exec"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/text/v2"
	geojson "github.com/paulmach/go.geojson"
	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

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

type EventShape int

const (
	ShapeCircle EventShape = iota
	ShapeFlare
	ShapeSquare
)

type Pulse struct {
	X, Y      float64
	StartTime time.Time
	Duration  time.Duration
	Color     color.RGBA
	MaxRadius float64
	Shape     EventShape
}

type QueuedPulse struct {
	Lat, Lng      float64
	Type          EventType
	Color         color.RGBA
	Count         int
	ScheduledTime time.Time
	Shape         EventShape
}

type PulseKey struct {
	Color color.RGBA
	Shape EventShape
}

type BufferedCity struct {
	Lat, Lng float64
	Counts   map[PulseKey]int
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
	ColorLinkFlap       = color.RGBA{255, 127, 0, 255}
	ColorOutage         = color.RGBA{255, 50, 50, 255}
	ColorLeak           = color.RGBA{255, 0, 0, 255}
	ColorNextHop        = color.RGBA{218, 165, 32, 255}
	ColorAggFlap        = color.RGBA{255, 140, 0, 255}
	ColorOscill         = color.RGBA{148, 0, 211, 255}
	ColorHunting        = color.RGBA{148, 0, 211, 255}
	ColorDDoSMitigation = color.RGBA{148, 0, 211, 255} // Purple (Policy)

	// Lighter versions for UI text and trendlines
	ColorGossipUI         = color.RGBA{135, 206, 250, 255} // Light Sky Blue
	ColorNewUI            = color.RGBA{152, 255, 152, 255} // Light Green
	ColorUpdUI            = color.RGBA{218, 112, 214, 255} // Orchid (Lighter Purple)
	ColorWithUI           = color.RGBA{255, 127, 127, 255} // Light Red
	ColorDDoSMitigationUI = color.RGBA{218, 112, 214, 255} // Orchid (Lighter Purple)

	ColorNote = color.RGBA{255, 255, 255, 255} // White
	ColorPeer = color.RGBA{255, 255, 0, 255}   // Yellow
	ColorOpen = color.RGBA{0, 100, 255, 255}   // Blue
)

const (
	MaxActivePulses      = 50000
	MaxVisualQueueSize   = 300000
	DefaultPulsesPerTick = 400
	BurstPulsesPerTick   = 1500
	VisualQueueThreshold = 20000
	VisualQueueCull      = 100000
)

type asnGroupKey struct {
	ASN  uint32
	Anom string
}

type CriticalEvent struct {
	Timestamp time.Time
	Anom      string
	ASN       uint32
	ASNStr    string
	OrgID     string
	LeakType  LeakType
	LeakerASN uint32
	VictimASN uint32
	Locations string
	Color     color.RGBA
	UIColor   color.RGBA

	ImpactedIPs      uint64
	ImpactedPrefixes map[string]struct{}

	// Pre-rendered layout values
	CachedTypeLabel string
	CachedTypeWidth float64
	CachedFirstLine string

	CachedLeakerLabel string
	CachedLeakerVal   string
	CachedVictimLabel string
	CachedVictimVal   string
	CachedASNLabel    string
	CachedASNVal      string
	CachedNetLabel    string
	CachedNetVal      string
	CachedLocLabel    string
	CachedLocVal      string

	CachedImpactStr string
}

type Engine struct {
	Width, Height int
	FPS           int
	Scale         float64

	pulses   []Pulse
	pulsesMu sync.Mutex

	geo *geoservice.GeoService

	cityBuffer         map[uint64]*BufferedCity
	cityBufferPool     sync.Pool
	seenBuffer         map[string]uint32
	bufferMu           sync.Mutex
	visualQueue        []QueuedPulse
	queueMu            sync.Mutex
	nextPulseEmittedAt time.Time

	bgImage        *ebiten.Image
	pulseImage     *ebiten.Image
	flareImage     *ebiten.Image
	squareImage    *ebiten.Image
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

	windowFlap, windowTE                           int64
	windowHunting, windowOutage                    int64
	windowLeak, windowHijack, windowBogon          int64
	windowGlobal, windowDDoS                       int64
	windowHoneypot, windowResearch, windowSecurity int64

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
	impactBuffer     *ebiten.Image
	streamBuffer     *ebiten.Image
	streamClipBuffer *ebiten.Image
	trendLinesBuffer *ebiten.Image
	trendClipBuffer  *ebiten.Image
	nowPlayingBuffer *ebiten.Image
	nowPlayingDirty  bool

	trendGridVertices []ebiten.Vertex
	trendGridIndices  []uint16

	hubChangedAt map[string]time.Time
	lastHubs     map[string]int
	hubPosition  map[string]int

	lastMetricsUpdate time.Time
	lastTrendUpdate   time.Time
	lastDrawTime      time.Time
	droppedFrames     atomic.Uint64
	hubUpdatedAt      time.Time
	impactUpdatedAt   time.Time
	streamUpdatedAt   time.Time
	prefixCounts      []PrefixCount

	VisualHubs map[string]*VisualHub
	ActiveHubs []*VisualHub

	prefixImpactHistory    []map[string]int
	prefixToClassification map[string]ClassificationType
	currentAnomalies       map[ClassificationType]map[string]int
	VisualImpact           map[string]*VisualImpact
	ActiveImpacts          []*VisualImpact
	ActiveASNImpacts       []*ASNImpact
	CriticalStream         []*CriticalEvent
	criticalQueue          []*CriticalEvent
	lastCriticalAddedAt    time.Time
	streamOffset           float64
	streamDirty            bool
	streamMu               sync.Mutex
	impactDirty            bool
	criticalCooldown       map[string]time.Time

	SeenDB  *utils.DiskTrie
	StateDB *utils.DiskTrie
	RPKI    *utils.RPKIManager

	audioPlayer *AudioPlayer
	processor   *BGPProcessor
	asnMapping  *utils.ASNMapping
	geoResolver geoservice.GeoResolver
	dataMgr     *geoservice.DataManager
	MMDBFiles   []string
	AudioDir    string

	HideUI                 bool
	VideoPath              string
	VideoWriter            io.WriteCloser
	VideoCmd               *exec.Cmd
	videoBuffer            []byte
	VideoStartDelay        time.Duration
	virtualTime            time.Time
	virtualStartTime       time.Time
	MinimalUI              bool
	minimalUIKeyPressed    bool
	tourKeyPressed         bool
	tourSkipKeyPressed     bool
	tourOffset             time.Duration
	tourRegionStayDuration time.Duration

	lastPerfLog time.Time

	// Viewport and Tour state
	currentZoom         float64
	currentCX           float64
	currentCY           float64
	targetZoom          float64
	targetCX            float64
	targetCY            float64
	tourManualStartTime time.Time
	tourRegionIndex     int // -1 means no tour active (full map)
	lastTourStateChange time.Time
	pipImage            *ebiten.Image

	FrameCaptureInterval time.Duration
	FrameCaptureDir      string
	lastFrameCapturedAt  time.Time
	mapImage             *ebiten.Image

	// Reusable rendering resources
	face, monoFace, titleFace, titleMonoFace    *text.GoTextFace
	subFace, subMonoFace, extraFace, artistFace *text.GoTextFace
	titleFace09, titleFace05                    *text.GoTextFace
	drawOp                                      *ebiten.DrawImageOptions
	legendRows                                  []legendRow

	droppedPulses atomic.Uint64
	droppedQueue  atomic.Uint64
	droppedStale  atomic.Uint64

	eventCh chan *bgpEvent
	statsCh chan *statsEvent
}

type bgpEvent struct {
	lat, lng           float64
	cc                 string
	city               string
	eventType          EventType
	classificationType ClassificationType
	prefix             string
	asn                uint32
	historicalASN      uint32
	leakDetail         *LeakDetail
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
	Type     ClassificationType
	Count    int
	CountStr string
	ASNCount int
	ASNStr   string
	Rate     float64
	RateStr  string
	Color    color.RGBA
	Priority int

	// Pre-calculated widths
	RateWidth  float64
	ASNWidth   float64
	CountWidth float64
}

type ASNImpact struct {
	ASNStr    string
	Prefixes  []string
	MoreStr   string
	Anom      string
	AnomWidth float64
	Color     color.RGBA
	Count     int
	Rate      float64

	LeakType  LeakType
	LeakerASN uint32
	VictimASN uint32
	Locations string
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
	Count                      float64
	RateStr                    string
	RateWidth                  float64
	DisplayY                   float64
	TargetY                    float64
	Alpha                      float32
	TargetAlpha                float32
	Active                     bool

	LeakType  LeakType
	LeakerASN uint32
	VictimASN uint32
	CCs       map[string]struct{}
}

type MetricSnapshot struct {
	New, Upd, With, Gossip, Note, Peer, Open int
	Beacon, Honeypot, Research, Security     int

	Flap, TE, Oscill                                       int
	Hunting, NextHop, Outage                               int
	Leak, Hijack, Bogon, Attr, Global, DDoS, Dedupe, Uncat int
}

type asnGroup struct {
	asnStr     string
	prefixes   []string
	anom       string
	color      color.RGBA
	priority   int
	maxCount   float64
	totalCount float64

	leakType  LeakType
	leakerASN uint32
	victimASN uint32
	locations map[string]struct{}
}

func (e *Engine) Now() time.Time {
	if e.VideoWriter != nil {
		if e.virtualTime.IsZero() {
			e.virtualTime = time.Now()
		}
		return e.virtualTime
	}
	return time.Now()
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
		geo:        geoservice.NewGeoService(width, height, scale),
		cityBuffer: make(map[uint64]*BufferedCity),
		cityBufferPool: sync.Pool{
			New: func() interface{} {
				return &BufferedCity{}
			},
		},
		seenBuffer:             make(map[string]uint32),
		nextPulseEmittedAt:     time.Now(),
		fontSource:             s,
		monoSource:             m,
		countryActivity:        make(map[string]int),
		history:                make([]MetricSnapshot, 120),
		hubChangedAt:           make(map[string]time.Time),
		lastHubs:               make(map[string]int),
		hubPosition:            make(map[string]int),
		lastMetricsUpdate:      time.Now(),
		VisualHubs:             make(map[string]*VisualHub),
		prefixImpactHistory:    make([]map[string]int, 60), // 60 buckets * 20s = 20 mins
		prefixToClassification: make(map[string]ClassificationType),
		currentAnomalies:       make(map[ClassificationType]map[string]int),
		VisualImpact:           make(map[string]*VisualImpact),
		lastFrameCapturedAt:    time.Now(),
		drawOp:                 &ebiten.DrawImageOptions{},
		currentZoom:            1.0,
		targetZoom:             1.0,
		currentCX:              float64(width) / 2,
		currentCY:              float64(height) / 2,
		targetCX:               float64(width) / 2,
		targetCY:               float64(height) / 2,
		tourRegionIndex:        -1, // Start with full map
		tourRegionStayDuration: 10 * time.Second,
		eventCh:                make(chan *bgpEvent, 250000),
		statsCh:                make(chan *statsEvent, 250000),
		criticalCooldown:       make(map[string]time.Time),
		streamDirty:            true,
	}
	e.dataMgr = geoservice.NewDataManager(e.geo)
	go e.runEventWorker()
	go e.runStatsWorker()

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
	e.subFace = &text.GoTextFace{Source: s, Size: fontSize * 0.8}
	e.subMonoFace = &text.GoTextFace{Source: m, Size: fontSize * 0.8}
	e.extraFace = &text.GoTextFace{Source: s, Size: fontSize * 0.8}
	e.artistFace = &text.GoTextFace{Source: s, Size: fontSize * 0.7}
	e.titleFace09 = &text.GoTextFace{Source: s, Size: fontSize * 0.9}
	e.titleFace05 = &text.GoTextFace{Source: s, Size: fontSize * 0.5}

	e.legendRows = []legendRow{
		// Column 1: Normal (Blue/Purple)
		{"DISCOVERY", 0, ColorDiscovery, ColorGossipUI, func(s MetricSnapshot) int { return s.Global }},
		{"DDOS MITIGATION", 0, ColorDDoSMitigation, ColorDDoSMitigationUI, func(s MetricSnapshot) int { return s.DDoS }},
		{"PATH HUNTING", 0, ColorPolicy, ColorUpdUI, func(s MetricSnapshot) int { return s.Hunting }},
		{"TRAFFIC ENG", 0, ColorPolicy, ColorUpdUI, func(s MetricSnapshot) int { return s.TE }},

		// Column 2: Bad (Orange)
		{"FLAP", 0, ColorBad, ColorBad, func(s MetricSnapshot) int { return s.Flap }},

		// Column 3: Critical (Red)
		{"ROUTE LEAK", 0, ColorLeak, ColorWithUI, func(s MetricSnapshot) int { return s.Leak }},
		{"OUTAGE", 0, ColorOutage, ColorWithUI, func(s MetricSnapshot) int { return s.Outage }},
		{"BGP HIJACK", 0, ColorCritical, ColorWithUI, func(s MetricSnapshot) int { return s.Hijack }},
		{"BOGON / MARTIAN", 0, ColorCritical, ColorWithUI, func(s MetricSnapshot) int { return s.Bogon }},
	}

	return e
}

func (e *Engine) InitGeoOnly(readOnly bool) error {
	// Initialize GeoService if not already done
	if e.geo == nil {
		e.geo = geoservice.NewGeoService(e.Width, e.Height, e.Scale)
	}
	if e.dataMgr == nil {
		e.dataMgr = geoservice.NewDataManager(e.geo)
	}

	// Open Databases
	if err := e.geo.OpenHintDBs("data", readOnly); err != nil {
		log.Printf("Warning: failed to open hint databases: %v", err)
	}

	// Load city data
	_ = e.dataMgr.DownloadWorldCities(false)
	e.dataMgr.LoadWorldCities()
	if err := e.dataMgr.LoadRemoteCityData(); err != nil {
		log.Printf("Warning: failed to load remote city data: %v", err)
	}

	for _, path := range e.MMDBFiles {
		if err := e.geo.AddMMDBReader(path); err != nil {
			log.Printf("Warning: failed to load MMDB database %s: %v", path, err)
		}
	}

	e.geoResolver = e
	return nil
}

func (e *Engine) GetIPCoords(ip uint32) (lat, lng float64, countryCode, city string, resType geoservice.ResolutionType) {
	if e.geo == nil {
		return 0, 0, "", "", geoservice.ResUnknown
	}
	return e.geo.GetIPCoords(ip)
}

func (e *Engine) LoadRemainingData() error {
	// 1. Open databases and load city data
	if err := e.InitGeoOnly(true); err != nil {
		return err
	}

	var err error
	e.SeenDB, err = utils.OpenDiskTrie("./data/seen-prefixes.db")
	if err != nil {
		log.Printf("Warning: Failed to open seen prefixes database: %v. Persistent state will be disabled.", err)
	}
	e.StateDB, err = utils.OpenDiskTrie("./data/prefix-state.db")
	if err != nil {
		log.Printf("Warning: Failed to open prefix state database: %v. Persistent state will be disabled.", err)
	}
	e.RPKI, err = utils.NewRPKIManager("./data/rpki-vrps.db")
	if err != nil {
		log.Printf("Warning: Failed to initialize RPKI manager: %v", err)
	} else {
		// Initial sync
		go func() {
			if err := e.RPKI.Sync(); err != nil {
				log.Printf("Error during RPKI sync: %v", err)
			}
			// Periodic sync every 30 minutes
			ticker := time.NewTicker(30 * time.Minute)
			for range ticker.C {
				if err := e.RPKI.Sync(); err != nil {
					log.Printf("Error during RPKI sync: %v", err)
				}
			}
		}()
	}

	// 2. Load prefix data
	if err := e.loadPrefixData(); err != nil {
		return err
	}

	// 3. Render historical activity if we have both data sources
	// if e.SeenDB != nil {
	// 	go e.renderHistoricalData()
	// }

	e.asnMapping = utils.NewASNMapping()
	if err := e.asnMapping.Load(); err != nil {
		log.Printf("Warning: Failed to load ASN mapping: %v", err)
	}

	e.processor = NewBGPProcessor(e.GetIPCoords, e.SeenDB, e.StateDB, e.asnMapping, e.RPKI, e.prefixToIP, e.Now, e.recordEvent)

	log.Println("Engine startup complete. Listening for events...")

	return nil
}

func (e *Engine) loadPrefixData() error {
	var prefixData geoservice.PrefixData
	cachePath := "./data/prefix-dump-cache.json"
	if data, err := os.ReadFile(cachePath); err == nil {
		if err := json.Unmarshal(data, &prefixData); err == nil {
			log.Printf("[GEO] Loaded %d prefix segments from cache", len(prefixData.R)/2)
			e.geo.SetPrefixData(prefixData)
		}
	}

	var hubsData geoservice.PrefixData
	hubsCachePath := "./data/hubs-dump-cache.json"
	if data, err := os.ReadFile(hubsCachePath); err == nil {
		if err := json.Unmarshal(data, &hubsData); err == nil {
			log.Printf("[GEO] Loaded %d country hubs from cache", len(hubsData.R)/2)
			e.geo.SetHubsData(hubsData)
		}
	}

	if len(prefixData.R) == 0 && len(hubsData.R) == 0 {
		log.Println("[GEO] Warning: No prefix or hub data loaded. Geolocation will be limited.")
		log.Println("[GEO] Please run 'just fetch-data' to download and process the required data.")
	}

	return nil
}

func (e *Engine) GetGeoService() *geoservice.GeoService {
	return e.geo
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
	cacheDir := "./data/cache"
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

	// Bake cities into the CPU image
	e.drawCitiesCPU(cpuImg)

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

func (e *Engine) drawCitiesCPU(img *image.RGBA) {
	cities := e.geo.GetCities()
	drawnCount := 0

	// Track some stats for debugging
	popSum := uint64(0)

	for _, c := range cities {
		x, y := e.geo.Project(float64(c.Lat), float64(c.Lng))
		ix, iy := int(x), int(y)
		if ix < 1 || ix >= e.Width-2 || iy < 1 || iy >= e.Height-2 {
			continue
		}

		logPop := 0.0
		if c.Population > 0 {
			logPop = math.Log10(float64(c.Population))
			popSum += c.Population
		} else {
			continue
		}
		drawnCount++

		// High alpha for visibility: 180 to 255
		alpha := uint32(180 + (logPop * 10))
		if alpha > 255 {
			alpha = 255
		}

		// Bright warm-white
		cr, cg, cb := uint32(255), uint32(250), uint32(220)

		e.drawCityBloom(img, ix, iy, logPop, cr, cg, cb, alpha)
	}
	log.Printf("[GEO] Baked %d city lights into CPU background image. Avg pop: %d", drawnCount, popSum/uint64(len(cities)+1))
}

func (e *Engine) drawCityBloom(img *image.RGBA, ix, iy int, logPop float64, cr, cg, cb, alpha uint32) {
	// 1. Draw a dense 3x3 core for ALL cities to ensure visibility on high-res displays
	for dx := -1; dx <= 1; dx++ {
		for dy := -1; dy <= 1; dy++ {
			e.setPixelCPU(img, ix+dx, iy+dy, cr, cg, cb, alpha)
		}
	}

	// 2. Larger cities (> 100k) get a 5x5 bloom
	if logPop > 5.0 {
		dimAlpha := alpha / 2
		for dx := -2; dx <= 2; dx++ {
			for dy := -2; dy <= 2; dy++ {
				if (dx >= -1 && dx <= 1) && (dy >= -1 && dy <= 1) {
					continue
				}
				e.setPixelCPU(img, ix+dx, iy+dy, cr, cg, cb, dimAlpha)
			}
		}
	}

	// 3. Megacities (> 5m) get a 7x7 outer glow
	if logPop > 6.7 {
		glowAlpha := alpha / 4
		for dx := -3; dx <= 3; dx++ {
			for dy := -3; dy <= 3; dy++ {
				if (dx >= -2 && dx <= 2) && (dy >= -2 && dy <= 2) {
					continue
				}
				e.setPixelCPU(img, ix+dx, iy+dy, cr, cg, cb, glowAlpha)
			}
		}
	}
}

func (e *Engine) setPixelCPU(img *image.RGBA, x, y int, r, g, b, alpha uint32) {
	off := y*img.Stride + x*4
	dr, dg, db := uint32(img.Pix[off+0]), uint32(img.Pix[off+1]), uint32(img.Pix[off+2])

	img.Pix[off+0] = uint8((r*alpha + dr*(255-alpha)) / 255)
	img.Pix[off+1] = uint8((g*alpha + dg*(255-alpha)) / 255)
	img.Pix[off+2] = uint8((b*alpha + db*(255-alpha)) / 255)
	img.Pix[off+3] = 255
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

func (e *Engine) GenerateInitialBackground() error {
	if err := os.MkdirAll("data", 0o755); err != nil {
		log.Printf("Warning: Failed to create data directory: %v", err)
	}

	// Ensure city data is loaded for background generation
	if e.dataMgr == nil {
		e.dataMgr = geoservice.NewDataManager(e.geo)
	}
	_ = e.dataMgr.DownloadWorldCities(false)
	e.dataMgr.LoadWorldCities()

	if err := e.generateBackground(); err != nil {
		return fmt.Errorf("failed to generate background: %w", err)
	}
	return nil
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

func (e *Engine) AddPulse(lat, lng float64, c color.RGBA, count int, shape ...EventShape) {
	// De-emphasize Discovery/None pulses (Blue) - less aggressively now
	// if c == ColorDiscovery && rand.Float64() > 0.4 {
	// 	return
	// }
	s := ShapeCircle
	if len(shape) > 0 {
		s = shape[0]
	} else if c == ColorLeak {
		s = ShapeFlare
	}

	lat += (rand.Float64() - 0.5) * 1.5
	lng += (rand.Float64() - 0.5) * 1.5
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

		// Randomize duration to prevent synchronized expiration and 'emptying out'
		duration := 1200*time.Millisecond + time.Duration(rand.Intn(800))*time.Millisecond

		e.pulses = append(e.pulses, Pulse{
			X: x, Y: y,
			StartTime: e.Now(),
			Duration:  duration,
			Color:     c,
			MaxRadius: radius,
			Shape:     s,
		})
	} else {
		e.droppedPulses.Add(1)
	}
}

func (e *Engine) GetProcessor() *BGPProcessor {
	return e.processor
}

func (e *Engine) UpdatePerformanceMetrics() {
	now := e.Now()
	if now.Sub(e.lastPerfLog) < 5*time.Second {
		return
	}
	e.lastPerfLog = now

	tps := ebiten.ActualTPS()
	fps := ebiten.ActualFPS()
	droppedPulses := e.droppedPulses.Swap(0)
	droppedQueue := e.droppedQueue.Swap(0)
	droppedStale := e.droppedStale.Swap(0)
	droppedFrames := e.droppedFrames.Swap(0)

	if tps < 28 || fps < 28 || droppedPulses > 0 || droppedQueue > 0 || droppedStale > 0 || droppedFrames > 0 {
		var sb strings.Builder
		sb.WriteString("[PERF]")
		fmt.Fprintf(&sb, " TPS: %.2f, FPS: %.2f", tps, fps)
		if droppedFrames > 0 {
			fmt.Fprintf(&sb, ", DroppedFrames: %d", droppedFrames)
		}
		if droppedPulses > 0 {
			fmt.Fprintf(&sb, ", DroppedPulses: %d", droppedPulses)
		}
		if droppedQueue > 0 {
			fmt.Fprintf(&sb, ", DroppedQueue: %d", droppedQueue)
		}
		if droppedStale > 0 {
			fmt.Fprintf(&sb, ", DroppedStale: %d", droppedStale)
		}
		if tps < 28 || fps < 28 {
			sb.WriteString(" (Lag detected)")
		}
		log.Println(sb.String())
	}
}

func (e *Engine) Update() error {
	if e.VideoWriter != nil {
		if e.virtualTime.IsZero() {
			e.virtualTime = time.Now()
			e.virtualStartTime = e.virtualTime
		} else {
			// Advance virtual clock by exactly 1/30s per frame
			e.virtualTime = e.virtualTime.Add(time.Second / 30)
		}
	}

	e.UpdateTour()
	e.UpdatePerformanceMetrics()
	e.updateVisualQueue()
	e.updateInput()
	e.updateMetrics()
	e.updateCriticalStream()
	e.updateActivePulses()
	return nil
}

func (e *Engine) updateVisualQueue() {
	now := e.Now()
	e.queueMu.Lock()
	defer e.queueMu.Unlock()

	added := 0
	maxAdded := DefaultPulsesPerTick
	if len(e.visualQueue) > VisualQueueThreshold {
		maxAdded = BurstPulsesPerTick
	}

	for len(e.visualQueue) > 0 && (e.visualQueue[0].ScheduledTime.Before(now) || len(e.visualQueue) > VisualQueueCull) && added < maxAdded {
		p := e.visualQueue[0]
		e.visualQueue = e.visualQueue[1:]
		added++
		// Allow up to 5 seconds of delay during massive spikes before dropping pulses
		if now.Sub(p.ScheduledTime) < 5*time.Second {
			e.AddPulse(p.Lat, p.Lng, p.Color, p.Count, p.Shape)
		} else {
			e.droppedStale.Add(1)
		}
	}
}

func (e *Engine) updateInput() {
	if ebiten.IsKeyPressed(ebiten.KeyM) {
		if !e.minimalUIKeyPressed {
			e.MinimalUI = !e.MinimalUI
			e.minimalUIKeyPressed = true
		}
	} else {
		e.minimalUIKeyPressed = false
	}

	if ebiten.IsKeyPressed(ebiten.KeyT) {
		if !e.tourKeyPressed {
			e.tourManualStartTime = e.Now()
			e.tourOffset = 0 // Reset offset on manual start
			e.tourKeyPressed = true
		}
	} else {
		e.tourKeyPressed = false
	}

	if ebiten.IsKeyPressed(ebiten.KeyN) {
		if !e.tourSkipKeyPressed {
			e.handleTourSkip()
			e.tourSkipKeyPressed = true
		}
	} else {
		e.tourSkipKeyPressed = false
	}
}

func (e *Engine) handleTourSkip() {
	// Calculate current elapsed time to figure out the next jump
	now := e.Now()
	tourDuration := time.Duration(len(regions)) * e.tourRegionStayDuration
	cycleDuration := 10 * time.Minute
	elapsedInCycle := now.Sub(now.Truncate(cycleDuration))
	elapsedSinceManual := now.Sub(e.tourManualStartTime)

	var elapsed time.Duration
	if !e.tourManualStartTime.IsZero() && elapsedSinceManual < tourDuration+10*time.Second {
		elapsed = elapsedSinceManual + e.tourOffset
	} else {
		elapsed = elapsedInCycle + e.tourOffset
	}

	// Snap to the beginning of the next 10-second bucket
	currentIdx := int(elapsed.Seconds() / e.tourRegionStayDuration.Seconds())
	targetElapsed := time.Duration(currentIdx+1) * e.tourRegionStayDuration
	e.tourOffset += (targetElapsed - elapsed)
}

func (e *Engine) updateMetrics() {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()

	for cc, vh := range e.VisualHubs {
		vh.DisplayY = vh.TargetY
		vh.Alpha += (vh.TargetAlpha - vh.Alpha) * 0.2
		if !vh.Active || vh.Alpha < 0.01 {
			delete(e.VisualHubs, cc)
		}
	}

	e.updateBeaconPercent()

	// Cleanup Critical Event Stream (remove entries older than 10 mins)
	now := e.Now()
	e.streamMu.Lock()
	activeStream := e.CriticalStream[:0]
	removedAny := false
	for _, ce := range e.CriticalStream {
		if now.Sub(ce.Timestamp) < 10*time.Minute {
			activeStream = append(activeStream, ce)
		} else {
			removedAny = true
		}
	}
	if removedAny {
		e.CriticalStream = activeStream
		e.streamDirty = true
	}
	e.streamMu.Unlock()
}

func (e *Engine) updateBeaconPercent() {
	sumTotal, sumBeacon := 0, 0
	hLen := len(e.history)
	window := 10
	if hLen < window {
		window = hLen
	}
	for i := hLen - window; i < hLen; i++ {
		s := e.history[i]
		sumTotal += s.New + s.Upd + s.With + s.Gossip
		sumBeacon += s.Beacon + s.Honeypot + s.Research + s.Security
	}

	targetPercent := 0.0
	if sumTotal > 0 {
		targetPercent = (float64(sumBeacon) / float64(sumTotal)) * 100
	}
	if targetPercent > 100 {
		targetPercent = 100
	}
	e.displayBeaconPercent += (targetPercent - e.displayBeaconPercent) * 0.1
}

func (e *Engine) updateActivePulses() {
	now := e.Now()
	e.pulsesMu.Lock()
	defer e.pulsesMu.Unlock()

	active := e.pulses[:0]
	for _, p := range e.pulses {
		if now.Sub(p.StartTime) < p.Duration {
			active = append(active, p)
		}
	}
	e.pulses = active
}

func (e *Engine) Draw(screen *ebiten.Image) {
	now := e.Now()
	if !e.lastDrawTime.IsZero() {
		if now.Sub(e.lastDrawTime) > 50*time.Millisecond {
			e.droppedFrames.Add(1)
		}
	}
	e.lastDrawTime = now

	if e.mapImage == nil || e.mapImage.Bounds().Dx() != e.Width || e.mapImage.Bounds().Dy() != e.Height {
		e.mapImage = ebiten.NewImage(e.Width, e.Height)
	}

	if e.bgImage != nil {
		e.mapImage.DrawImage(e.bgImage, nil)
	} else {
		e.mapImage.Fill(color.RGBA{8, 10, 15, 255})
	}

	e.pulsesMu.Lock()
	now = e.Now()
	e.drawOp.GeoM.Reset()
	e.drawOp.ColorScale.Reset()
	e.drawOp.Filter = ebiten.FilterLinear // Use linear for smooth scaling
	e.drawOp.Blend = ebiten.BlendLighter

	// Batch pulses by image to reduce draw calls
	for _, p := range e.pulses {
		elapsed := now.Sub(p.StartTime).Seconds()
		totalDuration := p.Duration.Seconds()
		progress := elapsed / totalDuration
		if progress > 1.0 {
			continue
		}

		baseAlpha := 0.5
		alpha := (1.0 - progress) * baseAlpha
		maxRadiusMultiplier := 1.0

		imgW := float64(e.pulseImage.Bounds().Dx())
		imgToDraw := e.pulseImage

		switch p.Shape {
		case ShapeFlare:
			imgW = float64(e.flareImage.Bounds().Dx())
			imgToDraw = e.flareImage
			maxRadiusMultiplier = 3.0
			flareIntensity := math.Sin(progress * math.Pi)
			flareIntensity = math.Pow(flareIntensity, 1.5) * 2.5
			alpha = flareIntensity
		case ShapeSquare:
			imgW = float64(e.squareImage.Bounds().Dx())
			imgToDraw = e.squareImage
			// A square pulse can have similar radius expansion
		}

		scale := (1 + progress*p.MaxRadius*maxRadiusMultiplier) / imgW * 2.0
		halfW := imgW / 2

		e.drawOp.GeoM.Reset()
		e.drawOp.GeoM.Translate(-halfW, -halfW)
		e.drawOp.GeoM.Scale(scale, scale)
		e.drawOp.GeoM.Translate(p.X, p.Y)

		r, g, b := float32(p.Color.R)/255.0, float32(p.Color.G)/255.0, float32(p.Color.B)/255.0
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.Scale(r*float32(alpha), g*float32(alpha), b*float32(alpha), float32(alpha))
		e.mapImage.DrawImage(imgToDraw, e.drawOp)
	}
	e.pulsesMu.Unlock()

	shouldCapture := e.FrameCaptureInterval > 0 && now.Sub(e.lastFrameCapturedAt) >= e.FrameCaptureInterval
	if shouldCapture {
		e.lastFrameCapturedAt = now
		e.captureFrame(e.mapImage, "map", now)
	}

	e.drawOp.GeoM.Reset()
	e.drawOp.ColorScale.Reset()
	e.ApplyTourTransform(e.drawOp)
	screen.DrawImage(e.mapImage, e.drawOp)

	if !e.HideUI {
		e.DrawPIP(screen)
		e.DrawBGPStatus(screen)
	}

	if shouldCapture {
		e.captureFrame(screen, "full", now)
	}

	if e.VideoWriter != nil {
		if e.virtualStartTime.IsZero() || e.Now().Sub(e.virtualStartTime) >= e.VideoStartDelay {
			e.captureVideoFrame(screen)
		}
	}
}

func (e *Engine) Layout(w, h int) (width, height int) { return e.Width, e.Height }

func (e *Engine) runEventWorker() {
	batch := make([]*bgpEvent, 0, 1000)
	ticker := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case ev, ok := <-e.eventCh:
			if !ok {
				return
			}
			batch = append(batch, ev)
			if len(batch) >= 1000 {
				e.processEventBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				e.processEventBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (e *Engine) processEventBatch(batch []*bgpEvent) {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()

	for _, ev := range batch {
		e.processEventLocked(ev)
	}
}

func (e *Engine) recordEvent(lat, lng float64, cc, city string, eventType EventType, classificationType ClassificationType, prefix string, asn, historicalASN uint32, leakDetail ...*LeakDetail) {
	var ld *LeakDetail
	if len(leakDetail) > 0 {
		ld = leakDetail[0]
	}
	select {
	case e.eventCh <- &bgpEvent{lat, lng, cc, city, eventType, classificationType, prefix, asn, historicalASN, ld}:
	default:
		// Drop event if engine is too busy
	}
}

func (e *Engine) processEventLocked(ev *bgpEvent) {
	// 1. Track prefix impact (latest bucket)
	if ev.prefix != "" {
		if utils.IsBeaconPrefix(ev.prefix) {
			e.windowBeacon++
		}
	}

	// 2. Determine color, name, and shape based on Level 2 type
	c, name, shape := e.getClassificationVisuals(ev.classificationType)

	// 3. Buffer city activity
	e.incrementCityBuffer(ev.lat, ev.lng, c, shape)

	// 4. Record to CriticalStream if it's a critical anomaly
	if ev.classificationType == ClassificationOutage || ev.classificationType == ClassificationRouteLeak || ev.classificationType == ClassificationDDoSMitigation || ev.classificationType == ClassificationHijack || ev.classificationType == ClassificationBogon {
		e.recordToCriticalStream(ev, c, name)
	}

	// 5. Update windowed metrics (this drives the dashboard numbers)
	e.updateWindowedMetrics(ev.eventType, ev.classificationType, ev.prefix, ev.asn)

	// Filter out invalid DDoS Mitigation events from stats so they don't appear in summaries
	// We require both provider and victim ASN to be known to show a meaningful summary item.
	if e.isIgnoredDDoS(ev) {
		return
	}

	// 6. Offload remaining heavy stat accumulation to stats worker
	select {
	case e.statsCh <- &statsEvent{ev: ev, name: name, c: c}:
	default:
		// Drop stat tracking if channel full to preserve main UI thread
	}
}

func (e *Engine) getBestLocation(prefix, cc, city string) string {
	if city != "" && cc != "" {
		return fmt.Sprintf("%s, %s", city, cc)
	}

	// If we have a country but no city, try to re-resolve the IP to see if we can find a city.
	// This helps when the current event (like a withdrawal) has sparse geo metadata.
	if cc != "" && city == "" {
		ip := e.prefixToIP(prefix)
		if ip != 0 {
			_, _, _, resolvedCity, _ := e.GetIPCoords(ip)
			if resolvedCity != "" {
				return fmt.Sprintf("%s, %s", resolvedCity, cc)
			}
		}
	}

	return cc
}

func (e *Engine) isIgnoredDDoS(ev *bgpEvent) bool {
	if ev.classificationType != ClassificationDDoSMitigation {
		return false
	}

	providerASN := ev.asn
	if providerASN == 0 && ev.leakDetail != nil {
		providerASN = ev.leakDetail.LeakerASN
	}

	if providerASN == 0 {
		return true
	}

	victimASN := uint32(0)
	if ev.leakDetail != nil {
		victimASN = ev.leakDetail.VictimASN
	}
	if victimASN == 0 {
		victimASN = ev.historicalASN
	}

	// We used to filter out self-mitigation (victim == provider), but
	// for the Major Event Stream, even self-mitigation via RTBH is a
	// significant event for the prefix.
	if victimASN == 0 {
		return true
	}

	return false
}

func (e *Engine) recordToCriticalStream(ev *bgpEvent, c color.RGBA, name string) {
	// Filter out ASN 0 for outages as requested
	if ev.classificationType == ClassificationOutage && ev.asn == 0 {
		return
	}

	// Filter out excluded ASN categories
	if utils.IsExcludedASN(ev.asn) {
		return
	}

	// Filter out invalid DDoS Mitigation events
	if e.isIgnoredDDoS(ev) {
		return
	}

	now := e.Now()
	asnToUse := ev.asn
	if asnToUse == 0 {
		asnToUse = ev.historicalASN
	}

	asnStr := fmt.Sprintf("AS%d", asnToUse)
	orgID := ""
	if e.asnMapping != nil {
		orgID = e.asnMapping.GetOrgID(asnToUse)
		if n := e.asnMapping.GetName(asnToUse); n != "" {
			asnStr += " - " + n
		}
	}

	newLoc := e.getBestLocation(ev.prefix, ev.cc, ev.city)

	e.streamMu.Lock()
	defer e.streamMu.Unlock()

	// Check for duplicates across the entire visible stream
	for i, ce := range e.CriticalStream {
		if e.isSameEvent(ce, ev, name, orgID) {
			// Check if this specific prefix is still in the same critical state
			_, currentAnomName, _ := e.getClassificationVisuals(ev.classificationType)
			if currentAnomName != ce.Anom {
				// Prefix transitioned to a different state!
				// Remove it from THIS critical event
				if ce.ImpactedPrefixes != nil {
					if _, exists := ce.ImpactedPrefixes[ev.prefix]; exists {
						delete(ce.ImpactedPrefixes, ev.prefix)
						size := utils.GetPrefixSize(ev.prefix)
						if ce.ImpactedIPs >= size {
							ce.ImpactedIPs -= size
						} else {
							ce.ImpactedIPs = 0
						}
						e.updateCriticalEventCacheStrs(ce)
						e.streamDirty = true

						// If no prefixes remain, remove the event from the stream
						if len(ce.ImpactedPrefixes) == 0 {
							e.CriticalStream = append(e.CriticalStream[:i], e.CriticalStream[i+1:]...)
						}
					}
				}

				// If the new state is ALSO critical, it will be added as a new event below
				// (or matched to another existing event of that type)
				if ev.classificationType == ClassificationOutage || ev.classificationType == ClassificationRouteLeak || ev.classificationType == ClassificationDDoSMitigation || ev.classificationType == ClassificationHijack || ev.classificationType == ClassificationBogon {
					// Fallthrough to add/match for the NEW critical type
					break
				}
				return
			}

			ce.Timestamp = now // Reset expiration timer
			// If we found an existing event, update it in place
			if e.updateExistingCriticalEvent(ce, ev) {
				e.updateCriticalEventCacheStrs(ce)
			}
			e.streamDirty = true
			return
		}
	}

	// Also check for duplicates in the pending queue
	for i, ce := range e.criticalQueue {
		if e.isSameEvent(ce, ev, name, orgID) {
			_, currentAnomName, _ := e.getClassificationVisuals(ev.classificationType)
			if currentAnomName != ce.Anom {
				// Transition in queue
				if ce.ImpactedPrefixes != nil {
					if _, exists := ce.ImpactedPrefixes[ev.prefix]; exists {
						delete(ce.ImpactedPrefixes, ev.prefix)
						size := utils.GetPrefixSize(ev.prefix)
						if ce.ImpactedIPs >= size {
							ce.ImpactedIPs -= size
						} else {
							ce.ImpactedIPs = 0
						}
						e.updateCriticalEventCacheStrs(ce)
						if len(ce.ImpactedPrefixes) == 0 {
							e.criticalQueue = append(e.criticalQueue[:i], e.criticalQueue[i+1:]...)
						}
					}
				}
				if ev.classificationType == ClassificationOutage || ev.classificationType == ClassificationRouteLeak || ev.classificationType == ClassificationDDoSMitigation || ev.classificationType == ClassificationHijack || ev.classificationType == ClassificationBogon {
					break
				}
				return
			}

			ce.Timestamp = now // Reset expiration timer
			// If we found an existing event, update it in place
			if e.updateExistingCriticalEvent(ce, ev) {
				e.updateCriticalEventCacheStrs(ce)
			}
			return
		}
	}

	// Filter out outages with low impact (< 1000 IPs)
	// We only add NEW outages to the stream if they meet the threshold.
	if ev.classificationType == ClassificationOutage && utils.GetPrefixSize(ev.prefix) < 1000 {
		return
	}

	// Only add a new event if it's not on a per-prefix cooldown
	// Substantial cooldown: 10 minutes
	if lastTime, ok := e.criticalCooldown[ev.prefix]; ok && now.Sub(lastTime) < 10*time.Minute {
		return
	}

	e.criticalCooldown[ev.prefix] = now
	ce := e.createCriticalEvent(ev, c, name, asnStr, orgID, newLoc, now)
	e.criticalQueue = append(e.criticalQueue, ce)
	e.streamUpdatedAt = now
}

func (e *Engine) isSameEvent(ce *CriticalEvent, ev *bgpEvent, name, orgID string) bool {
	if ce.Anom != name {
		return false
	}

	// Fallback: If it's the exact same prefix and anomaly type, it's the same event.
	// This is the most robust check for "re-shifting" of the same item.
	if ev.prefix != "" && ce.ImpactedPrefixes != nil {
		if _, ok := ce.ImpactedPrefixes[ev.prefix]; ok {
			return true
		}
	}

	// For Route Leaks match based on leak details
	if name == nameRouteLeak && ev.leakDetail != nil {
		return (ce.LeakType == LeakUnknown || ce.LeakType == ev.leakDetail.Type) &&
			ce.LeakerASN == ev.leakDetail.LeakerASN && ce.VictimASN == ev.leakDetail.VictimASN
	}

	// For DDoS Mitigation, always match by provider/victim pair
	if name == nameDDoSMitigation {
		leaker := ev.asn
		victim := ev.historicalASN
		if ev.leakDetail != nil {
			leaker = ev.leakDetail.LeakerASN
			victim = ev.leakDetail.VictimASN
		}
		// If both are known/0, we can't safely deduplicate across victims
		if leaker == 0 && victim == 0 {
			return false
		}
		return ce.LeakerASN == leaker && ce.VictimASN == victim
	}

	// For other anomalies (like Outages), match by Origin ASN
	asn := ev.asn
	if asn == 0 {
		asn = ev.historicalASN
	}

	if asn != 0 && ce.ASN != 0 && asn == ce.ASN {
		return true
	}

	// Match by Organization (if known)
	if orgID != "" && ce.OrgID != "" && orgID == ce.OrgID {
		return true
	}
	return false
}

func (e *Engine) updateExistingCriticalEvent(ce *CriticalEvent, ev *bgpEvent) bool {
	newLoc := e.getBestLocation(ev.prefix, ev.cc, ev.city)
	needsUpdate := false
	// Update locations
	if newLoc != "" && !strings.Contains(ce.Locations, newLoc) {
		if ce.Locations == "" {
			ce.Locations = newLoc
		} else {
			ce.Locations += " | " + newLoc
		}
		needsUpdate = true
	}

	// Update IP Impact
	if ev.classificationType == ClassificationOutage || ev.classificationType == ClassificationRouteLeak || ev.classificationType == ClassificationDDoSMitigation || ev.classificationType == ClassificationHijack || ev.classificationType == ClassificationBogon {
		if ce.ImpactedPrefixes == nil {
			ce.ImpactedPrefixes = make(map[string]struct{})
		}
		if _, exists := ce.ImpactedPrefixes[ev.prefix]; !exists {
			ce.ImpactedPrefixes[ev.prefix] = struct{}{}
			ce.ImpactedIPs += utils.GetPrefixSize(ev.prefix)
			needsUpdate = true
		}
	}

	// RETROACTIVE UPDATE: If we now have leak details that the previous entry lacked, update it
	if ce.Anom == nameDDoSMitigation {
		if ce.LeakerASN == 0 {
			if ev.leakDetail != nil && ev.leakDetail.LeakerASN != 0 {
				ce.LeakerASN = ev.leakDetail.LeakerASN
				needsUpdate = true
			} else if ev.asn != 0 {
				ce.LeakerASN = ev.asn
				needsUpdate = true
			}
		}
		if ce.VictimASN == 0 {
			if ev.leakDetail != nil && ev.leakDetail.VictimASN != 0 {
				ce.VictimASN = ev.leakDetail.VictimASN
				needsUpdate = true
			} else if ev.historicalASN != 0 {
				ce.VictimASN = ev.historicalASN
				needsUpdate = true
			}
		}
	} else if ce.LeakType == LeakUnknown && ev.leakDetail != nil {
		ce.LeakType = ev.leakDetail.Type
		ce.LeakerASN = ev.leakDetail.LeakerASN
		ce.VictimASN = ev.leakDetail.VictimASN
		needsUpdate = true
	}
	return needsUpdate
}

func (e *Engine) createCriticalEvent(ev *bgpEvent, c color.RGBA, name, asnStr, orgID, newLoc string, now time.Time) *CriticalEvent {
	asnToUse := ev.asn
	if asnToUse == 0 {
		asnToUse = ev.historicalASN
	}
	ce := &CriticalEvent{
		Timestamp:        now,
		Anom:             name,
		ASN:              asnToUse,
		ASNStr:           asnStr,
		OrgID:            orgID,
		Color:            c,
		UIColor:          e.getClassificationUIColor(name),
		Locations:        newLoc,
		ImpactedPrefixes: make(map[string]struct{}),
	}
	if ev.classificationType == ClassificationOutage || ev.classificationType == ClassificationRouteLeak || ev.classificationType == ClassificationDDoSMitigation || ev.classificationType == ClassificationHijack || ev.classificationType == ClassificationBogon {
		ce.ImpactedPrefixes[ev.prefix] = struct{}{}
		ce.ImpactedIPs = utils.GetPrefixSize(ev.prefix)
	}
	if ev.leakDetail != nil {
		ce.LeakType = ev.leakDetail.Type
		ce.LeakerASN = ev.leakDetail.LeakerASN
		ce.VictimASN = ev.leakDetail.VictimASN
	}
	if ce.Anom == nameDDoSMitigation {
		if ce.LeakerASN == 0 {
			ce.LeakerASN = ev.asn
		}
		if ce.VictimASN == 0 {
			ce.VictimASN = ev.historicalASN
		}
	}
	e.updateCriticalEventCacheStrs(ce)
	return ce
}

func (e *Engine) updateCriticalStream() {
	e.streamMu.Lock()
	defer e.streamMu.Unlock()

	now := e.Now()

	// 1. Animate offset towards 0
	if math.Abs(e.streamOffset) > 0.1 {
		e.streamOffset *= 0.85
		e.streamDirty = true
	} else if e.streamOffset != 0 {
		e.streamOffset = 0
		e.streamDirty = true
	}

	// 2. Handle queue with 1 second gap
	if len(e.criticalQueue) > 0 && now.Sub(e.lastCriticalAddedAt) >= 1*time.Second {
		ce := e.criticalQueue[0]
		e.criticalQueue = e.criticalQueue[1:]

		// Final check: ensure this exact event isn't already in the stream (race prevention)
		isDup := false
		for _, existing := range e.CriticalStream {
			if existing.Anom == ce.Anom && existing.ASN == ce.ASN && existing.LeakerASN == ce.LeakerASN && existing.VictimASN == ce.VictimASN {
				isDup = true
				break
			}
		}
		if isDup {
			return
		}

		// Calculate height for animation
		boxW := 280.0
		fontSize := 18.0
		if e.Width > 2000 {
			boxW = 560.0
			fontSize = 36.0
		}
		height := e.calculateEventHeight(ce, boxW*1.1, fontSize)
		totalSpace := height + 25.0 // height + spacer

		// Shift down existing items by setting negative offset
		e.streamOffset -= totalSpace

		// Prepend to stream
		e.CriticalStream = append([]*CriticalEvent{ce}, e.CriticalStream...)
		e.lastCriticalAddedAt = now
		e.streamUpdatedAt = now
		e.streamDirty = true

		// Limit size
		if len(e.CriticalStream) > 100 {
			e.CriticalStream = e.CriticalStream[:100]
		}
	}
}

func (e *Engine) updateCriticalEventCacheStrs(ce *CriticalEvent) {
	ce.CachedTypeLabel = "[" + strings.ToUpper(ce.Anom) + "]"
	if e.subMonoFace != nil {
		ce.CachedTypeWidth, _ = text.Measure(ce.CachedTypeLabel, e.subMonoFace, 0)
	}

	if ce.Anom == nameHardOutage || ce.Anom == nameDDoSMitigation || ce.Anom == nameRouteLeak || ce.Anom == nameHijack {
		ce.CachedFirstLine = fmt.Sprintf(" %s IPs Impacted", utils.FormatNumber(ce.ImpactedIPs))
		if ce.Anom == nameHardOutage {
			e.cacheOutageStrings(ce)
		} else {
			e.cacheLeakStrings(ce)
			e.cacheOutageStrings(ce) // Now also used for Networks line in Leaks/DDoS
		}
	} else {
		ce.CachedFirstLine = ce.ASNStr
		if ce.Anom == nameRouteLeak && ce.LeakerASN != 0 {
			e.cacheLeakStrings(ce)
		}
	}

	if ce.ImpactedIPs > 0 && ce.Anom != nameHardOutage && ce.Anom != nameDDoSMitigation && ce.Anom != nameHijack {
		e.cacheImpactStrings(ce)
	}
}

func (e *Engine) cacheLeakStrings(ce *CriticalEvent) {
	if ce.Anom == nameRouteLeak {
		ce.CachedFirstLine = ce.LeakType.String()
	}

	leakerLabel := "  Leaker: "
	victimLabel := "  Impacted: "
	if ce.Anom == nameDDoSMitigation {
		leakerLabel = "  Provider: "
	}
	if ce.Anom == nameHijack {
		leakerLabel = "  Hijacker: "
		victimLabel = "  Victim: "
	}
	ce.CachedLeakerLabel = leakerLabel
	ce.CachedVictimLabel = victimLabel

	leakerVal := fmt.Sprintf("AS%d", ce.LeakerASN)
	if e.asnMapping != nil {
		if n := e.asnMapping.GetName(ce.LeakerASN); n != "" {
			leakerVal += " - " + n
		}
	}
	ce.CachedLeakerVal = leakerVal

	victimVal := "Unknown"
	if ce.VictimASN != 0 {
		victimVal = fmt.Sprintf("AS%d", ce.VictimASN)
		if e.asnMapping != nil {
			if n := e.asnMapping.GetName(ce.VictimASN); n != "" {
				victimVal += " - " + n
			}
		}
	}
	ce.CachedVictimVal = victimVal
}

func (e *Engine) cacheOutageStrings(ce *CriticalEvent) {
	// Impacted ASN line
	ce.CachedASNLabel = "  Impacted: "
	ce.CachedASNVal = ce.ASNStr

	// Networks line
	networks := make([]string, 0, len(ce.ImpactedPrefixes))
	for p := range ce.ImpactedPrefixes {
		networks = append(networks, p)
	}
	sort.Strings(networks)

	const maxShow = 2
	displayNets := networks
	moreCount := 0
	if len(networks) > maxShow {
		displayNets = networks[:maxShow]
		moreCount = len(networks) - maxShow
	}

	ce.CachedNetLabel = "  Networks: "
	netVal := strings.Join(displayNets, ", ")
	if moreCount > 0 {
		netVal += fmt.Sprintf(", (%d more)", moreCount)
	}
	ce.CachedNetVal = netVal

	// Locations line
	ce.CachedLocLabel = "  Location: "
	locs := strings.Split(ce.Locations, " | ")
	if len(locs) <= 2 {
		ce.CachedLocVal = ce.Locations
	} else {
		displayLocs := locs[:2]
		ce.CachedLocVal = fmt.Sprintf("%s | %s (%d more)", displayLocs[0], displayLocs[1], len(locs)-2)
	}
}

func (e *Engine) cacheImpactStrings(ce *CriticalEvent) {
	impactStr := ""
	switch {
	case ce.ImpactedIPs >= 1000000:
		impactStr = fmt.Sprintf("%.1fM IPs", float64(ce.ImpactedIPs)/1000000.0)
	case ce.ImpactedIPs >= 1000:
		impactStr = fmt.Sprintf("%.1fK IPs", float64(ce.ImpactedIPs)/1000.0)
	default:
		impactStr = fmt.Sprintf("%d IPs", ce.ImpactedIPs)
	}

	prefixes := make([]string, 0, len(ce.ImpactedPrefixes))
	for p := range ce.ImpactedPrefixes {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)

	pfxStr := ""
	if len(prefixes) > 0 {
		pfxStr = prefixes[0]
		if len(prefixes) > 1 {
			pfxStr += fmt.Sprintf(" (%d more)", len(prefixes)-1)
		}
	}

	ce.CachedImpactStr = fmt.Sprintf("  Impact(%s): %s", impactStr, pfxStr)
}

func (e *Engine) drawGlitchImage(screen, img *ebiten.Image, tx, ty, intensity float64, isGlitching bool) {
	if img == nil {
		return
	}
	op := &ebiten.DrawImageOptions{}
	if isGlitching && rand.Float64() < intensity {
		// More aggressive chromatic aberration
		offset := 8.0 * intensity
		jx := (rand.Float64() - 0.5) * 12.0 * intensity
		jy := (rand.Float64() - 0.5) * 4.0 * intensity

		op.GeoM.Reset()
		op.GeoM.Translate(tx+jx+offset, ty+jy)
		op.ColorScale.Reset()
		op.ColorScale.Scale(1, 0, 0, 0.6)
		screen.DrawImage(img, op)

		op.GeoM.Reset()
		op.GeoM.Translate(tx+jx-offset, ty+jy)
		op.ColorScale.Reset()
		op.ColorScale.Scale(0, 1, 1, 0.6)
		screen.DrawImage(img, op)

		// Occasional white flash
		if rand.Float64() < 0.2*intensity {
			op.GeoM.Reset()
			op.GeoM.Translate(tx+jx, ty+jy)
			op.ColorScale.Reset()
			op.ColorScale.Scale(1, 1, 1, 0.3)
			op.Blend = ebiten.BlendLighter
			screen.DrawImage(img, op)
		}
	}

	jx, jy := 0.0, 0.0
	alpha := float32(1.0)
	if isGlitching && rand.Float64() < intensity {
		jx = (rand.Float64() - 0.5) * 6.0 * intensity
		jy = (rand.Float64() - 0.5) * 3.0 * intensity
		alpha = float32(0.4 + rand.Float64()*0.6)
	}
	op.GeoM.Reset()
	op.GeoM.Translate(tx+jx, ty+jy)
	op.ColorScale.Reset()
	op.ColorScale.Scale(1, 1, 1, alpha)
	op.Blend = ebiten.BlendSourceOver
	screen.DrawImage(img, op)
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

func (e *Engine) incrementCityBuffer(lat, lng float64, c color.RGBA, shape EventShape) {
	if c == (color.RGBA{}) || (lat == 0 && lng == 0) {
		return
	}
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
	if b.Counts == nil {
		b.Counts = make(map[PulseKey]int)
	}
	b.Counts[PulseKey{Color: c, Shape: shape}]++
}

func (e *Engine) getClassificationVisuals(classificationType ClassificationType) (visualColor color.RGBA, classificationName string, shape EventShape) {
	switch classificationType {
	case ClassificationNone:
		return ColorDiscovery, nameDiscovery, ShapeCircle
	case ClassificationDiscovery:
		return ColorDiscovery, nameDiscovery, ShapeCircle
	case ClassificationTrafficEngineering:
		return ColorPolicy, nameTrafficEng, ShapeCircle
	case ClassificationPathHunting:
		return ColorPolicy, namePathHunting, ShapeCircle
	case ClassificationFlap:
		return ColorBad, nameFlap, ShapeCircle
	case ClassificationOutage:
		return ColorOutage, nameHardOutage, ShapeCircle
	case ClassificationRouteLeak:
		return ColorCritical, nameRouteLeak, ShapeCircle
	case ClassificationHijack:
		return ColorCritical, nameHijack, ShapeFlare
	case ClassificationBogon:
		return ColorCritical, nameBogon, ShapeFlare
	case ClassificationDDoSMitigation:
		return ColorDDoSMitigation, nameDDoSMitigation, ShapeSquare
	default:
		return color.RGBA{}, "", ShapeCircle
	}
}

func (e *Engine) GetPriority(name string) int {
	switch name {
	case nameRouteLeak, nameHardOutage, nameHijack:
		return 3 // Critical (Red)
	case nameFlap:
		return 2 // Bad (Orange)
	case nameTrafficEng, namePathHunting, nameDDoSMitigation:
		return 1 // Normalish (Purple)
	default:
		return 0 // Discovery (Blue)
	}
}

func (e *Engine) getClassificationUIColor(name string) color.RGBA {
	switch name {
	case nameRouteLeak, nameHardOutage, nameHijack:
		return ColorWithUI
	case nameFlap:
		return ColorBad // Already pretty bright
	case nameTrafficEng, namePathHunting, nameDDoSMitigation:
		return ColorUpdUI
	default:
		return ColorGossipUI
	}
}

func (e *Engine) updateWindowedMetrics(eventType EventType, classificationType ClassificationType, prefix string, asn uint32) {
	if cat, ok := utils.GetExcludedASNCategory(asn); ok {
		switch cat {
		case "DoD Honeypot":
			e.windowHoneypot++
		case "BGP Research":
			e.windowResearch++
		case "Security Scanner":
			e.windowSecurity++
		}
	}

	switch classificationType {
	case ClassificationFlap:
		e.windowFlap++
	case ClassificationTrafficEngineering:
		e.windowTE++
	case ClassificationPathHunting:
		e.windowHunting++
	case ClassificationOutage:
		e.windowOutage++
	case ClassificationRouteLeak:
	case ClassificationHijack:
		e.windowHijack++
	case ClassificationBogon:
		e.windowBogon++
		e.windowLeak++
	case ClassificationDDoSMitigation:
		e.windowDDoS++
	case ClassificationNone, ClassificationDiscovery:
		e.windowGlobal++
	}

	switch eventType {
	case EventNew:
		e.windowNew++
		if prefix != "" {
			e.bufferMu.Lock()
			e.seenBuffer[prefix] = asn
			e.bufferMu.Unlock()
		}
	case EventUpdate:
		e.windowUpd++
	case EventWithdrawal:
		e.windowWith++
	case EventGossip:
		e.windowGossip++
	}
}

func (e *Engine) InitAudio() {
	if e.AudioDir == "" {
		return
	}
	e.audioPlayer = NewAudioPlayer(e.AudioDir, nil, func(song, artist, extra string) {
		e.CurrentSong = song
		e.CurrentArtist = artist
		e.CurrentExtra = extra
		e.songChangedAt = time.Now()
		e.nowPlayingDirty = true
		e.songBuffer = nil
		e.artistBuffer = nil
		e.extraBuffer = nil
	})
}

func (e *Engine) SetAudioWriter(w io.Writer) {
	if e.audioPlayer != nil {
		e.audioPlayer.AudioWriter = w
	} else if e.AudioDir != "" {
		e.audioPlayer = NewAudioPlayer(e.AudioDir, w, func(song, artist, extra string) {
			e.CurrentSong = song
			e.CurrentArtist = artist
			e.CurrentExtra = extra
			e.songChangedAt = time.Now()
			e.nowPlayingDirty = true
			e.songBuffer = nil
			e.artistBuffer = nil
			e.extraBuffer = nil
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

func (e *Engine) InitFlareTexture() {
	size := 256
	e.flareImage = ebiten.NewImage(size, size)
	flarePixels := e.generateFlarePixels(size)
	e.flareImage.WritePixels(flarePixels)
}

func (e *Engine) InitSquareTexture() {
	size := 256
	e.squareImage = ebiten.NewImage(size, size)
	pixels := make([]byte, size*size*4)
	center, maxDist := float64(size)/2.0, float64(size)/2.0
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			dx, dy := math.Abs(float64(x)-center), math.Abs(float64(y)-center)
			dist := math.Max(dx, dy)
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
	e.squareImage.WritePixels(pixels)
}

func (e *Engine) calculateFlareBrightness(rdx, rdy, maxDist, rayThickness float64) float64 {
	dist := math.Sqrt(rdx*rdx + rdy*rdy)
	brightness := 0.0
	if dist < maxDist*0.15 {
		brightness = 1.0
	}
	if math.Abs(rdy) < rayThickness {
		rayIntensity := 1.0 - (math.Abs(rdx) / (maxDist * 1.2))
		if rayIntensity > 0 {
			edgeFalloff := 1.0 - (math.Abs(rdy) / rayThickness)
			brightness = math.Max(brightness, rayIntensity*edgeFalloff)
		}
	}
	if math.Abs(rdx) < rayThickness {
		rayIntensity := 1.0 - (math.Abs(rdy) / (maxDist * 1.2))
		if rayIntensity > 0 {
			edgeFalloff := 1.0 - (math.Abs(rdx) / rayThickness)
			brightness = math.Max(brightness, rayIntensity*edgeFalloff)
		}
	}
	diagDist1 := math.Abs(rdx-rdy) / math.Sqrt(2)
	diagDist2 := math.Abs(rdx+rdy) / math.Sqrt(2)
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
	if brightness > 1.0 {
		brightness = 1.0
	}
	return brightness
}

func (e *Engine) generateFlarePixels(size int) []byte {
	flarePixels := make([]byte, size*size*4)
	centerX, centerY := float64(size)/2.0, float64(size)/2.0
	rayThickness := float64(size) / 20.0
	rotationAngle := 15.0 * math.Pi / 180.0
	cosA, sinA := math.Cos(rotationAngle), math.Sin(rotationAngle)
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			fx, fy := float64(x), float64(y)
			dx, dy := fx-centerX, fy-centerY
			rdx := dx*cosA - dy*sinA
			rdy := dx*sinA + dy*cosA
			brightness := e.calculateFlareBrightness(rdx, rdy, centerX, rayThickness)
			if brightness > 0 {
				idx := (y*size + x) * 4
				flarePixels[idx+0] = uint8(brightness * 255)
				flarePixels[idx+1] = uint8(brightness * 255)
				flarePixels[idx+2] = uint8(brightness * 255)
				flarePixels[idx+3] = uint8(brightness * 255)
			}
		}
	}
	return flarePixels
}

func (e *Engine) InitTrendlineTexture() {
	e.trendLineImg = ebiten.NewImage(1, 1)
	e.trendLineImg.Fill(color.White)
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

func (e *Engine) drainCityBuffer() []QueuedPulse {
	e.bufferMu.Lock()
	defer e.bufferMu.Unlock()
	var nextBatch []QueuedPulse
	// 2. Convert buffered city activity into discrete pulse events for each color and shape
	for _, d := range e.cityBuffer {
		for pk, count := range d.Counts {
			if count > 0 {
				nextBatch = append(nextBatch, QueuedPulse{Lat: d.Lat, Lng: d.Lng, Color: pk.Color, Count: count, Shape: pk.Shape})
			}
		}
		// Reset and return to pool
		d.Counts = nil
		*d = BufferedCity{}
		e.cityBufferPool.Put(d)
	}
	// Clear the map after iteration to avoid concurrent modification
	e.cityBuffer = make(map[uint64]*BufferedCity)
	return nextBatch
}

func (e *Engine) scheduleVisualPulses(nextBatch []QueuedPulse) {
	// Shuffle the batch so events from different geographic locations are interleaved
	rand.Shuffle(len(nextBatch), func(i, j int) { nextBatch[i], nextBatch[j] = nextBatch[j], nextBatch[i] })

	// Spread the batch evenly across a longer window to smooth out bursts
	// We overlap batches to ensure a continuous flow (every 500ms we add a 1500ms batch)
	spreadWindow := 1500 * time.Millisecond
	spacing := spreadWindow / time.Duration(len(nextBatch))
	now := e.Now()

	// If we're too far behind (more than 1s), jump closer to 'now' but keep a small
	// buffer to avoid a hard gap in the visualization.
	if e.nextPulseEmittedAt.Before(now.Add(-1000 * time.Millisecond)) {
		e.nextPulseEmittedAt = now.Add(-500 * time.Millisecond)
	}

	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	// Cap the visual backlog to prevent memory exhaustion during massive BGP spikes
	maxQueueSize := MaxVisualQueueSize
	currentSize := len(e.visualQueue)

	if currentSize < maxQueueSize {
		if currentSize+len(nextBatch) > maxQueueSize {
			dropped := (currentSize + len(nextBatch)) - maxQueueSize
			e.droppedQueue.Add(uint64(dropped))
			log.Printf("Truncating batch of %d pulses to fit queue (Current: %d, Max: %d)", len(nextBatch), currentSize, maxQueueSize)
			nextBatch = nextBatch[:maxQueueSize-currentSize]
			if len(nextBatch) > 0 {
				spacing = spreadWindow / time.Duration(len(nextBatch))
			}
		}

		for i, p := range nextBatch {
			// Schedule the pulse to be processed by the Update() loop at a specific time
			p.ScheduledTime = e.nextPulseEmittedAt.Add(time.Duration(i) * spacing)
			e.visualQueue = append(e.visualQueue, p)
		}
	} else {
		e.droppedQueue.Add(uint64(len(nextBatch)))
		log.Printf("Dropping batch of %d pulses (Queue size: %d)", len(nextBatch), len(e.visualQueue))
	}

	// Advance the next emission baseline by 500ms (the ticker interval),
	// capping the visual backlog to 3 seconds to prevent falling too far behind.
	e.nextPulseEmittedAt = e.nextPulseEmittedAt.Add(500 * time.Millisecond)
	if e.nextPulseEmittedAt.After(now.Add(3 * time.Second)) {
		e.nextPulseEmittedAt = now.Add(3 * time.Second)
	}
}

func (e *Engine) Stop() {
	if e.VideoWriter != nil {
		log.Printf("Closing video writer and finalizing video...")
		if err := e.VideoWriter.Close(); err != nil {
			log.Printf("Error closing video writer: %v", err)
		}
		if e.VideoCmd != nil {
			if err := e.VideoCmd.Wait(); err != nil {
				log.Printf("Error waiting for video encoder: %v", err)
			}
		}
		log.Printf("Video generation complete.")
		e.VideoWriter = nil
	}

	if e.audioPlayer != nil {
		e.audioPlayer.Shutdown()
	}
	if e.processor != nil {
		e.processor.Close()
	}
	if e.SeenDB != nil {
		_ = e.SeenDB.Close()
	}
	if e.StateDB != nil {
		_ = e.StateDB.Close()
	}
	if e.geo != nil {
		_ = e.geo.Close()
	}
}
