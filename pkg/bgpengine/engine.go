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
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/audio"
	"github.com/hajimehoshi/ebiten/v2/text/v2"
	"github.com/oschwald/maxminddb-golang"
	geojson "github.com/paulmach/go.geojson"
	"github.com/sudorandom/bgp-stream/pkg/utils"
	"golang.org/x/image/font/gofont/gomono"
	"golang.org/x/image/font/gofont/goregular"
)

type Location []interface{}

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
}

type QueuedPulse struct {
	Lat, Lng      float64
	Type          string
	Count         int
	ScheduledTime time.Time
}

type BufferedCity struct {
	Lat, Lng              float64
	New, Upd, With, Gossip int
}

var (
	ColorGossip = color.RGBA{0, 191, 255, 255}  // Deep Sky Blue (Propagation)
	ColorNew    = color.RGBA{255, 255, 255, 255} // White (New Announcement)
	ColorUpd    = color.RGBA{148, 0, 211, 255}   // Deep Violet/Purple (Path Change)
	ColorWith   = color.RGBA{255, 50, 50, 255}   // Red (Withdrawal)

	// Lighter versions for UI text and trendlines
	ColorGossipUI = color.RGBA{135, 206, 250, 255} // Light Sky Blue
	ColorNewUI    = color.RGBA{255, 255, 255, 255} // White
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

	pulses            []*Pulse
	pulsesMu          sync.Mutex
	prefixData        PrefixData
	countryHubs       map[string][]CityHub
	prefixToCityCache map[uint32]cacheEntry
	cacheMu           sync.Mutex

	cityBuffer         map[uint64]*BufferedCity
	cityBufferPool     sync.Pool
	bufferMu           sync.Mutex
	visualQueue        []*QueuedPulse
	queueMu            sync.Mutex
	nextPulseEmittedAt time.Time

	bgImage    *ebiten.Image
	pulseImage *ebiten.Image
	fontSource *text.GoTextFaceSource
	monoSource *text.GoTextFaceSource

	// Metrics (Windowed for Rate calculation)
	windowNew, windowUpd, windowWith, windowGossip int64
	windowNote, windowPeer, windowOpen             int64

	rateNew, rateUpd, rateWith, rateGossip float64
	rateNote, ratePeer, rateOpen float64

	countryActivity map[string]int
	topHubs         []struct {
		CC   string
		Rate float64
	}

	// History for trendlines (last 60 snapshots, 2s each = 2 mins)
	history   []MetricSnapshot
	metricsMu sync.Mutex

	audioContext *audio.Context
	AudioWriter  io.Writer

	CurrentSong   string
	NextSong      string
	CurrentArtist string
	NextArtist    string
	lastSong      string
	songChangedAt time.Time

	hubChangedAt      map[string]time.Time
	lastHubs          map[string]int
	hubPosition       map[string]int
	lastMetricsUpdate time.Time

	VisualHubs map[string]*VisualHub

	recentlySeen map[uint32]struct {
		Time time.Time
		Type string
	}

	SeenDB    *utils.DiskTrie
	CloudTrie *utils.CloudTrie

	cityCoords map[string][2]float32
}

type VisualHub struct {
	CC          string
	Rate        float64
	DisplayY    float64
	TargetY     float64
	Alpha       float32
	TargetAlpha float32
	Active      bool
}

type MetricSnapshot struct {
	New, Upd, With, Gossip, Note, Peer, Open int
}

func NewEngine(width, height int, scale float64) *Engine {
	s, err := text.NewGoTextFaceSource(bytes.NewReader(goregular.TTF))
	if err != nil {
		log.Printf("Fatal: failed to load regular font: %v", err)
	}
	m, err := text.NewGoTextFaceSource(bytes.NewReader(gomono.TTF))
	if err != nil {
		log.Printf("Fatal: failed to load mono font: %v", err)
	}

	return &Engine{
		Width:             width,
		Height:            height,
		FPS:               30,
		Scale:             scale,
		countryHubs:       make(map[string][]CityHub),
		prefixToCityCache: make(map[uint32]cacheEntry),
		cityBuffer:        make(map[uint64]*BufferedCity),
		cityBufferPool: sync.Pool{
			New: func() interface{} {
				return &BufferedCity{}
			},
		},
		nextPulseEmittedAt: time.Now(),
		fontSource:         s,
		monoSource:         m,
		countryActivity:    make(map[string]int),
		history:            make([]MetricSnapshot, 60),
		hubChangedAt:       make(map[string]time.Time),
		lastHubs:           make(map[string]int),
		hubPosition:        make(map[string]int),
		lastMetricsUpdate:  time.Now(),
		VisualHubs:         make(map[string]*VisualHub),
		recentlySeen: make(map[uint32]struct {
			Time time.Time
			Type string
		}),
		cityCoords: make(map[string][2]float32),
	}
}

func (e *Engine) StartMemoryWatcher() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for range ticker.C {
			debug.FreeOSMemory()
		}
	}()
}

func (e *Engine) InitAudio() {
	if e.audioContext == nil {
		e.audioContext = audio.NewContext(44100)
	}
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
			var c color.RGBA
			switch p.Type {
			case "new":
				c = ColorNew
			case "upd":
				c = ColorUpd
			case "with":
				c = ColorWith
			case "gossip":
				c = ColorGossip
			}
			e.AddPulse(p.Lat, p.Lng, c, p.Count)
		}
	}
	e.queueMu.Unlock()

	e.metricsMu.Lock()
	for cc, vh := range e.VisualHubs {
		// Smoothly interpolate Y position
		vh.DisplayY += (vh.TargetY - vh.DisplayY) * 0.2
		
		// Interpolate Alpha
		vh.Alpha += (vh.TargetAlpha - vh.Alpha) * 0.2

		// Cleanup inactive or invisible hubs instantly
		if !vh.Active || vh.Alpha < 0.01 {
			delete(e.VisualHubs, cc)
		}
	}
	e.metricsMu.Unlock()

	e.pulsesMu.Lock()
	active := e.pulses[:0]
	for _, p := range e.pulses {
		if now.Sub(p.StartTime) < 1500*time.Millisecond {
			active = append(active, p)
		}
	}
	e.pulses = active
	e.pulsesMu.Unlock()
	return nil
}

func (e *Engine) drawGlitchTextAggressive(screen *ebiten.Image, label string, face *text.GoTextFace, tx, ty float64, baseAlpha float32, intensity float64, isGlitching bool) {
	fontSize := face.Size
	if isGlitching && rand.Float64() < intensity {
		offset := 4.0 * intensity
		jx := (rand.Float64() - 0.5) * fontSize * intensity
		jy := (rand.Float64() - 0.5) * (fontSize / 2) * intensity

		ro := &text.DrawOptions{}
		ro.GeoM.Translate(tx+jx+offset, ty+jy)
		ro.ColorScale.Scale(1, 0, 0, baseAlpha*0.7)
		text.Draw(screen, label, face, ro)

		co := &text.DrawOptions{}
		co.GeoM.Translate(tx+jx-offset, ty+jy)
		co.ColorScale.Scale(0, 1, 1, baseAlpha*0.7)
		text.Draw(screen, label, face, co)
	}

	op := &text.DrawOptions{}
	jx, jy := 0.0, 0.0
	alpha := baseAlpha
	if isGlitching && rand.Float64() < intensity {
		jx = (rand.Float64() - 0.5) * (fontSize / 2) * intensity
		jy = (rand.Float64() - 0.5) * (fontSize / 4) * intensity
		alpha = float32((0.2 + rand.Float64()*0.8) * float64(baseAlpha))
	}
	op.GeoM.Translate(tx+jx, ty+jy)
	op.ColorScale.Scale(1, 1, 1, float32(alpha))
	text.Draw(screen, label, face, op)
}

func (e *Engine) drawGlitchTextSubtle(screen *ebiten.Image, label string, face *text.GoTextFace, tx, ty float64, baseAlpha float32, intensity float64, isGlitching bool) {
	fontSize := face.Size
	if isGlitching && rand.Float64() < intensity {
		// Even subtler chromatic aberration for hubs
		offset := 1.0 * intensity
		jx := (rand.Float64() - 0.5) * (fontSize / 4) * intensity
		jy := (rand.Float64() - 0.5) * (fontSize / 8) * intensity

		ro := &text.DrawOptions{}
		ro.GeoM.Translate(tx+jx+offset, ty+jy)
		ro.ColorScale.Scale(1, 0, 0, baseAlpha*0.5)
		text.Draw(screen, label, face, ro)

		co := &text.DrawOptions{}
		co.GeoM.Translate(tx+jx-offset, ty+jy)
		co.ColorScale.Scale(0, 1, 1, baseAlpha*0.5)
		text.Draw(screen, label, face, co)
	}

	op := &text.DrawOptions{}
	jx, jy := 0.0, 0.0
	alpha := baseAlpha
	if isGlitching && rand.Float64() < intensity {
		jx = (rand.Float64() - 0.5) * (fontSize / 4) * intensity
		jy = (rand.Float64() - 0.5) * (fontSize / 8) * intensity
		alpha = float32((0.4 + rand.Float64()*0.6) * float64(baseAlpha))
	}
	op.GeoM.Translate(tx+jx, ty+jy)
	op.ColorScale.Scale(1, 1, 1, float32(alpha))
	text.Draw(screen, label, face, op)
}

func (e *Engine) Draw(screen *ebiten.Image) {
	screen.DrawImage(e.bgImage, nil)
	e.pulsesMu.Lock()
	now := time.Now()
	op := &ebiten.DrawImageOptions{}
	op.Blend = ebiten.BlendLighter
	imgW, _ := e.pulseImage.Bounds().Dx(), e.pulseImage.Bounds().Dy()
	halfW := float64(imgW) / 2
	for _, p := range e.pulses {
		elapsed := now.Sub(p.StartTime).Seconds()
		progress := elapsed / 1.5
		if progress > 1.0 {
			continue
		}

		baseAlpha := 0.6
		if p.Color == ColorNew {
			baseAlpha = 0.3
		}
		if p.Color == ColorGossip {
			baseAlpha = 0.2
		}

		scale := (3 + progress*p.MaxRadius) / float64(imgW) * 2.0
		alpha := (1.0 - progress) * baseAlpha
		op.GeoM.Reset()
		op.GeoM.Translate(-halfW, -halfW)
		op.GeoM.Scale(scale, scale)
		op.GeoM.Translate(p.X, p.Y)
		r, g, b := float64(p.Color.R)/255.0, float64(p.Color.G)/255.0, float64(p.Color.B)/255.0
		op.ColorScale.Reset()
		op.ColorScale.Scale(float32(r*alpha), float32(g*alpha), float32(b*alpha), float32(alpha))
		screen.DrawImage(e.pulseImage, op)
	}
	e.pulsesMu.Unlock()

	e.drawMetrics(screen)
	e.drawSong(screen)
}

func (e *Engine) drawSong(screen *ebiten.Image) {
	if e.fontSource == nil || e.CurrentSong == "" {
		return
	}

	fontSize, margin := 20.0, 100.0
	if e.Width > 2000 {
		fontSize, margin = 40.0, 200.0
	}

	now := time.Now()
	glitchDuration := 2 * time.Second
	isGlitching := now.Sub(e.songChangedAt) < glitchDuration
	intensity := 0.0
	if isGlitching {
		intensity = 1.0 - (now.Sub(e.songChangedAt).Seconds() / glitchDuration.Seconds())
	}

	titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize}
	artistFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.7}
	titleLabel := ">> " + e.CurrentSong
	artistLabel := e.CurrentArtist

	x, y := margin/2, float64(e.Height)-margin*1.5

	e.drawGlitchTextAggressive(screen, titleLabel, titleFace, x, y, 0.8, intensity, isGlitching)
	if artistLabel != "" {
		e.drawGlitchTextAggressive(screen, artistLabel, artistFace, x+fontSize*1.6, y+fontSize*1.2, 0.5, intensity, isGlitching)
	}
}

func (e *Engine) Layout(w, h int) (int, int) { return e.Width, e.Height }

func (e *Engine) InitPulseTexture() {
	size := 128
	if e.Width > 2000 {
		size = 256
	}
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

func (e *Engine) LoadData() error {
	if err := os.MkdirAll("data", 0755); err != nil {
		log.Printf("Warning: Failed to create data directory: %v", err)
	}

	// 1. Download worldcities.csv if missing
	citiesPath := "data/worldcities.csv"
	if _, err := os.Stat(citiesPath); os.IsNotExist(err) {
		url := os.Getenv("WORLD_CITIES_URL")
		if url == "" {
			url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/cities.csv"
		}
		log.Printf("Downloading world cities database from %s...", url)
		if err := utils.DownloadFile(url, citiesPath); err != nil {
			log.Printf("Error downloading cities: %v", err)
		}
	}

	// 2. Load cloud data from sources of truth
	if err := e.loadCloudData(); err != nil {
		log.Printf("Warning: Failed to load cloud data: %v", err)
	}

	var err error
	e.SeenDB, err = utils.OpenDiskTrie("data/seen-prefixes.db")
	if err != nil {
		log.Printf("Warning: Failed to open seen prefixes database: %v. Persistent state will be disabled.", err)
	}

	if err := e.loadPrefixData(); err != nil {
		return err
	}
	if err := e.loadRemoteCityData(); err != nil {
		return err
	}
	return e.generateBackground()
}

func (e *Engine) loadRemoteCityData() error {
	resp, err := http.Get("https://map.kmcd.dev/data/city-dominance/meta.json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var meta struct {
		MaxYear int `json:"max_year"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return err
	}
	resp, err = http.Get(fmt.Sprintf("https://map.kmcd.dev/data/city-dominance/%d.json", meta.MaxYear))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var cities []struct {
		Country             string
		Coordinates         []float64
		LogicalDominanceIPs float64 `json:"logical_dominance_ips"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cities); err != nil {
		return err
	}
	for _, c := range cities {
		hubs := e.countryHubs[c.Country]
		weight := c.LogicalDominanceIPs
		if weight <= 0 {
			weight = 1
		}
		last := 0.0
		if len(hubs) > 0 {
			last = hubs[len(hubs)-1].CumulativeWeight
		}
		e.countryHubs[c.Country] = append(hubs, CityHub{Lat: c.Coordinates[1], Lng: c.Coordinates[0], CumulativeWeight: last + weight})
	}
	return nil
}

func (e *Engine) loadCloudData() error {
	var allPrefixes []utils.CloudPrefix

	// 1. Google Cloud (Geofeed - Source of Truth)
	log.Println("Fetching Google Cloud Geofeed...")
	goog, err := utils.FetchGoogleGeofeed()
	if err == nil {
		allPrefixes = append(allPrefixes, goog...)
	} else {
		log.Printf("Warning: Failed to fetch GCP geofeed: %v", err)
	}

	// 2. AWS IP Ranges
	log.Println("Fetching AWS IP Ranges...")
	resp, err := http.Get("https://ip-ranges.amazonaws.com/ip-ranges.json")
	if err == nil {
		defer resp.Body.Close()
		aws, err := utils.ParseAWSRanges(resp.Body)
		if err == nil {
			allPrefixes = append(allPrefixes, aws...)
		}
	} else {
		log.Printf("Warning: Failed to fetch AWS ranges: %v", err)
	}

	if len(allPrefixes) > 0 {
		e.CloudTrie = utils.NewCloudTrie(allPrefixes)
		log.Printf("Loaded %d cloud prefixes into CloudTrie", len(allPrefixes))
	}

	return nil
}

func (e *Engine) generateBackground() error {
	cpuImg := image.NewRGBA(image.Rect(0, 0, e.Width, e.Height))
	draw.Draw(cpuImg, cpuImg.Bounds(), &image.Uniform{color.RGBA{8, 10, 15, 255}}, image.Point{}, draw.Src)

	// Draw subtle latitude/longitude grid (Cyber-grid)
	gridColor := color.RGBA{30, 35, 45, 255}
	// Longitude lines
	for lng := -180.0; lng <= 180.0; lng += 15.0 {
		var points [][]float64
		for lat := -90.0; lat <= 90.0; lat += 2.0 {
			points = append(points, []float64{lng, lat})
		}
		e.drawRingFast(cpuImg, points, gridColor)
	}
	// Latitude lines
	for lat := -90.0; lat <= 90.0; lat += 15.0 {
		var points [][]float64
		for lng := -180.0; lng <= 180.0; lng += 2.0 {
			points = append(points, []float64{lng, lat})
		}
		e.drawRingFast(cpuImg, points, gridColor)
	}

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
	e.bgImage = ebiten.NewImageFromImage(cpuImg)
	return nil
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
		if err := json.Unmarshal(data, &e.prefixData); err == nil {
			debug.FreeOSMemory()
			return nil
		}
	}

	// Try to load world cities from disk, fallback to embed
	var citiesReader io.Reader
	if f, err := os.Open("data/worldcities.csv"); err == nil {
		defer f.Close()
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
				e.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(rec[1]), strings.ToUpper(rec[5]))] = [2]float32{float32(lat), float32(lng)}
			} else if len(rec) >= 10 {
				// Supported Format 2 (dr5hn): id, name, state_id, state_code, state_name, country_id, country_code, country_name, latitude, longitude, wikiDataId
				lat, _ := strconv.ParseFloat(rec[8], 64)
				lng, _ := strconv.ParseFloat(rec[9], 64)
				e.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(rec[1]), strings.ToUpper(rec[6]))] = [2]float32{float32(lat), float32(lng)}
			}
		}
	} else {
		log.Println("Warning: No world cities data available.")
	}

	// Try to load geoip DB from disk, fallback to embed
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
		return fmt.Errorf("no GeoIP database available (ipinfo_lite.mmdb)")
	}
	defer geoReader.Close()
	var mu sync.Mutex
	var allRanges []ipRange
	var wg sync.WaitGroup
	handler := func(start, end uint32, city, cc string, lat, lng float32, priority int) {
		if lat == 0 && lng == 0 {
			if city != "" {
				if c, ok := e.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(city), strings.ToUpper(cc))]; ok {
					lat, lng = c[0], c[1]
				}
			}
			if lat == 0 {
				var record struct {
					City struct {
						Names map[string]string `maxminddb:"names"`
					} `maxminddb:"city"`
				}
				ip := make(net.IP, 4)
				binary.BigEndian.PutUint32(ip, start)
				if err := geoReader.Lookup(ip, &record); err == nil {
					cityName := record.City.Names["en"]
					if c, ok := e.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(cityName), strings.ToUpper(cc))]; ok {
						lat, lng = c[0], c[1]
						city = cityName
					}
				}
			}
		}
		if cc != "" {
			mu.Lock()
			allRanges = append(allRanges, ipRange{Start: start, End: end, City: city, CC: cc, Lat: lat, Lng: lng, Priority: priority})
			mu.Unlock()
		}
	}
	rirNames := []string{"APNIC", "RIPE", "AFRINIC", "LACNIC", "ARIN"}
	urls := map[string]string{
		"APNIC":   "https://ftp.apnic.net/stats/apnic/delegated-apnic-latest",
		"RIPE":    "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-latest",
		"AFRINIC": "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-latest",
		"LACNIC":  "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-latest",
		"ARIN":    "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
	}
	for _, name := range rirNames {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			r, err := utils.GetCachedReader(urls[n], true, "[RIR-"+n+"]")
			if err != nil {
				log.Printf("[RIR-%s] Error fetching data: %v", n, err)
				return
			}
			defer r.Close()

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
					// Priority based on mask size (shorter mask = lower priority)
					p := 32
					for c := uint32(count); c > 1; c >>= 1 {
						p--
					}
					handler(start, start+uint32(count)-1, "", strings.ToUpper(parts[1]), 0, 0, p)
				}
			}
		}(name)
	}
	wg.Wait()

	// STAGE 1: Proper Flattening (Sweep-line)
	type event struct {
		pos   uint32
		isEnd bool
		r     *ipRange
	}
	events := make([]event, 0, len(allRanges)*2)
	for i := range allRanges {
		events = append(events, event{allRanges[i].Start, false, &allRanges[i]})
		events = append(events, event{allRanges[i].End, true, &allRanges[i]})
	}
	allRanges = nil // Clear early to free memory
	sort.Slice(events, func(i, j int) bool {
		if events[i].pos != events[j].pos {
			return events[i].pos < events[j].pos
		}
		return !events[i].isEnd
	})

	var segments []struct {
		start, end uint32
		r          *ipRange
	}
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
			segments = append(segments, struct {
				start, end uint32
				r          *ipRange
			}{lastPos, pos - 1, best})
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
	events = nil // Clear events slice as it is no longer needed

	// STAGE 2: Indexing
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
	e.prefixData = PrefixData{L: locations, R: flatRanges}
	if err := os.MkdirAll("data", 0755); err != nil {
		log.Printf("Warning: Failed to create data directory: %v", err)
	}
	if f, err := os.Create(cachePath); err == nil {
		if err := json.NewEncoder(f).Encode(e.prefixData); err != nil {
			log.Printf("Warning: Failed to encode prefix cache: %v", err)
		}
		f.Close()
	}
	debug.FreeOSMemory()
	return nil
}

func (e *Engine) ListenToBGP() {
	url := "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream"
	pendingWithdrawals := make(map[uint32]time.Time)
	var mu sync.Mutex

	// De-duplication window: ignore redundant updates for the same prefix within 15 seconds
	const dedupeWindow = 15 * time.Second
	// Withdrawal resolution window: wait this long to see if an announcement follows a withdrawal
	const withdrawResolutionWindow = 10 * time.Second

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		ticks := 0
		for range ticker.C {
			now := time.Now()
			mu.Lock()
			
			// 1. Process pending withdrawals
			for ip, t := range pendingWithdrawals {
				if now.After(t) {
					if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
						e.recordEvent(lat, lng, cc, "with")
						e.recentlySeen[ip] = struct {
							Time time.Time
							Type string
						}{Time: now, Type: "with"}
					}
					delete(pendingWithdrawals, ip)
				}
			}

			// 2. Periodically clean up de-duplication cache (every 30s)
			ticks++
			if ticks >= 30 {
				ticks = 0
				if len(e.recentlySeen) > 500000 {
					for ip, entry := range e.recentlySeen {
						if now.Sub(entry.Time) > 5*time.Minute {
							delete(e.recentlySeen, ip)
						}
					}
				}
			}
			mu.Unlock()
		}
	}()

	backoff := 1 * time.Second
	for {
		log.Printf("Connecting to RIS Live: %s", url)
		c, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("Dial error: %v. Retrying in %v...", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			continue
		}
		backoff = 1 * time.Second

		subscribeMsg := `{"type": "ris_subscribe", "data": {"type": "UPDATE"}}`
		if err := c.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
			log.Printf("Subscribe error: %v", err)
			c.Close()
			continue
		}

		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v. Reconnecting...", err)
				break
			}
			var msg struct {
				Type string `json:"type"`
				Data struct {
					Announcements []struct {
						Prefixes []string `json:"prefixes"`
					} `json:"announcements"`
					Withdrawals []string `json:"withdrawals"`
					Peer        string   `json:"peer"`
				} `json:"data"`
			}
			if json.Unmarshal(message, &msg) != nil {
				continue
			}

			now := time.Now()
			switch msg.Type {
			case "ris_error":
				log.Printf("[RIS ERROR] %s", string(message))
			case "ris_message":
				mu.Lock()

				// 1. Process Withdrawals
				for _, prefix := range msg.Data.Withdrawals {
					ip := e.prefixToIP(prefix)
					if ip == 0 {
						continue
					}

					// If we've seen a WITHDRAWAL for this prefix very recently, it's gossip
					if last, ok := e.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == "with" {
						if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
							e.recordEvent(lat, lng, cc, "gossip")
						}
						continue
					}

					pendingWithdrawals[ip] = now.Add(withdrawResolutionWindow)
				}

				// 2. Process Announcements
				for _, ann := range msg.Data.Announcements {
					for _, prefix := range ann.Prefixes {
						ip := e.prefixToIP(prefix)
						if ip == 0 {
							continue
						}

						// Retroactive Path Change Detection:
						// If we see an announcement and the last thing we recorded was a withdrawal
						// within the dedupe window, this is actually a Path Change that arrived late.
						if last, ok := e.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == "with" {
							if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
								e.recordEvent(lat, lng, cc, "upd")
								e.recentlySeen[ip] = struct {
									Time time.Time
									Type string
								}{Time: now, Type: "upd"}
							}
							continue
						}

						// Normal Gossip Detection
						if last, ok := e.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && (last.Type == "new" || last.Type == "upd" || last.Type == "gossip") {
							if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
								e.recordEvent(lat, lng, cc, "gossip")
							}
							continue
						}

						if _, ok := pendingWithdrawals[ip]; ok {
							// Found a matching announcement for a pending withdrawal: this is a Path Change
							delete(pendingWithdrawals, ip)
							if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
								e.recordEvent(lat, lng, cc, "upd")
								e.recentlySeen[ip] = struct {
									Time time.Time
									Type string
								}{Time: now, Type: "upd"}
							}
						} else {
							// Check if we've EVER seen this prefix before (across sessions)
							isNew := true
							if e.SeenDB != nil {
								// We use the full CIDR string as the key for exact match
								if val, _ := e.SeenDB.Get(prefix); val != nil {
									isNew = false
								}
							}

							if isNew {
								// Truly new announcement (Discovery)
								if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
									e.recordEvent(lat, lng, cc, "new")
									e.recentlySeen[ip] = struct {
										Time time.Time
										Type string
									}{Time: now, Type: "new"}
									
									// Record that we've seen it now
									if e.SeenDB != nil {
										if err := e.SeenDB.BatchInsertRaw(map[string][]byte{prefix: []byte{1}}); err != nil {
											log.Printf("Warning: Failed to update seen database: %v", err)
										}
									}
								}
							} else {
								// We've seen this before, so this is just a path change (Re-discovery)
								if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
									e.recordEvent(lat, lng, cc, "upd")
									e.recentlySeen[ip] = struct {
										Time time.Time
										Type string
									}{Time: now, Type: "upd"}
								}
							}
						}
					}
				}
				mu.Unlock()
			}
		}
		c.Close()
		time.Sleep(time.Second)
	}
}

// StartBufferLoop runs a background loop that periodically processes buffered BGP events.
// It aggregates high-frequency events into batches, shuffles them to prevent visual
// clustering, and paces their release into the visual queue to ensure smooth animations.
func (e *Engine) StartBufferLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		e.bufferMu.Lock()
		var nextBatch []*QueuedPulse
		// Convert buffered city activity into discrete pulse events for each type
		for key, d := range e.cityBuffer {
			if d.With > 0 {
				nextBatch = append(nextBatch, &QueuedPulse{Lat: d.Lat, Lng: d.Lng, Type: "with", Count: d.With})
			}
			if d.Upd > 0 {
				nextBatch = append(nextBatch, &QueuedPulse{Lat: d.Lat, Lng: d.Lng, Type: "upd", Count: d.Upd})
			}
			if d.New > 0 {
				nextBatch = append(nextBatch, &QueuedPulse{Lat: d.Lat, Lng: d.Lng, Type: "new", Count: d.New})
			}
			if d.Gossip > 0 {
				nextBatch = append(nextBatch, &QueuedPulse{Lat: d.Lat, Lng: d.Lng, Type: "gossip", Count: d.Gossip})
			}
			// Reset and return to pool
			*d = BufferedCity{}
			e.cityBufferPool.Put(d)
			delete(e.cityBuffer, key)
		}
		e.bufferMu.Unlock()

		if len(nextBatch) == 0 {
			continue
		}

		// Shuffle the batch so events from different geographic locations are interleaved
		rand.Shuffle(len(nextBatch), func(i, j int) { nextBatch[i], nextBatch[j] = nextBatch[j], nextBatch[i] })

		// Spread the batch evenly across the next 500ms interval
		spacing := 500 * time.Millisecond / time.Duration(len(nextBatch))
		now := time.Now()
		if e.nextPulseEmittedAt.Before(now) {
			e.nextPulseEmittedAt = now
		}

		e.queueMu.Lock()
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
		e.queueMu.Unlock()
	}
}

func (e *Engine) recordEvent(lat, lng float64, cc, eventType string) {
	// Use bit manipulation to create a unique uint64 key for cityBuffer
	// This avoids expensive fmt.Sprintf allocations on every BGP event
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

	e.metricsMu.Lock()
	if cc != "" {
		e.countryActivity[cc]++
	}
	switch eventType {
	case "new":
		b.New++
		e.windowNew++
	case "upd":
		b.Upd++
		e.windowUpd++
	case "with":
		b.With++
		e.windowWith++
	case "gossip":
		b.Gossip++
		e.windowGossip++
	}
	e.metricsMu.Unlock()
}

func (e *Engine) getPrefixCoords(ip uint32) (float64, float64, string) {
	e.cacheMu.Lock()
	if c, ok := e.prefixToCityCache[ip]; ok {
		e.cacheMu.Unlock()
		return c.Lat, c.Lng, c.CC
	}
	e.cacheMu.Unlock()

	var lat, lng float64
	var cc, city string

	// 1. Check CloudTrie first (highest precision for cloud IP blocks)
	if e.CloudTrie != nil {
		ipObj := make(net.IP, 4)
		binary.BigEndian.PutUint32(ipObj, ip)
		if loc, ok := e.CloudTrie.Lookup(ipObj); ok {
			// loc is "City|CC"
			parts := strings.Split(loc, "|")
			if len(parts) == 2 {
				city, cc = parts[0], parts[1]
				lat, lng, cc = e.resolveCityToCoords(city, cc)
			}
		}
	}

	// 2. Fallback to generic GeoIP if not a cloud IP or cloud resolution failed
	if lat == 0 && lng == 0 {
		loc := e.lookupIP(ip)
		if loc != nil {
			lat, _ = loc[0].(float64)
			lng, _ = loc[1].(float64)
			cc, _ = loc[2].(string)
			city, _ = loc[3].(string)
			
			// If lookup gave us a city but no coords, try to resolve it
			if lat == 0 && lng == 0 && city != "" {
				lat, lng, cc = e.resolveCityToCoords(city, cc)
			}
		}
	}

	// 3. Final Fallback: Country Hubs (for IP blocks known only by Country)
	if lat == 0 && lng == 0 && cc != "" {
		hubs := e.countryHubs[cc]
		if len(hubs) > 0 {
			r := rand.Float64() * hubs[len(hubs)-1].CumulativeWeight
			for _, h := range hubs {
				if h.CumulativeWeight >= r {
					lat, lng = h.Lat, h.Lng
					break
				}
			}
		}
	}

	if lat == 0 && lng == 0 {
		return 0, 0, ""
	}

	// Cache the resolved coordinates
	e.cacheMu.Lock()
	if len(e.prefixToCityCache) > 100000 {
		count := 0
		for k := range e.prefixToCityCache {
			delete(e.prefixToCityCache, k)
			count++
			if count > 20000 {
				break
			}
		}
	}
	e.prefixToCityCache[ip] = cacheEntry{Lat: lat, Lng: lng, CC: cc}
	e.cacheMu.Unlock()

	return lat, lng, cc
}

func (e *Engine) resolveCityToCoords(city, cc string) (float64, float64, string) {
	if c, ok := e.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(city), strings.ToUpper(cc))]; ok {
		return float64(c[0]), float64(c[1]), cc
	}
	return 0, 0, cc
}

func (e *Engine) lookupIP(ip uint32) Location {
	r := e.prefixData.R
	low, high := 0, (len(r)/2)-1
	for low <= high {
		mid := (low + high) / 2
		startIP := r[mid*2]
		nextStartIP := uint32(0xFFFFFFFF)
		if mid+1 < len(r)/2 {
			nextStartIP = r[(mid+1)*2]
		}
		if ip >= startIP && ip < nextStartIP {
			locIdx := r[mid*2+1]
			if locIdx == 4294967295 {
				return nil
			}
			return e.prefixData.L[locIdx]
		}
		if startIP < ip {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return nil
}

func (e *Engine) project(lat, lng float64) (x, y float64) {
	// Clamp latitude to avoid singularity at poles (+/- 90 degrees)
	if lat > 89.5 {
		lat = 89.5
	}
	if lat < -89.5 {
		lat = -89.5
	}

	latRad, lngRad := lat*math.Pi/180, lng*math.Pi/180
	theta := latRad
	for i := 0; i < 10; i++ {
		denom := 2 + 2*math.Cos(2*theta)
		if math.Abs(denom) < 1e-9 {
			break
		}
		delta := (2*theta + math.Sin(2*theta) - math.Pi*math.Sin(latRad)) / denom
		theta -= delta
		if math.Abs(delta) < 1e-7 {
			break
		}
	}
	r := e.Scale
	x = (float64(e.Width) / 2) + r*(2*math.Sqrt(2)/math.Pi)*lngRad*math.Cos(theta)
	y = (float64(e.Height) / 2) - r*math.Sqrt(2)*math.Sin(theta)
	return x, y
}

func (e *Engine) fillPolygon(img *image.RGBA, rings [][][]float64, c color.RGBA) {
	if len(rings) == 0 {
		return
	}
	type point struct{ x, y float64 }
	projectedRings := make([][]point, len(rings))
	minY, maxY := float64(e.Height), 0.0
	for i, ring := range rings {
		projectedRings[i] = make([]point, 0, len(ring))
		for _, p := range ring {
			x, y := e.project(p[1], p[0])
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

func (e *Engine) drawRingFast(img *image.RGBA, coords [][]float64, c color.RGBA) {
	for i := 0; i < len(coords)-1; i++ {
		x1, y1 := e.project(coords[i][1], coords[i][0])
		x2, y2 := e.project(coords[i+1][1], coords[i+1][0])
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

func (e *Engine) AddPulse(lat, lng float64, c color.RGBA, count int) {
	lat += (rand.Float64() - 0.5) * 0.8
	lng += (rand.Float64() - 0.5) * 0.8
	x, y := e.project(lat, lng)
	e.pulsesMu.Lock()
	defer e.pulsesMu.Unlock()
	if len(e.pulses) < MaxActivePulses {
		baseRad, growth := 10.0, 16.0
		if e.Width > 2000 {
			baseRad, growth = 20.0, 32.0
		}
		radius := baseRad + math.Log10(float64(count)+1.0)*growth
		if radius > 240 {
			radius = 240
		}
		e.pulses = append(e.pulses, &Pulse{X: x, Y: y, StartTime: time.Now(), Color: c, MaxRadius: radius})
	}
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
