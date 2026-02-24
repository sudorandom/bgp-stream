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
	Lat, Lng       float64
	New, Upd, With int
}

var (
	ColorNew  = color.RGBA{0, 191, 255, 255}   // Sky Blue
	ColorUpd  = color.RGBA{173, 255, 47, 255}  // Lime Green
	ColorWith = color.RGBA{255, 50, 50, 255}   // Red
	ColorNote = color.RGBA{255, 255, 255, 255} // White
	ColorPeer = color.RGBA{255, 255, 0, 255}   // Yellow
	ColorOpen = color.RGBA{0, 100, 255, 255}   // Blue
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
	windowNew, windowUpd, windowWith   int64
	windowNote, windowPeer, windowOpen int64

	rateNew, rateUpd, rateWith   float64
	rateNote, ratePeer, rateOpen float64

	countryActivity map[string]int
	topHubs         []struct {
		CC   string
		Rate int
	}

	// History for trendlines (last 60 snapshots, 2s each = 2 mins)
	history   []MetricSnapshot
	metricsMu sync.Mutex

	audioContext *audio.Context
	AudioWriter  io.Writer
}

type MetricSnapshot struct {
	New, Upd, With, Note, Peer, Open int
}

func NewEngine(width, height int, scale float64) *Engine {
	s, _ := text.NewGoTextFaceSource(bytes.NewReader(goregular.TTF))
	m, _ := text.NewGoTextFaceSource(bytes.NewReader(gomono.TTF))

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
	maxAdded := 20
	if len(e.visualQueue) > 1000 {
		maxAdded = 100
	}
	for len(e.visualQueue) > 0 && (e.visualQueue[0].ScheduledTime.Before(now) || len(e.visualQueue) > 2000) && added < maxAdded {
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
			}
			e.AddPulse(p.Lat, p.Lng, c, p.Count)
		}
	}
	e.queueMu.Unlock()

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

	e.drawLegend(screen)
	e.drawStatus(screen)
	e.drawMetrics(screen)
}

func (e *Engine) drawLegend(screen *ebiten.Image) {
	margin, fontSize, clockFontSize := 100.0, 18.0, 36.0
	spacing, swatchSize := 36.0, 18.0
	if e.Width > 2000 {
		margin, fontSize, clockFontSize = 200.0, 36.0, 72.0
		spacing, swatchSize = 72.0, 36.0
	}

	lx := margin
	ly := float64(e.Height) - margin - clockFontSize - (float64(3) * spacing) - 40.0

	items := []struct {
		Label string
		Color color.RGBA
	}{
		{"Announcement", ColorNew},
		{"Path Change", ColorUpd},
		{"Withdrawal", ColorWith},
	}

	imgW, _ := e.pulseImage.Bounds().Dx(), e.pulseImage.Bounds().Dy()
	halfW := float64(imgW) / 2

	for i, it := range items {
		ty := ly + float64(i)*spacing
		baseAlpha := 0.6
		if it.Color == ColorNew {
			baseAlpha = 0.3
		}
		r, g, b := float64(it.Color.R)/255.0, float64(it.Color.G)/255.0, float64(it.Color.B)/255.0

		// Outer Ring
		sop := &ebiten.DrawImageOptions{}
		sop.Blend = ebiten.BlendLighter
		pulseScaleOuter := swatchSize / float64(imgW) * 1.8
		sop.GeoM.Translate(-halfW, -halfW)
		sop.GeoM.Scale(pulseScaleOuter, pulseScaleOuter)
		sop.GeoM.Translate(lx+(swatchSize/2), ty+(swatchSize/2))
		sop.ColorScale.Scale(float32(r*baseAlpha), float32(g*baseAlpha), float32(b*baseAlpha), float32(baseAlpha))
		screen.DrawImage(e.pulseImage, sop)

		// Inner Ring
		sip := &ebiten.DrawImageOptions{}
		sip.Blend = ebiten.BlendLighter
		pulseScaleInner := swatchSize / float64(imgW) * 0.9
		sip.GeoM.Translate(-halfW, -halfW)
		sip.GeoM.Scale(pulseScaleInner, pulseScaleInner)
		sip.GeoM.Translate(lx+(swatchSize/2), ty+(swatchSize/2))
		sip.ColorScale.Scale(float32(r*baseAlpha), float32(g*baseAlpha), float32(b*baseAlpha), float32(baseAlpha))
		screen.DrawImage(e.pulseImage, sip)

		if e.fontSource != nil {
			face := &text.GoTextFace{Source: e.fontSource, Size: fontSize}
			top := &text.DrawOptions{}
			top.GeoM.Translate(lx+swatchSize+15, ty+(swatchSize/2)-(fontSize/2))
			top.ColorScale.Scale(1, 1, 1, 0.8)
			text.Draw(screen, it.Label, face, top)
		}
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

func (e *Engine) generateBackground() error {
	cpuImg := image.NewRGBA(image.Rect(0, 0, e.Width, e.Height))
	draw.Draw(cpuImg, cpuImg.Bounds(), &image.Uniform{color.RGBA{8, 10, 15, 255}}, image.Point{}, draw.Src)
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
	cityCoords := make(map[string][2]float32)
	csvReader := csv.NewReader(bytes.NewReader(worldCitiesCSV))
	csvReader.Read()
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		lat, _ := strconv.ParseFloat(rec[2], 64)
		lng, _ := strconv.ParseFloat(rec[3], 64)
		cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(rec[1]), strings.ToUpper(rec[5]))] = [2]float32{float32(lat), float32(lng)}
	}
	geoReader, err := maxminddb.FromBytes(geoIPDB)
	if err != nil {
		return err
	}
	defer geoReader.Close()
	var mu sync.Mutex
	var allRanges []ipRange
	var wg sync.WaitGroup
	handler := func(start, end uint32, city, cc string, lat, lng float32, priority int) {
		if lat == 0 && lng == 0 {
			if city != "" {
				if c, ok := cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(city), strings.ToUpper(cc))]; ok {
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
					if c, ok := cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(cityName), strings.ToUpper(cc))]; ok {
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
	os.MkdirAll("data", 0755)
	if f, err := os.Create(cachePath); err == nil {
		json.NewEncoder(f).Encode(e.prefixData)
		f.Close()
	}
	debug.FreeOSMemory()
	return nil
}

func (e *Engine) ListenToBGP() {
	url := "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream"
	pendingWithdrawals := make(map[uint32]time.Time)
	var mu sync.Mutex
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			now := time.Now()
			mu.Lock()
			for ip, t := range pendingWithdrawals {
				if now.After(t) {
					if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
						e.recordEvent(lat, lng, cc, "with")
					}
					delete(pendingWithdrawals, ip)
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
			switch msg.Type {
			case "ris_error":
				log.Printf("[RIS ERROR] %s", string(message))
			case "ris_message":
				for _, prefix := range msg.Data.Withdrawals {
					ip := e.prefixToIP(prefix)
					mu.Lock()
					pendingWithdrawals[ip] = time.Now().Add(5 * time.Second)
					mu.Unlock()
				}
				for _, ann := range msg.Data.Announcements {
					for _, prefix := range ann.Prefixes {
						ip := e.prefixToIP(prefix)
						mu.Lock()
						if _, ok := pendingWithdrawals[ip]; ok {
							delete(pendingWithdrawals, ip)
							mu.Unlock()
							if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
								e.recordEvent(lat, lng, cc, "upd")
							}
						} else {
							mu.Unlock()
							if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
								e.recordEvent(lat, lng, cc, "new")
							}
						}
					}
				}
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
		maxQueueSize := 5000
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
	loc := e.lookupIP(ip)
	if loc == nil {
		return 0, 0, ""
	}
	lat, _ := loc[0].(float64)
	lng, _ := loc[1].(float64)
	cc, _ := loc[2].(string)

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

	if lat != 0 || lng != 0 {
		e.cacheMu.Lock()
		if len(e.prefixToCityCache) > 100000 {
			// Aggressively clear 20% of the cache to avoid constant cleanup
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
	}
	return lat, lng, cc
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
	latRad, lngRad := lat*math.Pi/180, lng*math.Pi/180
	theta := latRad
	for i := 0; i < 10; i++ {
		delta := (2*theta + math.Sin(2*theta) - math.Pi*math.Sin(latRad)) / (2 + 2*math.Cos(2*theta))
		theta -= delta
		if math.Abs(delta) < 1e-7 {
			break
		}
	}
	r := float64(e.Width) / (2 * math.Sqrt(8)) * 1.2
	x = (float64(e.Width) * 0.45) + r*(2*math.Sqrt(2)/math.Pi)*lngRad*math.Cos(theta)
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
		projectedRings[i] = make([]point, len(ring))
		for j, p := range ring {
			x, y := e.project(p[1], p[0])
			projectedRings[i][j] = point{x, y}
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
	if len(e.pulses) < 1500 {
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
	var a, b, c, d uint32
	fmt.Sscanf(p, "%d.%d.%d.%d", &a, &b, &c, &d)
	return (a << 24) | (b << 16) | (c << 8) | d
}
