// Package geoservice provides geographic resolution and data management services.
package geoservice

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cloudflare/ahocorasick"
	"github.com/oschwald/maxminddb-golang"
	"github.com/sudorandom/bgp-stream/pkg/utils"
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

type GeoMetrics struct {
	CacheHits    atomic.Uint64
	CustomHits   atomic.Uint64
	CloudHits    atomic.Uint64
	MMDBHits     atomic.Uint64
	RIRHits      atomic.Uint64
	WHOISHits    atomic.Uint64
	PeeringHits  atomic.Uint64
	HubHits      atomic.Uint64
	UnknownHits  atomic.Uint64
	TotalLookups atomic.Uint64
	CacheResets  atomic.Uint64
}

type ripeHint struct {
	Lat, Lng float32
	CC       string
	City     string
}

type GeoService struct {
	width, height     int
	scale             float64
	countryHubs       map[string][]CityHub
	prefixToCityCache map[uint32]cacheEntry
	cacheMu           sync.Mutex
	dataMu            sync.RWMutex
	prefixData        PrefixData
	hubsData          PrefixData
	cityCoords        map[cityKey][2]float32
	countryCoords     map[string][2]float32
	citiesByCountry   map[string][]string
	cityMatchers      map[string]*ahocorasick.Matcher
	isoToCountry      map[string]string
	countryToISO      map[string]string
	ripeHints         *utils.DiskTrie
	peeringHints      *utils.DiskTrie
	cloudHints        *utils.DiskTrie
	customHints       *utils.DiskTrie
	mmdbHints         *utils.DiskTrie
	geoReaders        []*maxminddb.Reader
	metrics           GeoMetrics
}

type cityKey struct {
	city string
	cc   string
}

func NewGeoService(width, height int, scale float64) *GeoService {
	return &GeoService{
		width:             width,
		height:            height,
		scale:             scale,
		countryHubs:       make(map[string][]CityHub),
		prefixToCityCache: make(map[uint32]cacheEntry),
		cityCoords:        make(map[cityKey][2]float32),
		countryCoords:     make(map[string][2]float32),
		citiesByCountry:   make(map[string][]string),
		cityMatchers:      make(map[string]*ahocorasick.Matcher),
		isoToCountry:      make(map[string]string),
		countryToISO:      make(map[string]string),
	}
}

func (g *GeoService) AddMMDBReader(path string) error {
	db, err := maxminddb.Open(path)
	if err != nil {
		return err
	}
	g.geoReaders = append(g.geoReaders, db)
	log.Printf("[GEO] Added MMDB reader: %s", path)
	return nil
}

func (g *GeoService) SetPrefixData(data PrefixData) {
	g.prefixData = data
}

func (g *GeoService) SetHubsData(data PrefixData) {
	g.hubsData = data
}

func (g *GeoService) SetHintDBs(ripe, peering, cloud, custom, mmdb *utils.DiskTrie) {
	g.ripeHints = ripe
	g.peeringHints = peering
	g.cloudHints = cloud
	g.customHints = custom
	g.mmdbHints = mmdb
}

func (g *GeoService) OpenHintDBs(dir string, readOnly bool) error {
	var err error
	ripePath := filepath.Join(dir, "ripe-hints.db")
	peeringPath := filepath.Join(dir, "peering-hints.db")
	cloudPath := filepath.Join(dir, "cloud-hints.db")
	customPath := filepath.Join(dir, "custom-hints.db")
	mmdbPath := filepath.Join(dir, "mmdb-hints.db")

	if readOnly {
		g.ripeHints, _ = utils.OpenDiskTrieReadOnly(ripePath)
		g.peeringHints, _ = utils.OpenDiskTrieReadOnly(peeringPath)
		g.cloudHints, _ = utils.OpenDiskTrieReadOnly(cloudPath)
		g.customHints, _ = utils.OpenDiskTrieReadOnly(customPath)
		g.mmdbHints, _ = utils.OpenDiskTrieReadOnly(mmdbPath)
	} else {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		g.ripeHints, err = utils.OpenDiskTrie(ripePath)
		if err != nil {
			log.Printf("Warning: Failed to open WHOIS hints database: %v", err)
		}
		g.peeringHints, err = utils.OpenDiskTrie(peeringPath)
		if err != nil {
			log.Printf("Warning: Failed to open PeeringDB hints database: %v", err)
		}
		g.cloudHints, err = utils.OpenDiskTrie(cloudPath)
		if err != nil {
			log.Printf("Warning: Failed to open Cloud hints database: %v", err)
		}
		g.customHints, err = utils.OpenDiskTrie(customPath)
		if err != nil {
			log.Printf("Warning: Failed to open Custom hints database: %v", err)
		}
		g.mmdbHints, err = utils.OpenDiskTrie(mmdbPath)
		if err != nil {
			log.Printf("Warning: Failed to open MMDB hints database: %v", err)
		}
	}
	return nil
}

func (g *GeoService) Close() error {
	if g.ripeHints != nil {
		_ = g.ripeHints.Close()
	}
	if g.peeringHints != nil {
		_ = g.peeringHints.Close()
	}
	if g.cloudHints != nil {
		_ = g.cloudHints.Close()
	}
	if g.customHints != nil {
		_ = g.customHints.Close()
	}
	for _, r := range g.geoReaders {
		if r != nil {
			_ = r.Close()
		}
	}
	return nil
}

func (g *GeoService) ClearAll() error {
	if g.ripeHints != nil {
		_ = g.ripeHints.Clear()
	}
	if g.peeringHints != nil {
		_ = g.peeringHints.Clear()
	}
	if g.cloudHints != nil {
		_ = g.cloudHints.Clear()
	}
	if g.customHints != nil {
		_ = g.customHints.Clear()
	}
	return nil
}

func (g *GeoService) SetRIPEHints(hints map[uint32]ripeHint) {
	if g.ripeHints == nil {
		return
	}
	data := make(map[uint32][]byte)
	for ip, hint := range hints {
		b, _ := json.Marshal(hint)
		data[ip] = b
	}
	if err := g.ripeHints.BatchInsertUint32(data); err != nil {
		log.Printf("[GEO] Error batch inserting RIPE hints: %v", err)
	}
}

func (g *GeoService) SetRIPEHintsCIDR(hints map[string]ripeHint) {
	if g.ripeHints == nil {
		return
	}
	data := make(map[string][]byte)
	for prefix, hint := range hints {
		b, _ := json.Marshal(hint)
		data[prefix] = b
	}
	if err := g.ripeHints.BatchInsert(data); err != nil {
		log.Printf("[GEO] Error batch inserting RIPE CIDR hints: %v", err)
	}
}

func (g *GeoService) SetPeeringHints(hints map[uint32]ripeHint) {
	if g.peeringHints == nil {
		return
	}
	data := make(map[uint32][]byte)
	for ip, hint := range hints {
		b, _ := json.Marshal(hint)
		data[ip] = b
	}
	if err := g.peeringHints.BatchInsertUint32(data); err != nil {
		log.Printf("[GEO] Error batch inserting PeeringDB hints: %v", err)
	}
}

func (g *GeoService) SetPeeringHintsCIDR(hints map[string]ripeHint) {
	if g.peeringHints == nil {
		return
	}
	data := make(map[string][]byte)
	for prefix, hint := range hints {
		b, _ := json.Marshal(hint)
		data[prefix] = b
	}
	if err := g.peeringHints.BatchInsert(data); err != nil {
		log.Printf("[GEO] Error batch inserting PeeringDB CIDR hints: %v", err)
	}
}

func (g *GeoService) SetCustomHintsCIDR(hints map[string]ripeHint) {
	if g.customHints == nil {
		return
	}
	data := make(map[string][]byte)
	for prefix, hint := range hints {
		b, _ := json.Marshal(hint)
		data[prefix] = b
	}
	if err := g.customHints.BatchInsert(data); err != nil {
		log.Printf("[GEO] Error batch inserting Custom CIDR hints: %v", err)
	}
}

func (g *GeoService) ReportGeoMetrics() {
	cache := g.metrics.CacheHits.Swap(0)
	custom := g.metrics.CustomHits.Swap(0)
	cloud := g.metrics.CloudHits.Swap(0)
	mmdb := g.metrics.MMDBHits.Swap(0)
	rir := g.metrics.RIRHits.Swap(0)
	whois := g.metrics.WHOISHits.Swap(0)
	peering := g.metrics.PeeringHits.Swap(0)
	hubs := g.metrics.HubHits.Swap(0)
	unknown := g.metrics.UnknownHits.Swap(0)
	resets := g.metrics.CacheResets.Swap(0)
	total := custom + cloud + mmdb + rir + whois + peering + hubs + unknown

	if total == 0 || unknown == 0 {
		return
	}

	var sb strings.Builder
	sb.WriteString("[GEO-STATS]")
	fmt.Fprintf(&sb, " Total: %d (Cache: %.1f%%)", total, float64(cache)/float64(total)*100)

	appendMetric := func(label string, count uint64) {
		if count > 0 {
			fmt.Fprintf(&sb, ", %s: %d (%.1f%%)", label, count, float64(count)/float64(total)*100)
		}
	}

	appendMetric("Custom", custom)
	appendMetric("Cloud", cloud)
	appendMetric("MMDB", mmdb)
	appendMetric("RIR", rir)
	appendMetric("WHOIS", whois)
	appendMetric("Peering", peering)
	appendMetric("Hubs", hubs)
	appendMetric("Unknown", unknown)

	if resets > 0 {
		fmt.Fprintf(&sb, ", Resets: %d", resets)
	}

	log.Println(sb.String())
}

type ResolutionType string

const (
	ResCache   ResolutionType = "Cache"
	ResCustom  ResolutionType = "Custom"
	ResMMDB    ResolutionType = "MMDB"
	ResCloud   ResolutionType = "Cloud"
	ResGeoIP   ResolutionType = "GeoIP"
	ResRIR     ResolutionType = "RIR"
	ResWHOIS   ResolutionType = "WHOIS"
	ResPeering ResolutionType = "Peering"
	ResHubs    ResolutionType = "Hubs"
	ResUnknown ResolutionType = "Unknown"
)

type cacheEntry struct {
	Lat, Lng float64
	CC       string
	City     string
	ResType  ResolutionType
}

func (g *GeoService) GetIPCoords(ip uint32) (lat, lng float64, countryCode, city string, resType ResolutionType) {
	g.cacheMu.Lock()
	if c, ok := g.prefixToCityCache[ip]; ok {
		g.cacheMu.Unlock()
		g.metrics.CacheHits.Add(1)
		g.incrementSourceMetric(c.ResType)
		return c.Lat, c.Lng, c.CC, c.City, c.ResType
	}
	g.cacheMu.Unlock()

	lat, lng, countryCode, city, resType = g.resolveIP(ip)
	g.incrementSourceMetric(resType)

	// Always cache, even if 0,0, to prevent expensive re-resolution of unknown IPs
	g.updateCityCache(ip, lat, lng, countryCode, city, resType)

	return lat, lng, countryCode, city, resType
}

func (g *GeoService) incrementSourceMetric(resType ResolutionType) {
	switch resType {
	case ResCustom:
		g.metrics.CustomHits.Add(1)
	case ResMMDB, ResGeoIP:
		g.metrics.MMDBHits.Add(1)
	case ResCloud:
		g.metrics.CloudHits.Add(1)
	case ResRIR:
		g.metrics.RIRHits.Add(1)
	case ResWHOIS:
		g.metrics.WHOISHits.Add(1)
	case ResPeering:
		g.metrics.PeeringHits.Add(1)
	case ResHubs:
		g.metrics.HubHits.Add(1)
	case ResUnknown:
		g.metrics.UnknownHits.Add(1)
	}
}

func (g *GeoService) resolveIP(ip uint32) (lat, lng float64, countryCode, city string, resType ResolutionType) {
	// Metadata holders for fallbacks
	var bestCC, bestCity string

	updateMetadata := func(cc, cty string) {
		if cc != "" && bestCC == "" {
			bestCC = cc
		}
		if cty != "" && bestCity == "" {
			bestCity = cty
		}
	}

	// Stage 0: Custom Hints (Explicit overrides)
	if lat, lng, cc, cty, ok := g.resolveFromHints(g.customHints, ip); ok {
		return lat, lng, cc, cty, ResCustom
	} else {
		updateMetadata(cc, cty)
	}

	// Stage 1: MMDB Hints (Pre-processed MMDB files)
	if lat, lng, cc, cty, ok := g.resolveFromHints(g.mmdbHints, ip); ok {
		return lat, lng, cc, cty, ResMMDB
	} else {
		updateMetadata(cc, cty)
	}

	// Stage 2: Direct MMDB (Runtime loaded files)
	if lat, lng, cc, cty, ok := g.resolveFromMMDBs(ip); ok {
		return lat, lng, cc, cty, ResGeoIP
	} else {
		updateMetadata(cc, cty)
	}

	// Stage 3: Cloud Trie (Highest priority, very specific)
	if lat, lng, cc, cty, ok := g.resolveFromCloudTrie(ip); ok {
		return lat, lng, cc, cty, ResCloud
	} else {
		updateMetadata(cc, cty)
	}

	// Stage 4: PeeringDB Hints (Infrastructure)
	if lat, lng, cc, cty, ok := g.resolveFromHints(g.peeringHints, ip); ok {
		return lat, lng, cc, cty, ResPeering
	} else {
		updateMetadata(cc, cty)
	}

	// Stage 5: RIPE WHOIS hints (Background loaded)
	if lat, lng, cc, cty, ok := g.resolveFromHints(g.ripeHints, ip); ok {
		return lat, lng, cc, cty, ResWHOIS
	} else {
		updateMetadata(cc, cty)
	}

	// Stage 6: RIR-indexed data
	if lat, lng, cc, cty, ok := g.resolveFromRIRInternal(ip); ok {
		return lat, lng, cc, cty, ResRIR
	} else {
		updateMetadata(cc, cty)
	}

	// Stage 7: RIR-indexed hub data (Country only)
	if cc, cty, ok := g.resolveFromHubsInternal(ip); ok {
		if cty != "" && cc != "" {
			if l, ln, _ := g.ResolveCityToCoords(cty, cc); l != 0 || ln != 0 {
				return l, ln, cc, cty, ResHubs
			}
		}
		updateMetadata(cc, cty)
	}

	lat2, lng2, cc2, city2, resT2 := g.resolveFinalFallback(ip, bestCC, bestCity)
	return lat2, lng2, cc2, city2, resT2
}

func (g *GeoService) resolveFinalFallback(ip uint32, bestCC, bestCity string) (lat, lng float64, countryCode, city string, resType ResolutionType) {
	if bestCC != "" {
		l, ln, ccName, cityName := g.resolveFromCountryHubs(ip, bestCC)
		if l != 0 || ln != 0 {
			if cityName == "" {
				cityName = bestCity
			}
			return l, ln, ccName, cityName, ResHubs
		}

		// Absolute final fallback: Country center
		ccNorm := strings.ToUpper(bestCC)
		g.dataMu.RLock()
		coords, ok := g.countryCoords[ccNorm]
		countryName := bestCC
		if name, okName := g.isoToCountry[ccNorm]; okName {
			countryName = name
		}
		g.dataMu.RUnlock()

		if ok {
			return float64(coords[0]), float64(coords[1]), countryName, bestCity, ResHubs
		}
		return 0, 0, bestCC, bestCity, ResUnknown
	}

	return 0, 0, "", "", ResUnknown
}

func (g *GeoService) resolveFromMMDBs(ip uint32) (lat, lng float64, cc, city string, ok bool) {
	if len(g.geoReaders) == 0 {
		return 0, 0, "", "", false
	}
	ipObj := make(net.IP, 4)
	binary.BigEndian.PutUint32(ipObj, ip)

	for _, reader := range g.geoReaders {
		var record map[string]interface{}
		if err := reader.Lookup(ipObj, &record); err == nil && record != nil {
			lat, lng, ok = g.ExtractCoords(record)
			cc = g.ExtractCC(record)
			city = g.ExtractCity(record)
			if ok {
				return lat, lng, cc, city, true
			}
			if city != "" && cc != "" {
				if l, ln, _ := g.ResolveCityToCoords(city, cc); l != 0 || ln != 0 {
					return l, ln, cc, city, true
				}
			}
		}
	}
	return 0, 0, cc, city, false
}

func (g *GeoService) ExtractCity(record map[string]interface{}) string {
	cityKeys := []string{"city", "City", "city_name"}
	for _, k := range cityKeys {
		if v, ok := record[k].(string); ok {
			return v
		}
	}

	// Check MaxMind style nested city names
	if city, ok := record["city"].(map[string]interface{}); ok {
		if names, ok := city["names"].(map[string]interface{}); ok {
			if name, ok := names["en"].(string); ok {
				return name
			}
		}
	}
	return ""
}

func (g *GeoService) ExtractCC(record map[string]interface{}) string {
	cc := ""

	// Check for "country_code" first (common in IPInfo, IP2Location, etc.)
	if c, ok := record["country_code"].(string); ok && len(c) == 2 {
		cc = c
	}

	// Check for "country" next
	if cc == "" {
		if country, ok := record["country"].(map[string]interface{}); ok {
			if iso, ok := country["iso_code"].(string); ok && len(iso) == 2 {
				cc = iso
			}
		} else if c, ok := record["country"].(string); ok && len(c) == 2 {
			cc = c
		}
	}

	if cc == "" {
		// If we still have nothing, let's see what was in there that we rejected
		if c, ok := record["country_code"].(string); ok {
			cc = c
		} else if c, ok := record["country"].(string); ok {
			cc = c
		}
	}

	sanitized := g.SanitizeCC(cc)
	if sanitized == "" && cc != "" {
		return ""
	}
	return sanitized
}

func (g *GeoService) ExtractCoords(record map[string]interface{}) (lat, lng float64, ok bool) {
	// 1. Check MaxMind style: location -> latitude/longitude
	if loc, ok := record["location"].(map[string]interface{}); ok {
		lat, ok1 := g.AsFloat(loc["latitude"])
		if !ok1 {
			lat, ok1 = g.AsFloat(loc["lat"])
		}
		lng, ok2 := g.AsFloat(loc["longitude"])
		if !ok2 {
			lng, ok2 = g.AsFloat(loc["lng"])
		}
		if !ok2 {
			lng, ok2 = g.AsFloat(loc["long"])
		}
		if ok1 && ok2 {
			return lat, lng, true
		}
	}

	// 2. Check flat style (IPInfo and others)
	lat, ok1 := g.AsFloat(record["latitude"])
	if !ok1 {
		lat, ok1 = g.AsFloat(record["lat"])
	}
	if !ok1 {
		lat, ok1 = g.AsFloat(record["Latitude"])
	}

	lng, ok2 := g.AsFloat(record["longitude"])
	if !ok2 {
		lng, ok2 = g.AsFloat(record["lng"])
	}
	if !ok2 {
		lng, ok2 = g.AsFloat(record["long"])
	}
	if !ok2 {
		lng, ok2 = g.AsFloat(record["Longitude"])
	}

	if ok1 && ok2 {
		return lat, lng, true
	}

	// 3. Check IPInfo "loc" field: "lat,lng" string
	if locStr, ok := record["loc"].(string); ok {
		var lat, lng float64
		if _, err := fmt.Sscanf(locStr, "%f,%f", &lat, &lng); err == nil {
			return lat, lng, true
		}
	}

	return 0, 0, false
}

func (g *GeoService) resolveFromHints(trie *utils.DiskTrie, ip uint32) (lat, lng float64, cc, city string, ok bool) {
	h, ok := g.lookupHint(trie, ip)
	if !ok {
		return 0, 0, "", "", false
	}
	cc = g.SanitizeCC(h.CC)
	city = h.City
	if h.Lat != 0 || h.Lng != 0 {
		return float64(h.Lat), float64(h.Lng), cc, city, true
	}
	if h.City != "" && cc != "" {
		if l, ln, _ := g.ResolveCityToCoords(h.City, cc); l != 0 || ln != 0 {
			return l, ln, cc, city, true
		}
	}
	return 0, 0, cc, city, false
}

func (g *GeoService) resolveFromRIRInternal(ip uint32) (lat, lng float64, cc, city string, ok bool) {
	loc := g.lookupIP(ip)
	if loc == nil {
		return 0, 0, "", "", false
	}
	lat, _ = g.AsFloat(loc[0])
	lng, _ = g.AsFloat(loc[1])
	ccRaw, _ := loc[2].(string)
	cc = g.SanitizeCC(ccRaw)
	city, _ = loc[3].(string)

	if lat != 0 || lng != 0 {
		return lat, lng, cc, city, true
	}
	if city != "" && cc != "" {
		if l, ln, _ := g.ResolveCityToCoords(city, cc); l != 0 || ln != 0 {
			return l, ln, cc, city, true
		}
	}
	return 0, 0, cc, city, false
}

func (g *GeoService) resolveFromHubsInternal(ip uint32) (cc, city string, ok bool) {
	loc := g.lookupHubIP(ip)
	if loc == nil {
		return "", "", false
	}
	ccRaw, _ := loc[2].(string)
	cc = g.SanitizeCC(ccRaw)
	city, _ = loc[3].(string)
	return cc, city, true
}

func (g *GeoService) lookupHint(trie *utils.DiskTrie, ip uint32) (ripeHint, bool) {
	if trie == nil {
		return ripeHint{}, false
	}
	val, _, err := trie.LookupUint32(ip)
	if err != nil || val == nil {
		return ripeHint{}, false
	}
	var hint ripeHint
	if err := json.Unmarshal(val, &hint); err != nil {
		return ripeHint{}, false
	}
	return hint, true
}

func (g *GeoService) SanitizeCC(cc string) string {
	// Filter out invalid or generic country codes
	if strings.Contains(cc, "WORLD WIDE") || strings.Contains(cc, "ANYCAST") || strings.Contains(cc, "#") || len(cc) > 3 {
		return ""
	}
	return cc
}

func (g *GeoService) AsFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case string:
		var f float64
		if _, err := fmt.Sscanf(val, "%f", &f); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (g *GeoService) resolveFromCloudTrie(ip uint32) (lat, lng float64, cc, city string, ok bool) {
	if g.cloudHints == nil {
		return 0, 0, "", "", false
	}
	val, _, err := g.cloudHints.LookupUint32(ip)
	if err != nil || val == nil {
		return 0, 0, "", "", false
	}

	loc := string(val)
	parts := strings.Split(loc, "|")
	if len(parts) == 2 {
		city, countryCode := parts[0], parts[1]
		lat, lng, cc = g.ResolveCityToCoords(city, countryCode)
		if lat != 0 || lng != 0 {
			g.metrics.CloudHits.Add(1)
			return lat, lng, cc, city, true
		}
		return 0, 0, countryCode, city, false
	}
	return 0, 0, "", "", false
}

func (g *GeoService) resolveFromCountryHubs(ip uint32, countryCode string) (lat, lng float64, cc, city string) {
	if countryCode == "" {
		return 0, 0, "", ""
	}
	g.dataMu.RLock()
	hubs := g.countryHubs[countryCode]
	g.dataMu.RUnlock()
	if len(hubs) > 0 {
		h := utils.HashUint32(ip)
		r := (float64(h) / float64(0xFFFFFFFF)) * hubs[len(hubs)-1].CumulativeWeight
		for _, h := range hubs {
			if h.CumulativeWeight < r {
				continue
			}
			g.metrics.HubHits.Add(1)
			// Find city name for these coords if possible
			cityName := ""
			g.dataMu.RLock()
			for ck, coords := range g.cityCoords {
				if ck.cc == countryCode && math.Abs(float64(coords[0])-h.Lat) < 0.01 && math.Abs(float64(coords[1])-h.Lng) < 0.01 {
					cityName = ck.city
					break
				}
			}
			g.dataMu.RUnlock()
			return h.Lat, h.Lng, countryCode, cityName
		}
	}
	return 0, 0, countryCode, ""
}

func (g *GeoService) updateCityCache(ip uint32, lat, lng float64, cc, city string, resType ResolutionType) {
	g.cacheMu.Lock()
	defer g.cacheMu.Unlock()
	if len(g.prefixToCityCache) > 200000 {
		// Fast reset: reassigning the map is O(1) and allows the old one to be GC'd
		g.prefixToCityCache = make(map[uint32]cacheEntry)
		g.metrics.CacheResets.Add(1)
	}
	g.prefixToCityCache[ip] = cacheEntry{Lat: lat, Lng: lng, CC: cc, City: city, ResType: resType}
}

type GeoResolver interface {
	GetIPCoords(ip uint32) (lat, lng float64, countryCode, city string, resType ResolutionType)
}

func (g *GeoService) ResolveCityToCoords(city, cc string) (lat, lng float64, countryCode string) {
	g.dataMu.RLock()
	defer g.dataMu.RUnlock()
	if c, ok := g.cityCoords[cityKey{city: strings.ToLower(city), cc: strings.ToUpper(cc)}]; ok {
		return float64(c[0]), float64(c[1]), cc
	}
	return 0, 0, cc
}

func (g *GeoService) lookupIP(ip uint32) Location {
	r := g.prefixData.R
	low, high := 0, (len(r)/2)-1
	for low <= high {
		mid := (low + high) / 2
		startIP := r[mid*2]
		nextStartIP := uint64(0x100000000)
		if mid+1 < len(r)/2 {
			nextStartIP = uint64(r[(mid+1)*2])
		}
		if uint64(ip) >= uint64(startIP) && uint64(ip) < nextStartIP {
			locIdx := r[mid*2+1]
			if locIdx == 4294967295 {
				return nil
			}
			return g.prefixData.L[locIdx]
		}
		if startIP < ip {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return nil
}

func (g *GeoService) lookupHubIP(ip uint32) Location {
	r := g.hubsData.R
	low, high := 0, (len(r)/2)-1
	for low <= high {
		mid := (low + high) / 2
		startIP := r[mid*2]
		nextStartIP := uint64(0x100000000)
		if mid+1 < len(r)/2 {
			nextStartIP = uint64(r[(mid+1)*2])
		}
		if uint64(ip) >= uint64(startIP) && uint64(ip) < nextStartIP {
			locIdx := r[mid*2+1]
			if locIdx == 4294967295 {
				return nil
			}
			return g.hubsData.L[locIdx]
		}
		if startIP < ip {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return nil
}

func (g *GeoService) GetCountryCoords() map[string][2]float32 {
	return g.countryCoords
}

func (g *GeoService) GetCountryHubs() map[string][]CityHub {
	return g.countryHubs
}

func (g *GeoService) Project(lat, lng float64) (x, y float64) {
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
	r := g.scale
	x = (float64(g.width) / 2) + r*(2*math.Sqrt(2)/math.Pi)*lngRad*math.Cos(theta)
	y = (float64(g.height) / 2) - r*math.Sqrt(2)*math.Sin(theta)
	return x, y
}
