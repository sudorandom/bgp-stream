package geoservice

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cloudflare/ahocorasick"
	"github.com/sudorandom/bgp-stream/pkg/sources"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type DataManager struct {
	geo *GeoService
}

func NewDataManager(geo *GeoService) *DataManager {
	return &DataManager{geo: geo}
}

type ipRange struct {
	Start, End uint32
	CC, City   string
	Lat, Lng   float32
	Priority   int
}

type prefixSegment struct {
	start, end uint32
	r          *ipRange
}

func (dm *DataManager) LoadWorldCities() {
	var citiesReader io.Reader
	citiesPath := "./data/worldcities.csv"
	if f, err := os.Open(citiesPath); err == nil {
		defer func() { _ = f.Close() }()
		citiesReader = f
		log.Println("Using worldcities.csv from disk")
	} else if len(worldCitiesCSV) > 0 {
		citiesReader = strings.NewReader(string(worldCitiesCSV))
		log.Println("Using embedded worldcities.csv")
	}

	if citiesReader != nil {
		dm.geo.dataMu.Lock()
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
				cc := strings.ToUpper(rec[5])
				countryName := strings.ToUpper(rec[4])
				cityName := strings.ToLower(rec[1])
				dm.geo.cityCoords[cityKey{city: cityName, cc: cc}] = [2]float32{float32(lat), float32(lng)}
				dm.geo.cityCoords[cityKey{city: cityName, cc: countryName}] = [2]float32{float32(lat), float32(lng)}
				dm.geo.citiesByCountry[cc] = append(dm.geo.citiesByCountry[cc], cityName)
				dm.geo.citiesByCountry[countryName] = append(dm.geo.citiesByCountry[countryName], cityName)
				dm.geo.isoToCountry[cc] = rec[4]
				dm.geo.countryToISO[strings.ToUpper(rec[4])] = cc

				if _, ok := dm.geo.countryCoords[cc]; !ok || (len(rec) >= 9 && rec[8] == "primary") {
					dm.geo.countryCoords[cc] = [2]float32{float32(lat), float32(lng)}
					dm.geo.countryCoords[countryName] = [2]float32{float32(lat), float32(lng)}
				}
			} else if len(rec) >= 10 {
				// Supported Format 2 (dr5hn): id, name, state_id, state_code, state_name, country_id, country_code, country_name, latitude, longitude, wikiDataId
				lat, _ := strconv.ParseFloat(rec[8], 64)
				lng, _ := strconv.ParseFloat(rec[9], 64)
				cc := strings.ToUpper(rec[6])
				countryName := strings.ToUpper(rec[7])
				cityName := strings.ToLower(rec[1])
				dm.geo.cityCoords[cityKey{city: cityName, cc: cc}] = [2]float32{float32(lat), float32(lng)}
				dm.geo.cityCoords[cityKey{city: cityName, cc: countryName}] = [2]float32{float32(lat), float32(lng)}
				dm.geo.citiesByCountry[cc] = append(dm.geo.citiesByCountry[cc], cityName)
				dm.geo.citiesByCountry[countryName] = append(dm.geo.citiesByCountry[countryName], cityName)
				dm.geo.isoToCountry[cc] = rec[7]
				dm.geo.countryToISO[strings.ToUpper(rec[7])] = cc

				if _, ok := dm.geo.countryCoords[cc]; !ok {
					dm.geo.countryCoords[cc] = [2]float32{float32(lat), float32(lng)}
					dm.geo.countryCoords[countryName] = [2]float32{float32(lat), float32(lng)}
				}
			}
		}

		log.Printf("[GEO] City dictionary loaded. %d countries, %d total cities. US: %d, CN: %d, GB: %d, DE: %d, FR: %d",
			len(dm.geo.citiesByCountry), len(dm.geo.cityCoords)/2,
			len(dm.geo.citiesByCountry["US"]), len(dm.geo.citiesByCountry["CN"]),
			len(dm.geo.citiesByCountry["GB"]), len(dm.geo.citiesByCountry["DE"]),
			len(dm.geo.citiesByCountry["FR"]))
		dm.geo.dataMu.Unlock()
	}
}

func (dm *DataManager) InitMatchers() {
	dm.geo.dataMu.Lock()
	defer dm.geo.dataMu.Unlock()
	log.Printf("[GEO] Building Aho-Corasick matchers for %d countries...", len(dm.geo.citiesByCountry))
	for cc := range dm.geo.citiesByCountry {
		sort.Slice(dm.geo.citiesByCountry[cc], func(i, j int) bool {
			return len(dm.geo.citiesByCountry[cc][i]) > len(dm.geo.citiesByCountry[cc][j])
		})
		cityBytes := make([][]byte, len(dm.geo.citiesByCountry[cc]))
		for i, city := range dm.geo.citiesByCountry[cc] {
			cityBytes[i] = []byte(city)
		}
		dm.geo.cityMatchers[cc] = ahocorasick.NewMatcher(cityBytes)
	}
	log.Printf("[GEO] Built %d matchers", len(dm.geo.cityMatchers))
}

func (dm *DataManager) DownloadWorldCities(force bool) error {
	citiesPath := "./data/worldcities.csv"
	if !force {
		if _, err := os.Stat(citiesPath); err == nil {
			return nil
		}
	}
	log.Printf("Downloading world cities database from %s...", sources.WorldCitiesURL)
	return sources.DownloadWorldCities(citiesPath)
}

func (dm *DataManager) LoadRemoteCityData() error {
	cities, err := sources.FetchCityDominance()
	if err != nil {
		return err
	}
	dm.geo.dataMu.Lock()
	defer dm.geo.dataMu.Unlock()
	for _, c := range cities {
		cc, ok := dm.geo.countryToISO[strings.ToUpper(c.Country)]
		if !ok {
			// Fallback to name if code not found
			cc = c.Country
		}

		hubs := dm.geo.countryHubs[cc]
		weight := c.LogicalDominanceIPs
		if weight <= 0 {
			weight = 1
		}
		last := 0.0
		if len(hubs) > 0 {
			last = hubs[len(hubs)-1].CumulativeWeight
		}
		dm.geo.countryHubs[cc] = append(hubs, CityHub{Lat: c.Coordinates[1], Lng: c.Coordinates[0], CumulativeWeight: last + weight})
	}
	return nil
}

func (dm *DataManager) ProcessCustomHints(hints []string) {
	if len(hints) == 0 {
		return
	}
	ripeHints := make(map[string]ripeHint)
	for _, h := range hints {
		// Format: CIDR:City,CC
		parts := strings.SplitN(h, ":", 2)
		if len(parts) != 2 {
			log.Printf("[CUSTOM] Invalid hint format (expected CIDR:City,CC): %s", h)
			continue
		}
		prefix := parts[0]
		locParts := strings.SplitN(parts[1], ",", 2)
		if len(locParts) != 2 {
			log.Printf("[CUSTOM] Invalid location format (expected City,CC): %s", parts[1])
			continue
		}
		city, cc := strings.TrimSpace(locParts[0]), strings.ToUpper(strings.TrimSpace(locParts[1]))
		hint := ripeHint{CC: cc, City: city}

		// Try to resolve city to coordinates if possible
		lat, lng, _ := dm.geo.ResolveCityToCoords(city, cc)
		hint.Lat, hint.Lng = float32(lat), float32(lng)

		ripeHints[prefix] = hint
	}

	if len(ripeHints) > 0 {
		dm.geo.SetCustomHintsCIDR(ripeHints)
		log.Printf("[CUSTOM] Loaded %d custom hints into CustomHintsDB", len(ripeHints))
	}
}

func (dm *DataManager) ProcessRIRData() error {
	log.Println("[GEO] Fetching RIR data...")
	allRanges, countryOnlyRanges := dm.fetchRIRData()
	log.Printf("[GEO] RIR fetch complete. Total ranges: %d city-level, %d country-only", len(allRanges), len(countryOnlyRanges))

	segments := dm.flattenPrefixData(allRanges)
	dm.indexPrefixData(segments, &dm.geo.prefixData)

	hubSegments := dm.flattenPrefixData(countryOnlyRanges)
	dm.indexPrefixData(hubSegments, &dm.geo.hubsData)

	cachePath := "./data/prefix-dump-cache.json"
	if f, err := os.Create(cachePath); err == nil {
		if err := json.NewEncoder(f).Encode(dm.geo.prefixData); err != nil {
			log.Printf("Warning: Failed to encode prefix cache: %v", err)
		}
		_ = f.Close()
	}

	hubsCachePath := "./data/hubs-dump-cache.json"
	if f, err := os.Create(hubsCachePath); err == nil {
		if err := json.NewEncoder(f).Encode(dm.geo.hubsData); err != nil {
			log.Printf("Warning: Failed to encode hubs cache: %v", err)
		}
		_ = f.Close()
	}
	return nil
}

func (dm *DataManager) fetchRIRData() (allRanges, countryOnlyRanges []ipRange) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, src := range sources.GetRIRSources() {
		wg.Add(1)
		go func(s sources.RIRSource) {
			defer wg.Done()
			dm.processRIRSource(s, &mu, &allRanges, &countryOnlyRanges)
		}(src)
	}
	wg.Wait()
	return allRanges, countryOnlyRanges
}

func (dm *DataManager) processRIRSource(src sources.RIRSource, mu *sync.Mutex, allRanges, countryOnlyRanges *[]ipRange) {
	rc, err := utils.GetCachedReader(src.URL, true, "[RIR-"+src.Name+"]")
	if err != nil {
		log.Printf("[RIR-%s] Error fetching data: %v", src.Name, err)
		return
	}
	defer func() { _ = rc.Close() }()

	br := bufio.NewReader(rc)
	header, _ := br.Peek(3)
	var r io.Reader = br

	if len(header) >= 2 && header[0] == 0x1f && header[1] == 0x8b {
		gr, err := gzip.NewReader(br)
		if err != nil {
			log.Printf("[RIR-%s] Error creating gzip reader: %v", src.Name, err)
			return
		}
		defer func() { _ = gr.Close() }()
		r = gr
	} else if len(header) >= 3 && header[0] == 'B' && header[1] == 'Z' && header[2] == 'h' {
		r = bzip2.NewReader(br)
	}

	scanner := bufio.NewScanner(r)
	count := 0
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "|")
		if len(parts) < 7 || parts[2] != "ipv4" {
			continue
		}
		c, _ := strconv.ParseUint(parts[4], 10, 32)
		startIP := net.ParseIP(parts[3]).To4()
		if startIP != nil {
			start := binary.BigEndian.Uint32(startIP)
			p := 32
			for c2 := uint32(c); c2 > 1; c2 >>= 1 {
				p--
			}
			dm.handleRIRRange(start, start+uint32(c)-1, strings.ToUpper(parts[1]), p, mu, allRanges, countryOnlyRanges)
			count++
		}
	}
	log.Printf("[RIR-%s] Loaded %d ranges", src.Name, count)
}

func (dm *DataManager) handleRIRRange(start, end uint32, cc string, priority int, mu *sync.Mutex, allRanges, countryOnlyRanges *[]ipRange) {
	var lat, lng float64
	var city string

	ipsToTry := []uint32{start}
	if end > start {
		ipsToTry = append(ipsToTry, start+1)
	}
	if end > start+2 {
		ipsToTry = append(ipsToTry, end-1)
	}

	for _, testIP := range ipsToTry {
		lat, lng, _, _, _ = dm.geo.GetIPCoords(testIP)
		if lat != 0 || lng != 0 {
			// We found something via our hints or other sources
			break
		}
	}

	if lat != 0 || lng != 0 {
		mu.Lock()
		*allRanges = append(*allRanges, ipRange{Start: start, End: end, City: city, CC: dm.geo.SanitizeCC(cc), Lat: float32(lat), Lng: float32(lng), Priority: priority + 100})
		mu.Unlock()
	} else if sanitized := dm.geo.SanitizeCC(cc); sanitized != "" {
		mu.Lock()
		*countryOnlyRanges = append(*countryOnlyRanges, ipRange{Start: start, End: end, City: city, CC: sanitized, Priority: priority})
		mu.Unlock()
	}
}

func (dm *DataManager) flattenPrefixData(allRanges []ipRange) []prefixSegment {
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
		if hasActive && pos > lastPos {
			segments = append(segments, prefixSegment{lastPos, pos - 1, getBest()})
		}
		for i < len(events) && events[i].pos == pos && !events[i].isEnd {
			r := events[i].r
			activeStacks[r.Priority] = append(activeStacks[r.Priority], r)
			i++
		}
		best := getBest()
		if best != nil {
			segments = append(segments, prefixSegment{pos, pos, best})
		}
		for i < len(events) && events[i].pos == pos && events[i].isEnd {
			r := events[i].r
			stack := activeStacks[r.Priority]
			for idx, val := range stack {
				if val == r {
					activeStacks[r.Priority] = append(stack[:idx], stack[idx+1:]...)
					break
				}
			}
			i++
		}
		if pos == 0xFFFFFFFF {
			break
		}
		lastPos = pos + 1
		hasActive = getBest() != nil
	}
	return segments
}

func (dm *DataManager) indexPrefixData(segments []prefixSegment, target *PrefixData) {
	locToIdx := make(map[string]int)
	var locations []Location
	var flatRanges []uint32
	var lastEnd uint32
	var hasLast bool

	for _, seg := range segments {
		if !hasLast {
			if seg.start > 0 {
				flatRanges = append(flatRanges, 0, 4294967295)
			}
		} else if seg.start > lastEnd+1 {
			flatRanges = append(flatRanges, lastEnd+1, 4294967295)
		}

		// Use a key that includes the range boundaries to prevent merging distinct RIR records
		key := fmt.Sprintf("%d|%d|%s|%s|%f|%f", seg.start, seg.end, seg.r.CC, seg.r.City, seg.r.Lat, seg.r.Lng)
		idx, ok := locToIdx[key]
		if !ok {
			idx = len(locations)
			locations = append(locations, Location{float64(seg.r.Lat), float64(seg.r.Lng), seg.r.CC, seg.r.City})
			locToIdx[key] = idx
		}

		if len(flatRanges) >= 2 && flatRanges[len(flatRanges)-1] == uint32(idx) {
		} else {
			flatRanges = append(flatRanges, seg.start, uint32(idx))
		}
		lastEnd = seg.end
		hasLast = true
	}

	if hasLast && lastEnd < 0xFFFFFFFF {
		flatRanges = append(flatRanges, lastEnd+1, 4294967295)
	}

	target.L = locations
	target.R = flatRanges
}

type countingReader struct {
	io.Reader
	count *atomic.Uint64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.count.Add(uint64(n))
	return n, err
}

func (dm *DataManager) ProcessBulkWhoisData() {
	dm.InitMatchers()
	sourcesList := sources.GetBulkWhoisSources()
	log.Printf("[WHOIS] Starting bulk scan of %d registries in parallel", len(sourcesList))

	var wg sync.WaitGroup
	sem := make(chan struct{}, 2)

	for _, src := range sourcesList {
		wg.Add(1)
		go func(s sources.RIRSource) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			dm.processRegistry(s)
		}(src)
	}
	wg.Wait()
	log.Printf("[WHOIS] All scans complete.")
}

func (dm *DataManager) processRegistry(s sources.RIRSource) {
	log.Printf("[WHOIS-%s] Starting scan of bulk WHOIS dump from %s...", s.Name, s.URL)
	rc, err := utils.GetCachedReader(s.URL, true, "[WHOIS-"+s.Name+"]")
	if err != nil {
		log.Printf("[WHOIS-%s] Error fetching data: %v", s.Name, err)
		return
	}
	defer func() { _ = rc.Close() }()

	var totalSize int64
	cacheFileName := utils.GetCacheFileName(s.URL, "[WHOIS-"+s.Name+"]")
	if info, err := os.Stat(filepath.Join("data", "cache", cacheFileName)); err == nil {
		totalSize = info.Size()
	}

	readBytes := &atomic.Uint64{}
	cr := &countingReader{Reader: rc, count: readBytes}
	br := bufio.NewReader(cr)

	header, _ := br.Peek(3)
	var r io.Reader = br
	if len(header) >= 2 && header[0] == 0x1f && header[1] == 0x8b {
		gr, err := gzip.NewReader(br)
		if err != nil {
			log.Printf("[WHOIS-%s] Error creating gzip reader: %v", s.Name, err)
			return
		}
		defer func() { _ = gr.Close() }()
		r = gr
	} else if len(header) >= 3 && header[0] == 'B' && header[1] == 'Z' && header[2] == 'h' {
		r = bzip2.NewReader(br)
	}

	scanner := bufio.NewScanner(r)
	count, cityHits, coordHits, totalProcessed := 0, 0, 0, 0
	recordFields := make(map[string][]string)
	hints := make(map[string]ripeHint)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			count += dm.flushWhoisRecord(recordFields, hints, &cityHits, &coordHits)
			totalProcessed++
			if totalProcessed%100000 == 0 {
				pct := 0.0
				if totalSize > 0 {
					pct = float64(readBytes.Load()) / float64(totalSize) * 100
				}
				log.Printf("[WHOIS-%s] Processed %d records... %.1f%% (City hits: %d, Coord hits: %d)", s.Name, totalProcessed, pct, cityHits, coordHits)
			}
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])
		recordFields[key] = append(recordFields[key], val)
	}
	log.Printf("[WHOIS-%s] Scan complete. Loaded %d granular hints.", s.Name, count)
	if len(hints) > 0 {
		log.Printf("[WHOIS-%s] Final flush of %d hints to disk trie...", s.Name, len(hints))
		dm.geo.SetRIPEHintsCIDR(hints)
	}
}

func (dm *DataManager) flushWhoisRecord(recordFields map[string][]string, hints map[string]ripeHint, cityHits, coordHits *int) int {
	if len(recordFields) == 0 {
		return 0
	}
	defer func() {
		for k := range recordFields {
			delete(recordFields, k)
		}
	}()

	hint, ok := dm.processWhoisRecord(recordFields, cityHits, coordHits)
	if !ok {
		return 0
	}

	inetnums := recordFields["inetnum"]
	start, end := dm.parseInetnum(inetnums[0])
	cidrs := utils.RangeToCIDRs(start, end)
	for _, cidr := range cidrs {
		hints[cidr.String()] = hint
	}

	if len(hints) > 100000 {
		dm.geo.SetRIPEHintsCIDR(hints)
		for k := range hints {
			delete(hints, k)
		}
	}
	return 1
}

func (dm *DataManager) processWhoisRecord(recordFields map[string][]string, cityHits, coordHits *int) (ripeHint, bool) {
	inetnums := recordFields["inetnum"]
	countries := recordFields["country"]
	if len(inetnums) == 0 || len(countries) == 0 {
		return ripeHint{}, false
	}
	start, end := dm.parseInetnum(inetnums[0])
	if start == 0 || end == 0 {
		return ripeHint{}, false
	}
	cc := strings.ToUpper(countries[0])
	city := ""
	if cities := recordFields["city"]; len(cities) > 0 {
		city = cities[0]
	}
	if city == "" {
		searchFields := []string{"descr", "netname", "address", "remarks", "org-name", "cust-name", "owner", "organization"}
		for _, f := range searchFields {
			for _, val := range recordFields[f] {
				if found := dm.extractCityHeuristic(val, cc); found != "" {
					city = found
					break
				}
			}
			if city != "" {
				break
			}
		}
	}
	lat, lng := float32(0), float32(0)
	if geolocs := recordFields["geoloc"]; len(geolocs) > 0 {
		if lt, lg, ok := dm.parseGeoloc(geolocs[0]); ok {
			lat, lng = lt, lg
		}
	}
	if (lat == 0 && lng == 0) && city != "" {
		l, ln, _ := dm.geo.ResolveCityToCoords(city, cc)
		lat, lng = float32(l), float32(ln)
	}
	if city != "" {
		*cityHits++
	}
	if lat != 0 || lng != 0 {
		*coordHits++
	}
	return ripeHint{Lat: lat, Lng: lng, CC: cc, City: city}, true
}

func (dm *DataManager) ProcessPeeringDBData() {
	url := sources.GetPeeringDBBackupURL()
	log.Printf("[PeeringDB] Fetching backup from %s...", url)
	reader, err := utils.GetCachedReader(url, true, "[PeeringDB-Backup]")
	if err != nil {
		log.Printf("[PeeringDB] Error fetching backup: %v", err)
		return
	}
	defer func() { _ = reader.Close() }()

	var pdb struct {
		IX struct {
			Data []struct {
				ID   uint32 `json:"id"`
				City string `json:"city"`
				CC   string `json:"country"`
			} `json:"data"`
		} `json:"ix"`
		IXLAN struct {
			Data []struct {
				ID   uint32 `json:"id"`
				IXID uint32 `json:"ix_id"`
			} `json:"data"`
		} `json:"ixlan"`
		IXPFX struct {
			Data []struct {
				Prefix  string `json:"prefix"`
				IXLANID uint32 `json:"ixlan_id"`
			} `json:"data"`
		} `json:"ixpfx"`
	}

	if err := json.NewDecoder(reader).Decode(&pdb); err != nil {
		log.Printf("[PeeringDB] Error decoding backup: %v", err)
		return
	}

	ixMap := make(map[uint32]ripeHint)
	for _, ix := range pdb.IX.Data {
		hint := ripeHint{CC: ix.CC, City: ix.City}
		lat, lng, _ := dm.geo.ResolveCityToCoords(ix.City, ix.CC)
		hint.Lat, hint.Lng = float32(lat), float32(lng)
		ixMap[ix.ID] = hint
	}

	ixlanToIX := make(map[uint32]uint32)
	for _, lan := range pdb.IXLAN.Data {
		ixlanToIX[lan.ID] = lan.IXID
	}

	hints := make(map[string]ripeHint)
	for _, pfx := range pdb.IXPFX.Data {
		if ixID, ok := ixlanToIX[pfx.IXLANID]; ok {
			if hint, ok := ixMap[ixID]; ok && pfx.Prefix != "" {
				hints[pfx.Prefix] = hint
			}
		}
	}

	log.Printf("[PeeringDB] Processed %d IX, %d IXLAN, %d IXPFX", len(pdb.IX.Data), len(pdb.IXLAN.Data), len(pdb.IXPFX.Data))
	dm.geo.SetPeeringHintsCIDR(hints)
	log.Printf("[PeeringDB] Loaded %d prefixes into PeeringDB hints", len(hints))
}

func (dm *DataManager) LoadCloudData() {
	fetchers := []struct {
		name string
		f    func() ([]sources.CloudPrefix, error)
	}{
		{"GCP", sources.FetchGoogleGeofeed},
		{"AWS", sources.FetchAWSRanges},
		{"Azure", sources.FetchAzureRanges},
		{"Oracle", sources.FetchOracleRanges},
		{"DO", sources.FetchDigitalOceanRanges},
	}

	hints := make(map[string][]byte)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, fetcher := range fetchers {
		wg.Add(1)
		go func(f struct {
			name string
			f    func() ([]sources.CloudPrefix, error)
		}) {
			defer wg.Done()
			log.Printf("Fetching %s IP ranges...", f.name)
			prefixes, err := f.f()
			if err == nil {
				log.Printf("Successfully fetched %d %s ranges", len(prefixes), f.name)
				mu.Lock()
				for _, p := range prefixes {
					hint := sources.GetCloudCityHint(p)
					if hint != "" {
						hints[p.Prefix.String()] = []byte(hint)
					}
				}
				mu.Unlock()
			} else {
				log.Printf("Warning: Failed to fetch %s ranges: %v", f.name, err)
			}
		}(fetcher)
	}
	wg.Wait()

	if len(hints) > 0 && dm.geo.cloudHints != nil {
		if err := dm.geo.cloudHints.BatchInsert(hints); err != nil {
			log.Printf("[CLOUD] Error batch inserting cloud hints: %v", err)
		}
		log.Printf("[CLOUD] Loaded %d cloud prefixes into CloudHintsDB", len(hints))
	}
}

func (dm *DataManager) extractCityHeuristic(val, cc string) string {
	if cc == "" {
		return ""
	}
	ccNorm := strings.ToUpper(cc)
	valLower := []byte(strings.ToLower(val))

	// 1. Check for known cities in this country using Aho-Corasick
	dm.geo.dataMu.RLock()
	matcher, hasMatcher := dm.geo.cityMatchers[ccNorm]
	cities := dm.geo.citiesByCountry[ccNorm]
	dm.geo.dataMu.RUnlock()

	if hasMatcher {
		matches := matcher.Match(valLower)
		if len(matches) > 0 {
			// Aho-Corasick matches all occurrences.
			// Our city list was sorted by length DESC, but Aho-Corasick returns
			// matches based on their end position in the text.
			// Let's find the match that corresponds to the longest city name
			// from our original list.
			var bestCity string
			for _, cityIdx := range matches {
				city := cities[cityIdx]
				if len(city) > len(bestCity) {
					// Verify word boundaries for the best candidate
					idx := bytes.Index(valLower, []byte(city))
					if idx != -1 {
						isStart := idx == 0 || !isAlpha(valLower[idx-1])
						isEnd := idx+len(city) == len(valLower) || !isAlpha(valLower[idx+len(city)])
						if isStart && isEnd {
							bestCity = city
						}
					}
				}
			}
			if bestCity != "" {
				return bestCity
			}
		}
	}

	// 2. Fallback to existing pattern-based extraction
	return dm.extractCityFromDescr(val)
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func (dm *DataManager) extractCityFromDescr(descr string) string {
	parts := strings.Split(descr, ",")
	if len(parts) > 1 {
		city := strings.TrimSpace(parts[len(parts)-1])
		if len(city) > 2 && len(city) < 25 {
			return city
		}
	}
	parts = strings.Split(descr, "-")
	if len(parts) > 1 {
		city := strings.TrimSpace(parts[len(parts)-1])
		if len(city) > 2 && len(city) < 25 {
			return city
		}
	}
	words := strings.Fields(descr)
	if len(words) >= 2 {
		last := words[len(words)-1]
		if len(last) == 2 && last == strings.ToUpper(last) {
			city := words[len(words)-2]
			if len(city) > 2 {
				return city
			}
		}
	}
	return ""
}

func (dm *DataManager) parseInetnum(val string) (startIP, endIP uint32) {
	ips := strings.Split(val, "-")
	if len(ips) >= 2 {
		start := net.ParseIP(strings.TrimSpace(ips[0])).To4()
		end := net.ParseIP(strings.TrimSpace(ips[1])).To4()
		if start != nil && end != nil {
			return binary.BigEndian.Uint32(start), binary.BigEndian.Uint32(end)
		}
	}
	return 0, 0
}

func (dm *DataManager) parseGeoloc(val string) (lat, lng float32, ok bool) {
	if _, err := fmt.Sscanf(val, "%f %f", &lat, &lng); err == nil {
		return lat, lng, true
	}
	return 0, 0, false
}
