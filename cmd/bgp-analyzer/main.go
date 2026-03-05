package main

import (
	"bufio"
	"compress/gzip"
	"container/heap"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/osrg/gobgp/v3/pkg/packet/bgp"
	"github.com/osrg/gobgp/v3/pkg/packet/mrt"
	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgpengine/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type MRTMessage struct {
	Timestamp time.Time
	Collector string
	Peer      string
	Message   *bgp.BGPMessage
}

type MRTStream struct {
	collector string
	ch        chan *MRTMessage
	current   *MRTMessage
}

func readMRTFile(collector, path string, ch chan *MRTMessage) {
	f, err := os.Open(path)
	if err != nil {
		log.Printf("Error opening %s: %v", path, err)
		return
	}
	defer func() { _ = f.Close() }()

	gz, err := gzip.NewReader(bufio.NewReader(f))
	if err != nil {
		log.Printf("Error opening gzip %s: %v", path, err)
		return
	}
	defer func() { _ = gz.Close() }()

	for {
		header := make([]byte, mrt.MRT_COMMON_HEADER_LEN)
		_, err := io.ReadFull(gz, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading MRT header from %s: %v", collector, err)
			break
		}

		h := &mrt.MRTHeader{}
		if err := h.DecodeFromBytes(header); err != nil {
			log.Printf("Error decoding MRT header from %s: %v", collector, err)
			break
		}

		body := make([]byte, h.Len)
		if _, err := io.ReadFull(gz, body); err != nil {
			log.Printf("Error reading MRT body from %s: %v", collector, err)
			break
		}

		msg, err := mrt.ParseMRTBody(h, body)
		if err != nil {
			continue
		}

		if msg.Header.Type == mrt.BGP4MP || msg.Header.Type == mrt.BGP4MP_ET {
			subtype := mrt.MRTSubTypeBGP4MP(msg.Header.SubType)
			if subtype == mrt.MESSAGE || subtype == mrt.MESSAGE_AS4 ||
				subtype == mrt.MESSAGE_LOCAL || subtype == mrt.MESSAGE_AS4_LOCAL {

				bgp4mp := msg.Body.(*mrt.BGP4MPMessage)
				ch <- &MRTMessage{
					Timestamp: msg.Header.GetTime(),
					Collector: collector,
					Peer:      fmt.Sprintf("%d", bgp4mp.PeerAS),
					Message:   bgp4mp.BGPMessage,
				}
			}
		}
	}
}

type StreamHeap []*MRTStream

func (h StreamHeap) Len() int            { return len(h) }
func (h StreamHeap) Less(i, j int) bool  { return h[i].current.Timestamp.Before(h[j].current.Timestamp) }
func (h StreamHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *StreamHeap) Push(x interface{}) { *h = append(*h, x.(*MRTStream)) }
func (h *StreamHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type WorkerTask struct {
	msg    *MRTMessage
	update *bgp.BGPUpdate
}

func main() {
	cfg := parseFlags()

	geo, asnMapping, rpki := setupDependencies()
	defer func() { _ = geo.Close() }()

	csvWriter, closeCSV := setupCSVWriter(cfg.csvFile)
	defer closeCSV()

	// Custom TimeProvider (shared, atomic update)
	var currentTime int64
	timeProvider := func() time.Time {
		return time.Unix(currentTime, 0)
	}

	masterClassifier := bgpengine.NewClassifier(nil, nil, asnMapping, rpki, prefixToIP, nil, timeProvider)

	runReplay(&cfg, timeProvider, &currentTime, masterClassifier, csvWriter)

	// Write Summary
	writeSummary(cfg.summaryFile, masterClassifier)
}

type analyzerConfig struct {
	startTime   time.Time
	endTime     time.Time
	rrcs        []string
	csvFile     string
	summaryFile string
	cacheDir    string
	numWorkers  int
}

func parseFlags() analyzerConfig {
	startFlag := flag.String("start", "", "Start time (YYYY-MM-DD HH:mm)")
	endFlag := flag.String("end", "", "End time (YYYY-MM-DD HH:mm)")
	defaultRRCs := []string{}
	for i := 0; i <= 26; i++ {
		defaultRRCs = append(defaultRRCs, fmt.Sprintf("rrc%02d", i))
	}
	rrcsFlag := flag.String("rrcs", strings.Join(defaultRRCs, ","), "Comma-separated list of RRCs")
	csvFile := flag.String("csv", "transitions.csv", "Output CSV file for state transitions")
	summaryFile := flag.String("summary", "summary.txt", "Output text summary")
	cacheDir := flag.String("cache", "data/mrt-cache", "Directory for cached MRT files")
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Number of parallel classification workers")
	flag.Parse()

	if *startFlag == "" || *endFlag == "" {
		flag.Usage()
		os.Exit(1)
	}

	startTime, err := time.Parse("2006-01-02 15:04", *startFlag)
	if err != nil {
		log.Fatalf("Invalid start time: %v", err)
	}
	endTime, err := time.Parse("2006-01-02 15:04", *endFlag)
	if err != nil {
		log.Fatalf("Invalid end time: %v", err)
	}

	return analyzerConfig{
		startTime:   startTime,
		endTime:     endTime,
		rrcs:        strings.Split(*rrcsFlag, ","),
		csvFile:     *csvFile,
		summaryFile: *summaryFile,
		cacheDir:    *cacheDir,
		numWorkers:  *numWorkers,
	}
}

func setupDependencies() (*geoservice.GeoService, *utils.ASNMapping, *utils.RPKIManager) {
	geo := geoservice.NewGeoService(3840, 2160, 760.0)
	if err := geo.OpenHintDBs("data", true); err != nil {
		log.Printf("Warning: failed to open hint databases: %v", err)
	}
	dm := geoservice.NewDataManager(geo)
	dm.LoadWorldCities()
	_ = dm.LoadRemoteCityData()

	asnMapping := utils.NewASNMapping()
	_ = asnMapping.Load()

	rpki, _ := utils.NewRPKIManager("./data/rpki-vrps.db")
	return geo, asnMapping, rpki
}

func setupCSVWriter(csvFile string) (writer *csv.Writer, closer func()) {
	fCsv, err := os.Create(csvFile)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	csvWriter := csv.NewWriter(fCsv)
	_ = csvWriter.Write([]string{"timestamp", "prefix", "old_type", "new_type", "origin_asn"})

	return csvWriter, func() {
		csvWriter.Flush()
		_ = fCsv.Close()
	}
}

var csvMu sync.Mutex

func runReplay(cfg *analyzerConfig, timeProvider bgpengine.TimeProvider, currentTime *int64, masterClassifier *bgpengine.Classifier, csvWriter *csv.Writer) {
	// Sharded Classifier State
	workers := make([]chan WorkerTask, cfg.numWorkers)
	var wg sync.WaitGroup

	asnMapping := masterClassifier.GetASNMapping()
	rpki := masterClassifier.GetRPKIManager()

	for i := 0; i < cfg.numWorkers; i++ {
		workers[i] = make(chan WorkerTask, 1000)
		wg.Add(1)
		go func(ch chan WorkerTask) {
			defer wg.Done()
			// Each worker has its own local state cache to avoid locks
			localPrefixStates := utils.NewLRUCache[string, *bgpproto.PrefixState](1000000 / cfg.numWorkers)
			localClassifier := bgpengine.NewClassifier(nil, nil, asnMapping, rpki, prefixToIP, localPrefixStates, timeProvider)

			for task := range ch {
				processUpdate(localClassifier, masterClassifier, task.msg, task.update, csvWriter, &csvMu)
			}
		}(workers[i])
	}

	// Prepare MRT Streams (Parallel Readers)
	h := &StreamHeap{}
	heap.Init(h)

	for _, rrc := range cfg.rrcs {
		files := getMRTFiles(rrc, cfg.startTime, cfg.endTime)
		for _, remoteURL := range files {
			localPath := filepath.Join(cfg.cacheDir, filepath.Base(remoteURL))
			if err := downloadFile(remoteURL, localPath); err != nil {
				log.Printf("Failed to download %s: %v", remoteURL, err)
				continue
			}

			ch := make(chan *MRTMessage, 1000)
			go func(c, p string, out chan *MRTMessage) {
				readMRTFile(c, p, out)
				close(out)
			}(rrc, localPath, ch)

			if msg, ok := <-ch; ok {
				heap.Push(h, &MRTStream{collector: rrc, ch: ch, current: msg})
			}
		}
	}

	log.Printf("Starting parallel replay with %d workers...", cfg.numWorkers)

	count := 0
	lastSecond := int64(0)
	messagesThisSecond := 0
	for h.Len() > 0 {
		stream := heap.Pop(h).(*MRTStream)
		msg := stream.current
		atomic.StoreInt64(currentTime, msg.Timestamp.Unix())
		ts := atomic.LoadInt64(currentTime)

		if lastSecond == 0 {
			lastSecond = ts
		}

		if ts > lastSecond {
			if messagesThisSecond > 0 {
				log.Printf("[%s] Rate: %d msg/s", time.Unix(lastSecond, 0).Format("15:04:05"), messagesThisSecond)
			}
			lastSecond = ts
			messagesThisSecond = 0
		}
		messagesThisSecond++

		if msg.Message.Header.Type == bgp.BGP_MSG_UPDATE {
			update := msg.Message.Body.(*bgp.BGPUpdate)
			dispatchUpdate(update, msg, workers, cfg.numWorkers)
		}

		if next, ok := <-stream.ch; ok {
			stream.current = next
			heap.Push(h, stream)
		}

		count++
	}

	// Close workers and wait
	for i := 0; i < cfg.numWorkers; i++ {
		close(workers[i])
	}
	wg.Wait()
	log.Printf("Done. Processed %d messages.", count)
}

func dispatchUpdate(update *bgp.BGPUpdate, msg *MRTMessage, workers []chan WorkerTask, numWorkers int) {
	// Announcements
	for _, nlri := range update.NLRI {
		prefix := nlri.String()
		ip := prefixToIP(prefix)
		if ip == 0 && !strings.HasPrefix(prefix, "0.0.0.0") {
			continue // Ignore IPv6 or invalid
		}
		workerID := utils.HashUint32(ip) % uint32(numWorkers)
		workers[workerID] <- WorkerTask{
			msg: msg,
			update: &bgp.BGPUpdate{
				PathAttributes: update.PathAttributes,
				NLRI:           []*bgp.IPAddrPrefix{nlri},
			},
		}
	}

	// Withdrawals
	for _, nlri := range update.WithdrawnRoutes {
		prefix := nlri.String()
		ip := prefixToIP(prefix)
		if ip == 0 && !strings.HasPrefix(prefix, "0.0.0.0") {
			continue // Ignore IPv6 or invalid
		}
		workerID := utils.HashUint32(ip) % uint32(numWorkers)
		workers[workerID] <- WorkerTask{
			msg: msg,
			update: &bgp.BGPUpdate{
				PathAttributes:  update.PathAttributes,
				WithdrawnRoutes: []*bgp.IPAddrPrefix{nlri},
			},
		}
	}
}

func getMRTFiles(rrc string, start, end time.Time) []string {
	var files []string
	current := start.Truncate(5 * time.Minute)
	for current.Before(end) {
		url := fmt.Sprintf("https://data.ris.ripe.net/%s/%s/updates.%s.gz",
			rrc, current.Format("2006.01"), current.Format("20060102.1504"))
		files = append(files, url)
		current = current.Add(5 * time.Minute)
	}
	return files
}

func downloadFile(url, path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	log.Printf("Downloading %s...", url)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(f, resp.Body)
	return err
}

func prefixToIP(p string) uint32 {
	parts := strings.Split(p, "/")
	ipStr := parts[0]
	parsedIP := net.ParseIP(ipStr)
	if parsedIP == nil {
		return 0
	}
	return utils.IPToUint32(parsedIP)
}

func processUpdate(localClassifier, masterClassifier *bgpengine.Classifier, msg *MRTMessage, update *bgp.BGPUpdate, writer *csv.Writer, csvMu *sync.Mutex) {
	ctx := &bgpengine.MessageContext{
		Peer: msg.Peer,
		Host: msg.Collector,
		Now:  msg.Timestamp,
	}

	for _, attr := range update.PathAttributes {
		switch a := attr.(type) {
		case *bgp.PathAttributeAsPath:
			ctx.PathLen = 0
			var asns []string
			for _, param := range a.Value {
				for _, asn := range param.GetAS() {
					asns = append(asns, fmt.Sprintf("%d", asn))
					ctx.PathLen++
					ctx.OriginASN = asn
				}
			}
			ctx.PathStr = "[" + strings.Join(asns, " ") + "]"
		case *bgp.PathAttributeNextHop:
			ctx.NextHop = a.Value.String()
		case *bgp.PathAttributeAggregator:
			ctx.Aggregator = fmt.Sprintf("AS%d:%s", a.Value.AS, a.Value.Address.String())
		case *bgp.PathAttributeMultiExitDisc:
			ctx.Med = int32(a.Value)
		case *bgp.PathAttributeLocalPref:
			ctx.LocalPref = int32(a.Value)
		case *bgp.PathAttributeCommunities:
			var comms []string
			for _, c := range a.Value {
				comms = append(comms, fmt.Sprintf("%d:%d", (c>>16)&0xffff, c&0xffff))
			}
			ctx.CommStr = "[" + strings.Join(comms, " ") + "]"
		}
	}

	// Announcements
	for _, nlri := range update.NLRI {
		prefix := nlri.String()
		handlePrefix(localClassifier, masterClassifier, prefix, ctx, writer, csvMu)
	}

	// Withdrawals
	ctx.IsWithdrawal = true
	for _, nlri := range update.WithdrawnRoutes {
		prefix := nlri.String()
		handlePrefix(localClassifier, masterClassifier, prefix, ctx, writer, csvMu)
	}
}

func handlePrefix(localClassifier, masterClassifier *bgpengine.Classifier, prefix string, ctx *bgpengine.MessageContext, writer *csv.Writer, csvMu *sync.Mutex) {
	oldType := bgpengine.ClassificationNone
	state, ok := localClassifier.GetPrefixState(prefix)
	if ok {
		oldType = bgpengine.ClassificationType(state.ClassifiedType)
	}

	ev, classified := localClassifier.ClassifyEvent(prefix, ctx)

	if classified {
		state, _ = localClassifier.GetPrefixState(prefix)
		newType := bgpengine.ClassificationType(state.ClassifiedType)

		if oldType != newType {
			// Record the classification in the master classifier for the summary
			masterClassifier.RecordClassification(prefix, state, newType, ctx.Now.Unix(), ctx, ev.LeakDetail)

			csvMu.Lock()
			_ = writer.Write([]string{
				ctx.Now.Format(time.RFC3339),
				prefix,
				oldType.String(),
				newType.String(),
				fmt.Sprintf("%d", ctx.OriginASN),
			})
			csvMu.Unlock()
		}
	}
}

func writeSummary(path string, c *bgpengine.Classifier) {
	stats, total := c.GetClassificationStats()
	f, err := os.Create(path)
	if err != nil {
		log.Printf("Failed to create summary file: %v", err)
		return
	}
	defer func() { _ = f.Close() }()

	_, _ = fmt.Fprintf(f, "BGP Analysis Summary\n")
	_, _ = fmt.Fprintf(f, "====================\n\n")
	_, _ = fmt.Fprintf(f, "Total Classification Events: %d\n\n", total)
	_, _ = fmt.Fprintf(f, "Breakdown:\n")

	var keys []int
	for k := range stats {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	for _, k := range keys {
		ct := bgpengine.ClassificationType(k)
		_, _ = fmt.Fprintf(f, "  %-20s: %d\n", ct.String(), stats[ct])
	}
}
