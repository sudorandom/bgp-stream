package main

import (
	"bufio"
	"compress/gzip"
	"container/heap"
	"encoding/csv"
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
	bgp_pkg "github.com/sudorandom/bgp-stream/pkg/bgp"
	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgp/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type AnalyzeCmd struct {
	Start   string `required:"" help:"Start time (YYYY-MM-DD HH:mm)"`
	End     string `required:"" help:"End time (YYYY-MM-DD HH:mm)"`
	RRCs    string `default:"" help:"Comma-separated list of RRCs (e.g. rrc00,rrc01). Defaults to all 27."`
	CSV     string `default:"transitions.csv" help:"Output CSV file for state transitions"`
	Summary string `default:"summary.txt" help:"Output text summary"`
	Cache   string `default:"data/mrt-cache" help:"Directory for cached MRT files"`
	Workers int    `default:"0" help:"Number of parallel classification workers (default: runtime.NumCPU())"`
}

func (c *AnalyzeCmd) Run() error {
	startTime, err := time.Parse("2006-01-02 15:04", c.Start)
	if err != nil {
		return fmt.Errorf("invalid start time: %v", err)
	}
	endTime, err := time.Parse("2006-01-02 15:04", c.End)
	if err != nil {
		return fmt.Errorf("invalid end time: %v", err)
	}

	rrcs := strings.Split(c.RRCs, ",")
	if c.RRCs == "" {
		rrcs = []string{}
		for i := 0; i <= 26; i++ {
			rrcs = append(rrcs, fmt.Sprintf("rrc%02d", i))
		}
	}

	numWorkers := c.Workers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}

	geo, asnMapping, rpki := setupDependencies()
	defer func() { _ = geo.Close() }()

	csvWriter, closeCSV := setupCSVWriter(c.CSV)
	defer closeCSV()

	// Custom TimeProvider (shared, atomic update)
	var currentTime int64
	timeProvider := func() time.Time {
		return time.Unix(currentTime, 0)
	}

	masterClassifier := bgp_pkg.NewClassifier(nil, nil, asnMapping, rpki, prefixToIP, nil, timeProvider)

	runReplay(startTime, endTime, rrcs, c.Cache, numWorkers, timeProvider, &currentTime, masterClassifier, csvWriter)

	writeSummary(c.Summary, masterClassifier)
	return nil
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

func runReplay(startTime, endTime time.Time, rrcs []string, cacheDir string, numWorkers int, timeProvider bgp_pkg.TimeProvider, currentTime *int64, masterClassifier *bgp_pkg.Classifier, csvWriter *csv.Writer) {
	workers := make([]chan WorkerTask, numWorkers)
	var wg sync.WaitGroup

	asnMapping := masterClassifier.GetASNMapping()
	rpki := masterClassifier.GetRPKIManager()

	for i := 0; i < numWorkers; i++ {
		workers[i] = make(chan WorkerTask, 1000)
		wg.Add(1)
		go func(ch chan WorkerTask) {
			defer wg.Done()
			localPrefixStates := utils.NewLRUCache[string, *bgpproto.PrefixState](1000000 / numWorkers)
			localClassifier := bgp_pkg.NewClassifier(nil, nil, asnMapping, rpki, prefixToIP, localPrefixStates, timeProvider)

			for task := range ch {
				processUpdate(localClassifier, masterClassifier, task.msg, task.update, csvWriter, &csvMu)
			}
		}(workers[i])
	}

	h := &StreamHeap{}
	heap.Init(h)

	for _, rrc := range rrcs {
		files := getMRTFiles(rrc, startTime, endTime)
		for _, remoteURL := range files {
			localPath := filepath.Join(cacheDir, filepath.Base(remoteURL))
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

	log.Printf("Starting parallel replay with %d workers...", numWorkers)

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
			dispatchUpdate(update, msg, workers, numWorkers)
		}

		if next, ok := <-stream.ch; ok {
			stream.current = next
			heap.Push(h, stream)
		}

		count++
	}

	for i := 0; i < numWorkers; i++ {
		close(workers[i])
	}
	wg.Wait()
	log.Printf("Done. Processed %d messages.", count)
}

func dispatchUpdate(update *bgp.BGPUpdate, msg *MRTMessage, workers []chan WorkerTask, numWorkers int) {
	for _, nlri := range update.NLRI {
		prefix := nlri.String()
		ip := prefixToIP(prefix)
		if ip == 0 && !strings.HasPrefix(prefix, "0.0.0.0") {
			continue
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

	for _, nlri := range update.WithdrawnRoutes {
		prefix := nlri.String()
		ip := prefixToIP(prefix)
		if ip == 0 && !strings.HasPrefix(prefix, "0.0.0.0") {
			continue
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

func processUpdate(localClassifier, masterClassifier *bgp_pkg.Classifier, msg *MRTMessage, update *bgp.BGPUpdate, writer *csv.Writer, csvMu *sync.Mutex) {
	ctx := &bgp_pkg.MessageContext{
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

	for _, nlri := range update.NLRI {
		prefix := nlri.String()
		handlePrefix(localClassifier, masterClassifier, prefix, ctx, writer, csvMu)
	}

	ctx.IsWithdrawal = true
	for _, nlri := range update.WithdrawnRoutes {
		prefix := nlri.String()
		handlePrefix(localClassifier, masterClassifier, prefix, ctx, writer, csvMu)
	}
}

func handlePrefix(localClassifier, masterClassifier *bgp_pkg.Classifier, prefix string, ctx *bgp_pkg.MessageContext, writer *csv.Writer, csvMu *sync.Mutex) {
	oldType := bgp_pkg.ClassificationNone
	state, ok := localClassifier.GetPrefixState(prefix)
	if ok {
		oldType = bgp_pkg.ClassificationType(state.ClassifiedType)
	}

	ev, classified := localClassifier.ClassifyEvent(prefix, ctx)

	if classified {
		state, _ = localClassifier.GetPrefixState(prefix)
		newType := bgp_pkg.ClassificationType(state.ClassifiedType)

		if oldType != newType {
			masterClassifier.RecordClassification(prefix, state, newType, ctx.Now.Unix(), ctx, ev.HistoricalASN, ev.LeakDetail)

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

func writeSummary(path string, c *bgp_pkg.Classifier) {
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
		ct := bgp_pkg.ClassificationType(k)
		_, _ = fmt.Fprintf(f, "  %-20s: %d\n", ct.String(), stats[ct])
	}
}

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
