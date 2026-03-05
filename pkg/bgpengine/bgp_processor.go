package bgpengine

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgpengine/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type BGPEventCallback func(lat, lng float64, cc string, eventType EventType, classificationType ClassificationType, prefix string, asn uint32, leakDetail ...*LeakDetail)
type IPCoordsProvider func(ip uint32) (float64, float64, string, geoservice.ResolutionType)
type PrefixToIPConverter func(p string) uint32
type TimeProvider func() time.Time

type RISMessageData struct {
	Announcements []struct {
		NextHop  string   `json:"next_hop"`
		Prefixes []string `json:"prefixes"`
	} `json:"announcements"`
	Withdrawals []string          `json:"withdrawals"`
	Path        []json.RawMessage `json:"path"`
	Community   [][]interface{}   `json:"community"`
	Aggregator  string            `json:"aggregator"`
	Peer        string            `json:"peer"`
	Host        string            `json:"host"`
	Med         int32             `json:"med"`
	LocalPref   int32             `json:"local_pref"`
}

type processorWorker struct {
	classifier   *Classifier
	recentlySeen *utils.LRUCache[uint32, struct {
		Time time.Time
		Type EventType
	}]
	pendingWithdrawals map[uint32]struct {
		Time   time.Time
		Prefix string
	}
	taskCh chan *RISMessageData
}

type BGPProcessor struct {
	geo          IPCoordsProvider
	seenDB       *utils.DiskTrie
	stateDB      *utils.DiskTrie
	asnMapping   *utils.ASNMapping
	rpki         *utils.RPKIManager
	onEvent      BGPEventCallback
	prefixToIP   PrefixToIPConverter
	timeProvider TimeProvider

	workers []*processorWorker

	msgCount        atomic.Uint64
	collectorCounts sync.Map // map[string]*atomic.Uint64
	lastRateReport  time.Time
	mu              sync.Mutex
	stopping        atomic.Bool
}

func NewBGPProcessor(geo IPCoordsProvider, seenDB, stateDB *utils.DiskTrie, asnMapping *utils.ASNMapping, rpki *utils.RPKIManager, prefixToIP PrefixToIPConverter, timeProvider TimeProvider, onEvent BGPEventCallback) *BGPProcessor {
	numWorkers := runtime.NumCPU()
	if numWorkers < 4 {
		numWorkers = 4
	}

	p := &BGPProcessor{
		geo:            geo,
		seenDB:         seenDB,
		stateDB:        stateDB,
		asnMapping:     asnMapping,
		rpki:           rpki,
		onEvent:        onEvent,
		prefixToIP:     prefixToIP,
		timeProvider:   timeProvider,
		lastRateReport: time.Now(),
		workers:        make([]*processorWorker, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		prefixStates := utils.NewLRUCache[string, *bgpproto.PrefixState](1000000 / numWorkers)
		p.workers[i] = &processorWorker{
			classifier: NewClassifier(seenDB, stateDB, asnMapping, rpki, prefixToIP, prefixStates, timeProvider),
			recentlySeen: utils.NewLRUCache[uint32, struct {
				Time time.Time
				Type EventType
			}](1000000 / numWorkers),
			pendingWithdrawals: make(map[uint32]struct {
				Time   time.Time
				Prefix string
			}),
			taskCh: make(chan *RISMessageData, 10000),
		}
		go p.runWorker(p.workers[i])
	}

	return p
}

func (p *BGPProcessor) runWorker(w *processorWorker) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		if p.isStopping() {
			return
		}
		select {
		case data, ok := <-w.taskCh:
			if !ok {
				return
			}
			events := p.handleRISMessage(w, data)
			for _, e := range events {
				if lat, lng, cc, _ := p.geo(e.ip); cc != "" {
					p.onEvent(lat, lng, cc, e.eventType, e.classificationType, e.prefix, e.asn, e.leakDetail)
				}
			}
		case <-ticker.C:
			p.processWorkerWithdrawals(w)
		}
	}
}

func (p *BGPProcessor) processWorkerWithdrawals(w *processorWorker) {
	now := p.timeProvider()
	for ip, entry := range w.pendingWithdrawals {
		if now.After(entry.Time) {
			if lat, lng, cc, _ := p.geo(ip); cc != "" {
				p.onEvent(lat, lng, cc, EventWithdrawal, ClassificationNone, entry.Prefix, 0, nil)
				w.recentlySeen.Add(ip, struct {
					Time time.Time
					Type EventType
				}{Time: now, Type: EventWithdrawal})
			}
			delete(w.pendingWithdrawals, ip)
		}
	}
}

func (p *BGPProcessor) Close() {
	p.stopping.Store(true)
}

func (p *BGPProcessor) isStopping() bool {
	return p.stopping.Load()
}

const dedupeWindow = 15 * time.Second
const withdrawResolutionWindow = 10 * time.Second

func (p *BGPProcessor) Listen() {
	for i := 0; i <= 26; i++ {
		rrc := fmt.Sprintf("rrc%02d", i)
		go p.listenToCollector(rrc)
	}
}

func (p *BGPProcessor) listenToCollector(rrc string) {
	backoff := 1 * time.Second
	for {
		if p.isStopping() {
			return
		}
		c, err := p.connectAndSubscribe(rrc)
		if err != nil {
			log.Printf("[%s] Connection error: %v. Retrying in %v...", rrc, err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			continue
		}
		backoff = 1 * time.Second

		p.runMessageLoop(c, rrc)
		_ = c.Close()
		time.Sleep(time.Second)
	}
}

func (p *BGPProcessor) connectAndSubscribe(rrc string) (*websocket.Conn, error) {
	url := "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream"
	c, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	subscribeMsg := fmt.Sprintf(`{"type": "ris_subscribe", "data": {"type": "UPDATE", "host": "%s", "prefix": "0.0.0.0/0", "moreSpecific": true}}`, rrc)
	if err := c.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

type pendingEvent struct {
	ip                 uint32
	prefix             string
	asn                uint32
	eventType          EventType
	classificationType ClassificationType
	leakDetail         *LeakDetail
}

func (p *BGPProcessor) runMessageLoop(c *websocket.Conn, rrc string) {
	for {
		if p.isStopping() {
			return
		}
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Printf("[%s] Read error: %v. Reconnecting...", rrc, err)
			return
		}
		var msg struct {
			Type string         `json:"type"`
			Data RISMessageData `json:"data"`
		}
		if json.Unmarshal(message, &msg) != nil {
			continue
		}

		if msg.Type == "ris_error" {
			log.Printf("[RIS ERROR %s] %s", rrc, string(message))
			continue
		}
				if msg.Type == "ris_message" {
					p.msgCount.Add(1)
					
					actual, _ := p.collectorCounts.LoadOrStore(rrc, &atomic.Uint64{})
					actual.(*atomic.Uint64).Add(1)
		
					// Dispatch based on prefix. Since one message can have multiple prefixes,			// we might need to send it to multiple workers or split it.
			// To keep consistency and simplicity, we split it per prefix if they belong to different workers.
			p.dispatchMessage(&msg.Data)
		}
	}
}

func (p *BGPProcessor) dispatchMessage(data *RISMessageData) {
	if p.isStopping() {
		return
	}
	// Group prefixes by worker
	workerTasks := make(map[int]*RISMessageData)

	getWorker := func(prefix string) int {
		ip := p.prefixToIP(prefix)
		return int(utils.HashUint32(ip) % uint32(len(p.workers)))
	}

	for _, ann := range data.Announcements {
		for _, prefix := range ann.Prefixes {
			wIdx := getWorker(prefix)
			if _, ok := workerTasks[wIdx]; !ok {
				workerTasks[wIdx] = &RISMessageData{
					Peer: data.Peer, Host: data.Host, Path: data.Path,
					Community: data.Community, Aggregator: data.Aggregator,
					Med: data.Med, LocalPref: data.LocalPref,
				}
			}
			// Find or create announcement group for this worker task
			found := false
			for i := range workerTasks[wIdx].Announcements {
				if workerTasks[wIdx].Announcements[i].NextHop == ann.NextHop {
					workerTasks[wIdx].Announcements[i].Prefixes = append(workerTasks[wIdx].Announcements[i].Prefixes, prefix)
					found = true
					break
				}
			}
			if !found {
				workerTasks[wIdx].Announcements = append(workerTasks[wIdx].Announcements, struct {
					NextHop  string   `json:"next_hop"`
					Prefixes []string `json:"prefixes"`
				}{NextHop: ann.NextHop, Prefixes: []string{prefix}})
			}
		}
	}

	for _, prefix := range data.Withdrawals {
		wIdx := getWorker(prefix)
		if _, ok := workerTasks[wIdx]; !ok {
			workerTasks[wIdx] = &RISMessageData{
				Peer: data.Peer, Host: data.Host, Path: data.Path,
				Community: data.Community, Aggregator: data.Aggregator,
				Med: data.Med, LocalPref: data.LocalPref,
			}
		}
		workerTasks[wIdx].Withdrawals = append(workerTasks[wIdx].Withdrawals, prefix)
	}

	for wIdx, task := range workerTasks {
		p.workers[wIdx].taskCh <- task
	}
}

func (p *BGPProcessor) ReportProcessorMetrics() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(p.lastRateReport).Seconds()
	if elapsed <= 0 {
		elapsed = 1.0
	}

	globalCount := p.msgCount.Swap(0)
	globalRate := float64(globalCount) / elapsed
	log.Printf("[RIS] Global Message Rate: %.2f msg/s", globalRate)

	var sb strings.Builder
	sb.WriteString("[BGP-PROC]")
	for i, w := range p.workers {
		fmt.Fprintf(&sb, " W%d:%d", i, len(w.taskCh))
	}
	log.Println(sb.String())

	// Top collectors
	type colRate struct {
		name string
		rate float64
	}
	var rates []colRate
	p.collectorCounts.Range(func(key, value interface{}) bool {
		count := value.(*atomic.Uint64).Swap(0)
		rates = append(rates, colRate{key.(string), float64(count) / elapsed})
		return true
	})
	sort.Slice(rates, func(i, j int) bool { return rates[i].rate > rates[j].rate })

	if len(rates) > 0 {
		sb.Reset()
		sb.WriteString("[BGP-COLL]")
		for i, r := range rates {
			if i >= 10 { // Only show top 10
				break
			}
			fmt.Fprintf(&sb, " %s:%.1f", r.name, r.rate)
		}
		sb.WriteString(" (msg/s)")
		log.Println(sb.String())
	}

	p.lastRateReport = now
}

func (p *BGPProcessor) handleRISMessage(w *processorWorker, data *RISMessageData) []pendingEvent {
	var originASN uint32
	if len(data.Path) > 0 {
		last := data.Path[len(data.Path)-1]
		var asn uint32
		if err := json.Unmarshal(last, &asn); err == nil {
			originASN = asn
		}
	}

	var events []pendingEvent
	now := p.timeProvider()
	events = append(events, p.handleWithdrawals(w, data.Withdrawals, originASN, now)...)
	events = append(events, p.handleAnnouncements(w, data.Announcements, originASN, now)...)

	ctx := &MessageContext{
		Peer:       data.Peer,
		Aggregator: data.Aggregator,
		Host:       data.Host,
		OriginASN:  originASN,
		Med:        data.Med,
		LocalPref:  data.LocalPref,
		Now:        now,
	}
	if len(data.Path) > 0 {
		ctx.PathLen = len(data.Path)
		pathParts := make([]string, len(data.Path))
		for i, part := range data.Path {
			pathParts[i] = string(part)
		}
		ctx.PathStr = "[" + strings.Join(pathParts, " ") + "]"
	}
	if len(data.Community) > 0 {
		ctx.CommStr = fmt.Sprintf("%v", data.Community)
	}

	if len(data.Withdrawals) > 0 {
		ctx.IsWithdrawal = true
		ctx.NumPrefixes = len(data.Withdrawals)
		for _, prefix := range data.Withdrawals {
			if e, ok := w.classifier.ClassifyEvent(prefix, ctx); ok {
				events = append(events, e)
			}
		}
	}

	ctx.IsWithdrawal = false
	for _, ann := range data.Announcements {
		ctx.NumPrefixes = len(ann.Prefixes)
		ctx.NextHop = ann.NextHop
		for _, prefix := range ann.Prefixes {
			if e, ok := w.classifier.ClassifyEvent(prefix, ctx); ok {
				events = append(events, e)
			}
		}
	}
	return events
}

func (p *BGPProcessor) handleWithdrawals(w *processorWorker, withdrawals []string, originASN uint32, now time.Time) []pendingEvent {
	var events []pendingEvent
	for _, prefix := range withdrawals {
		ip := p.prefixToIP(prefix)
		if ip == 0 {
			continue
		}

		if last, ok := w.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
			events = append(events, pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventGossip, classificationType: ClassificationNone})
			continue
		}

		w.pendingWithdrawals[ip] = struct {
			Time   time.Time
			Prefix string
		}{Time: now.Add(withdrawResolutionWindow), Prefix: prefix}
	}
	return events
}

func (p *BGPProcessor) handleAnnouncements(w *processorWorker, announcements []struct {
	NextHop  string   `json:"next_hop"`
	Prefixes []string `json:"prefixes"`
}, originASN uint32, now time.Time) []pendingEvent {
	var events []pendingEvent
	for _, ann := range announcements {
		for _, prefix := range ann.Prefixes {
			if e, ok := p.processAnnouncement(w, prefix, originASN, now); ok {
				events = append(events, e)
			}
		}
	}
	return events
}

func (p *BGPProcessor) processAnnouncement(w *processorWorker, prefix string, originASN uint32, now time.Time) (pendingEvent, bool) {
	ip := p.prefixToIP(prefix)
	if ip == 0 {
		return pendingEvent{}, false
	}

	if last, ok := w.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
		w.recentlySeen.Add(ip, struct {
			Time time.Time
			Type EventType
		}{Time: now, Type: EventUpdate})
		return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventUpdate, classificationType: ClassificationNone}, true
	}

	if last, ok := w.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && (last.Type == EventNew || last.Type == EventUpdate || last.Type == EventGossip) {
		return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventGossip, classificationType: ClassificationNone}, true
	}

	if _, ok := w.pendingWithdrawals[ip]; ok {
		delete(w.pendingWithdrawals, ip)
		w.recentlySeen.Add(ip, struct {
			Time time.Time
			Type EventType
		}{Time: now, Type: EventUpdate})
		return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventUpdate, classificationType: ClassificationNone}, true
	}

	return p.handleNewOrUpdate(w, prefix, ip, originASN, now)
}

func (p *BGPProcessor) handleNewOrUpdate(w *processorWorker, prefix string, ip, originASN uint32, now time.Time) (pendingEvent, bool) {
	isNew := true
	if p.seenDB != nil {
		if val, _ := p.seenDB.Get(prefix); val != nil {
			isNew = false
		}
	}

	eventType := EventUpdate
	if isNew {
		eventType = EventNew
	}

	w.recentlySeen.Add(ip, struct {
		Time time.Time
		Type EventType
	}{Time: now, Type: eventType})

	return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: eventType, classificationType: ClassificationNone}, true
}

func (p *BGPProcessor) GetClassificationStats() (stats map[ClassificationType]int, totalEvents int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	combinedStats := make(map[ClassificationType]int)
	total := 0
	for _, w := range p.workers {
		s, t := w.classifier.GetClassificationStats()
		for k, v := range s {
			combinedStats[k] += v
		}
		total += t
	}
	return combinedStats, total
}

func (p *BGPProcessor) SyncRPKI() error {
	if p.rpki == nil {
		return fmt.Errorf("rpki manager not initialized")
	}
	return p.rpki.Sync()
}
