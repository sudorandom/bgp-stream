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

type BGPEventCallback func(lat, lng float64, cc, city string, eventType EventType, classificationType ClassificationType, prefix string, asn uint32, leakDetail ...*LeakDetail)
type IPCoordsProvider func(ip uint32) (float64, float64, string, string, geoservice.ResolutionType)
type PrefixToIPConverter func(p string) uint32
type TimeProvider func() time.Time

type RISAnnouncement struct {
	NextHop  string   `json:"next_hop"`
	Prefixes []string `json:"prefixes"`
}

type RISMessageData struct {
	Announcements []RISAnnouncement `json:"announcements"`
	Withdrawals   []string          `json:"withdrawals"`
	Path          []json.RawMessage `json:"path"`
	Community     [][]interface{}   `json:"community"`
	Aggregator    string            `json:"aggregator"`
	Peer          string            `json:"peer"`
	Host          string            `json:"host"`
	Med           int32             `json:"med"`
	LocalPref     int32             `json:"local_pref"`
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
	conns           sync.Map // map[string]*websocket.Conn
	stopCh          chan struct{}
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
		stopCh:         make(chan struct{}),
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
				if lat, lng, cc, city, _ := p.geo(e.IP); cc != "" {
					p.onEvent(lat, lng, cc, city, e.EventType, e.ClassificationType, e.Prefix, e.ASN, e.LeakDetail)
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
			if lat, lng, cc, city, _ := p.geo(ip); cc != "" {
				p.onEvent(lat, lng, cc, city, EventWithdrawal, ClassificationNone, entry.Prefix, 0, nil)
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
	if !p.stopping.CompareAndSwap(false, true) {
		return
	}
	close(p.stopCh)
	p.conns.Range(func(key, value interface{}) bool {
		if c, ok := value.(*websocket.Conn); ok {
			_ = c.Close()
		}
		return true
	})
}

func (p *BGPProcessor) isStopping() bool {
	return p.stopping.Load()
}

const dedupeWindow = 15 * time.Second
const withdrawResolutionWindow = 10 * time.Second

func (p *BGPProcessor) Listen() {
	go p.listenToAll()
}

func (p *BGPProcessor) listenToAll() {
	backoff := 5 * time.Second
	for {
		if p.isStopping() {
			return
		}

		c, err := p.connectAndSubscribeAll()
		if err != nil {
			log.Printf("[all] RIS-LIVE Connection error: %v. Retrying in %v...", err, backoff)

			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-p.stopCh:
				timer.Stop()
				return
			}
			backoff *= 2
			if backoff > 5*time.Minute {
				backoff = 5 * time.Minute
			}
			continue
		}

		// Reset backoff on successful connection
		backoff = 5 * time.Second
		p.conns.Store("all", c)

		p.runMessageLoop(c, "all")

		_ = c.Close()
		p.conns.Delete("all")

		if p.isStopping() {
			return
		}

		// Wait a bit before reconnecting if the loop exited
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
		case <-p.stopCh:
			timer.Stop()
			return
		}
	}
}

func (p *BGPProcessor) connectAndSubscribeAll() (*websocket.Conn, error) {
	url := "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream"
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second

	c, resp, err := dialer.Dial(url, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	subscribeMsg := `{"type": "ris_subscribe", "data": {"type": "UPDATE", "prefix": "0.0.0.0/0", "moreSpecific": true}}`
	if err := c.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

type PendingEvent struct {
	IP                 uint32
	Prefix             string
	ASN                uint32
	EventType          EventType
	ClassificationType ClassificationType
	LeakDetail         *LeakDetail
}

func (p *BGPProcessor) runMessageLoop(c *websocket.Conn, rrc string) {
	const readWait = 120 * time.Second
	const pingPeriod = (readWait * 9) / 10

	_ = c.SetReadDeadline(time.Now().Add(readWait))
	c.SetPongHandler(func(string) error {
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		return nil
	})
	c.SetPingHandler(func(string) error {
		_ = c.SetReadDeadline(time.Now().Add(readWait))
		// Respond with a pong
		return c.WriteControl(websocket.PongMessage, nil, time.Now().Add(10*time.Second))
	})

	// Start a ping ticker for this connection to keep it alive
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := c.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
					return
				}
			case <-done:
				return
			case <-p.stopCh:
				return
			}
		}
	}()
	defer close(done)

	for {
		if p.isStopping() {
			return
		}
		_, message, err := c.ReadMessage()
		if err != nil {
			if !p.isStopping() {
				log.Printf("[%s] Read error: %v. Reconnecting...", rrc, err)
			}
			return
		}

		var msg struct {
			Type string         `json:"type"`
			Data RISMessageData `json:"data"`
		}
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		if msg.Type == "ris_error" {
			log.Printf("[RIS ERROR %s] %s", rrc, string(message))
			continue
		}

		if msg.Type == "ris_message" {
			p.msgCount.Add(1)

			host := msg.Data.Host
			if host == "" {
				host = "unknown"
			}
			actual, _ := p.collectorCounts.LoadOrStore(host, &atomic.Uint64{})
			actual.(*atomic.Uint64).Add(1)

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

func (p *BGPProcessor) handleWithdrawals(w *processorWorker, withdrawals []string, ctx *MessageContext, now time.Time, originASN uint32) []PendingEvent {
	var events []PendingEvent
	ctx.IsWithdrawal = true
	ctx.NumPrefixes = len(withdrawals)
	for _, prefix := range withdrawals {
		ip := p.prefixToIP(prefix)
		if ip == 0 {
			continue
		}

		w.pendingWithdrawals[ip] = struct {
			Time   time.Time
			Prefix string
		}{Time: now.Add(withdrawResolutionWindow), Prefix: prefix}

		if e, ok := w.classifier.ClassifyEvent(prefix, ctx); ok {
			events = append(events, e)
		} else {
			if last, ok := w.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
				events = append(events, PendingEvent{IP: ip, Prefix: prefix, ASN: originASN, EventType: EventGossip, ClassificationType: ClassificationNone})
			} else {
				w.recentlySeen.Add(ip, struct {
					Time time.Time
					Type EventType
				}{Time: now, Type: EventWithdrawal})
				events = append(events, PendingEvent{IP: ip, Prefix: prefix, ASN: originASN, EventType: EventWithdrawal, ClassificationType: ClassificationNone})
			}
		}
	}
	return events
}

func (p *BGPProcessor) handleAnnouncements(w *processorWorker, announcements []RISAnnouncement, ctx *MessageContext, now time.Time, originASN uint32) []PendingEvent {
	var events []PendingEvent
	ctx.IsWithdrawal = false
	for _, ann := range announcements {
		ctx.NumPrefixes = len(ann.Prefixes)
		ctx.NextHop = ann.NextHop
		for _, prefix := range ann.Prefixes {
			ip := p.prefixToIP(prefix)
			if ip == 0 {
				continue
			}

			eventType := EventUpdate
			if isNew := p.isNewPrefix(prefix); isNew {
				eventType = EventNew
			}

			if _, ok := w.pendingWithdrawals[ip]; ok {
				delete(w.pendingWithdrawals, ip)
				eventType = EventUpdate
			}

			if e, ok := w.classifier.ClassifyEvent(prefix, ctx); ok {
				e.EventType = eventType
				events = append(events, e)
			} else {
				if last, ok := w.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
					w.recentlySeen.Add(ip, struct {
						Time time.Time
						Type EventType
					}{Time: now, Type: EventUpdate})
					events = append(events, PendingEvent{IP: ip, Prefix: prefix, ASN: originASN, EventType: EventUpdate, ClassificationType: ClassificationNone})
				} else if last, ok := w.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && (last.Type == EventNew || last.Type == EventUpdate || last.Type == EventGossip) {
					w.recentlySeen.Add(ip, struct {
						Time time.Time
						Type EventType
					}{Time: now, Type: EventGossip})
					events = append(events, PendingEvent{IP: ip, Prefix: prefix, ASN: originASN, EventType: EventGossip, ClassificationType: ClassificationNone})
				} else {
					w.recentlySeen.Add(ip, struct {
						Time time.Time
						Type EventType
					}{Time: now, Type: eventType})
					events = append(events, PendingEvent{IP: ip, Prefix: prefix, ASN: originASN, EventType: eventType, ClassificationType: ClassificationNone})
				}
			}
		}
	}
	return events
}

func (p *BGPProcessor) handleRISMessage(w *processorWorker, data *RISMessageData) []PendingEvent {
	var originASN uint32
	if len(data.Path) > 0 {
		last := data.Path[len(data.Path)-1]
		var asn uint32
		if err := json.Unmarshal(last, &asn); err == nil {
			originASN = asn
		}
	}

	now := p.timeProvider()
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

	var events []PendingEvent

	// 1. Process Withdrawals
	if len(data.Withdrawals) > 0 {
		events = append(events, p.handleWithdrawals(w, data.Withdrawals, ctx, now, originASN)...)
	}

	// 2. Process Announcements
	if len(data.Announcements) > 0 {
		events = append(events, p.handleAnnouncements(w, data.Announcements, ctx, now, originASN)...)
	}

	return events
}

func (p *BGPProcessor) isNewPrefix(prefix string) bool {
	if p.seenDB == nil {
		return true
	}
	val, _ := p.seenDB.Get(prefix)
	return val == nil
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
