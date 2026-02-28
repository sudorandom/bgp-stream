package bgpengine

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type BGPEventCallback func(lat, lng float64, cc string, eventType EventType, prefix string, asn uint32)
type IPCoordsProvider func(ip uint32) (float64, float64, string)
type PrefixToIPConverter func(p string) uint32

type Level2EventType int

const (
	Level2None Level2EventType = iota
	Level2LinkFlap
	Level2AggFlap
	Level2Anycast
	Level2PathLengthOscillation
	Level2Babbling
	Level2PathHunting
)

func (t Level2EventType) String() string {
	switch t {
	case Level2LinkFlap:
		return "Link Flap"
	case Level2AggFlap:
		return "Aggregator Flapping"
	case Level2Anycast:
		return "Anycast"
	case Level2PathLengthOscillation:
		return "Path Length Oscillation"
	case Level2Babbling:
		return "BGP Babbling"
	case Level2PathHunting:
		return "Path Hunting"
	default:
		return "None"
	}
}

type LastAttrs struct {
	Path        string
	Communities string
	NextHop     string
	Aggregator  string
}

type ChurnStats struct {
	PathChanges       int
	CommunityChanges  int
	NextHopChanges    int
	AggregatorChanges int
	PathLengthChanges int
	LastPathLen       int
}

type PrefixState struct {
	Announcements int
	Withdrawals   int
	TotalMessages int
	PeerChurn     map[string]*ChurnStats
	PeerLastAttrs map[string]LastAttrs
	StartTime     time.Time
}

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
}

type BGPProcessor struct {
	geo          IPCoordsProvider
	seenDB       *utils.DiskTrie
	onEvent      BGPEventCallback
	prefixToIP   PrefixToIPConverter
	recentlySeen map[uint32]struct {
		Time time.Time
		Type EventType
	}

	level2Stats       map[Level2EventType]int
	totalLevel2Events int
	prefixStates      map[string]*PrefixState

	mu  sync.Mutex
	url string
}

func NewBGPProcessor(geo IPCoordsProvider, seenDB *utils.DiskTrie, prefixToIP PrefixToIPConverter, onEvent BGPEventCallback) *BGPProcessor {
	return &BGPProcessor{
		geo:        geo,
		seenDB:     seenDB,
		onEvent:    onEvent,
		prefixToIP: prefixToIP,
		recentlySeen: make(map[uint32]struct {
			Time time.Time
			Type EventType
		}),
		level2Stats:  make(map[Level2EventType]int),
		prefixStates: make(map[string]*PrefixState),
		url:          "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream",
	}
}

const dedupeWindow = 15 * time.Second
const withdrawResolutionWindow = 10 * time.Second

func (p *BGPProcessor) Listen() {
	pendingWithdrawals := make(map[uint32]struct {
		Time   time.Time
		Prefix string
	})

	p.startWithdrawalPacer(pendingWithdrawals)

	backoff := 1 * time.Second
	for {
		log.Printf("Connecting to RIS Live: %s", p.url)
		c, resp, err := websocket.DefaultDialer.Dial(p.url, nil)
		if err != nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			log.Printf("Dial error: %v. Retrying in %v...", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			continue
		}
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		backoff = 1 * time.Second

		subscribeMsg := `{"type": "ris_subscribe", "data": {"type": "UPDATE"}}`
		if err := c.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
			log.Printf("Subscribe error: %v", err)
			_ = c.Close()
			continue
		}

		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v. Reconnecting...", err)
				break
			}
			var msg struct {
				Type string         `json:"type"`
				Data RISMessageData `json:"data"`
			}
			if json.Unmarshal(message, &msg) != nil {
				continue
			}

			if msg.Type == "ris_error" {
				log.Printf("[RIS ERROR] %s", string(message))
				continue
			}
			if msg.Type == "ris_message" {
				p.handleRISMessage(&msg.Data, pendingWithdrawals)
			}
		}
		_ = c.Close()
		time.Sleep(time.Second)
	}
}

func (p *BGPProcessor) startWithdrawalPacer(pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		ticks := 0
		for range ticker.C {
			now := time.Now()
			p.mu.Lock()

			for ip, entry := range pendingWithdrawals {
				if now.After(entry.Time) {
					if lat, lng, cc := p.geo(ip); cc != "" {
						p.onEvent(lat, lng, cc, EventWithdrawal, entry.Prefix, 0)
						p.recentlySeen[ip] = struct {
							Time time.Time
							Type EventType
						}{Time: now, Type: EventWithdrawal}
					}
					delete(pendingWithdrawals, ip)
				}
			}

			ticks++
			if ticks >= 30 {
				ticks = 0
				p.cleanupRecentlySeen(now)
			}
			p.mu.Unlock()
		}
	}()
}

func (p *BGPProcessor) cleanupRecentlySeen(now time.Time) {
	if len(p.recentlySeen) > 500000 {
		for ip, entry := range p.recentlySeen {
			if now.Sub(entry.Time) > 5*time.Minute {
				delete(p.recentlySeen, ip)
			}
		}
	}
	if len(p.prefixStates) > 50000 {
		for prefix, state := range p.prefixStates {
			if now.Sub(state.StartTime) > 5*time.Minute {
				delete(p.prefixStates, prefix)
			}
		}
	}
}

func (p *BGPProcessor) handleRISMessage(data *RISMessageData, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var originASN uint32
	if len(data.Path) > 0 {
		last := data.Path[len(data.Path)-1]
		var asn uint32
		if err := json.Unmarshal(last, &asn); err == nil {
			originASN = asn
		}
	}

	now := time.Now()
	p.handleWithdrawals(data.Withdrawals, originASN, now, pendingWithdrawals)
	p.handleAnnouncements(data.Announcements, originASN, now, pendingWithdrawals)

	for _, prefix := range data.Withdrawals {
		p.classifyEvent(prefix, data)
	}
	for _, ann := range data.Announcements {
		for _, prefix := range ann.Prefixes {
			p.classifyEvent(prefix, data)
		}
	}
}

func (p *BGPProcessor) handleWithdrawals(withdrawals []string, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	for _, prefix := range withdrawals {
		ip := p.prefixToIP(prefix)
		if ip == 0 {
			continue
		}

		if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
			if lat, lng, cc := p.geo(ip); cc != "" {
				p.onEvent(lat, lng, cc, EventGossip, prefix, originASN)
			}
			continue
		}

		pendingWithdrawals[ip] = struct {
			Time   time.Time
			Prefix string
		}{Time: now.Add(withdrawResolutionWindow), Prefix: prefix}
	}
}

func (p *BGPProcessor) handleAnnouncements(announcements []struct {
	NextHop  string   `json:"next_hop"`
	Prefixes []string `json:"prefixes"`
}, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	for _, ann := range announcements {
		for _, prefix := range ann.Prefixes {
			p.processAnnouncement(prefix, originASN, now, pendingWithdrawals)
		}
	}
}

func (p *BGPProcessor) processAnnouncement(prefix string, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	ip := p.prefixToIP(prefix)
	if ip == 0 {
		return
	}

	if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventUpdate, prefix, originASN)
			p.recentlySeen[ip] = struct {
				Time time.Time
				Type EventType
			}{Time: now, Type: EventUpdate}
		}
		return
	}

	if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && (last.Type == EventNew || last.Type == EventUpdate || last.Type == EventGossip) {
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventGossip, prefix, originASN)
		}
		return
	}

	if _, ok := pendingWithdrawals[ip]; ok {
		delete(pendingWithdrawals, ip)
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventUpdate, prefix, originASN)
			p.recentlySeen[ip] = struct {
				Time time.Time
				Type EventType
			}{Time: now, Type: EventUpdate}
		}
	} else {
		p.handleNewOrUpdate(prefix, ip, originASN, now)
	}
}

func (p *BGPProcessor) handleNewOrUpdate(prefix string, ip, originASN uint32, now time.Time) {
	isNew := true
	if p.seenDB != nil {
		if val, _ := p.seenDB.Get(prefix); val != nil {
			isNew = false
		}
	}

	if isNew {
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventNew, prefix, originASN)
			p.recentlySeen[ip] = struct {
				Time time.Time
				Type EventType
			}{Time: now, Type: EventNew}
		}
	} else {
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventUpdate, prefix, originASN)
			p.recentlySeen[ip] = struct {
				Time time.Time
				Type EventType
			}{Time: now, Type: EventUpdate}
		}
	}
}

func (p *BGPProcessor) classifyEvent(prefix string, data *RISMessageData) {
	state, ok := p.prefixStates[prefix]
	if !ok {
		state = &PrefixState{
			PeerChurn:     make(map[string]*ChurnStats),
			PeerLastAttrs: make(map[string]LastAttrs),
			StartTime:     time.Now(),
		}
		p.prefixStates[prefix] = state
	}

	state.TotalMessages++
	peer := data.Peer

	if len(data.Withdrawals) > 0 {
		state.Withdrawals += len(data.Withdrawals)
	}

	if len(data.Announcements) > 0 {
		state.Announcements += len(data.Announcements)

		pathStr := fmt.Sprintf("%v", data.Path)
		commStr := fmt.Sprintf("%v", data.Community)
		nextHop := data.Announcements[0].NextHop
		agg := data.Aggregator
		pathLen := len(data.Path)

		if last, ok := state.PeerLastAttrs[peer]; ok {
			churn, ok := state.PeerChurn[peer]
			if !ok {
				churn = &ChurnStats{}
				state.PeerChurn[peer] = churn
			}

			if pathStr != last.Path {
				churn.PathChanges++
			}
			if commStr != last.Communities {
				churn.CommunityChanges++
			}
			if nextHop != last.NextHop {
				churn.NextHopChanges++
			}
			if agg != last.Aggregator {
				churn.AggregatorChanges++
			}
			if pathLen != churn.LastPathLen && churn.LastPathLen != 0 {
				churn.PathLengthChanges++
			}
			churn.LastPathLen = pathLen
		}

		state.PeerLastAttrs[peer] = LastAttrs{
			Path:        pathStr,
			Communities: commStr,
			NextHop:     nextHop,
			Aggregator:  agg,
		}
	}

	elapsed := time.Since(state.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	// Ensure we have seen enough messages over a small time window to classify
	if elapsed < 2.0 && state.TotalMessages < 5 {
		return
	}

	msgRate := float64(state.TotalMessages) / elapsed

	totalPath, totalComm, totalHop, totalAgg, totalLen := 0, 0, 0, 0, 0
	for _, c := range state.PeerChurn {
		totalPath += c.PathChanges
		totalComm += c.CommunityChanges
		totalHop += c.NextHopChanges
		totalAgg += c.AggregatorChanges
		totalLen += c.PathLengthChanges
	}

	classified := false

	uniqueHops := make(map[string]bool)
	for _, attr := range state.PeerLastAttrs {
		if attr.NextHop != "" {
			uniqueHops[attr.NextHop] = true
		}
	}
	if len(uniqueHops) > 5 && msgRate < 1.0 {
		p.level2Stats[Level2Anycast]++
		classified = true
	} else if totalAgg > 10 && float64(totalAgg)/elapsed > 0.05 {
		p.level2Stats[Level2AggFlap]++
		classified = true
	} else if totalLen > 10 && float64(totalLen)/elapsed > 0.05 {
		p.level2Stats[Level2PathLengthOscillation]++
		classified = true
	} else if state.Withdrawals > 5 && float64(state.Announcements)/float64(state.Withdrawals) < 2.5 {
		p.level2Stats[Level2LinkFlap]++
		classified = true
	} else if state.Announcements > 50 && state.Withdrawals < (state.Announcements/10) && totalPath > state.Announcements/2 {
		p.level2Stats[Level2PathHunting]++
		classified = true
	} else if msgRate > 2.0 && state.TotalMessages > 10 {
		p.level2Stats[Level2Babbling]++
		classified = true
	}

	if classified {
		p.totalLevel2Events++
		// Reset state so we don't count it again immediately
		delete(p.prefixStates, prefix)
	}
}

func (p *BGPProcessor) GetLevel2Stats() (map[Level2EventType]int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	statsCopy := make(map[Level2EventType]int)
	for k, v := range p.level2Stats {
		statsCopy[k] = v
	}

	return statsCopy, p.totalLevel2Events
}
