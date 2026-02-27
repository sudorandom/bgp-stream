package bgpengine

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type BGPEventCallback func(lat, lng float64, cc string, eventType EventType, prefix string, asn uint32)
type IPCoordsProvider func(ip uint32) (float64, float64, string)
type PrefixToIPConverter func(p string) uint32

type BGPProcessor struct {
	geo          IPCoordsProvider
	seenDB       *utils.DiskTrie
	onEvent      BGPEventCallback
	prefixToIP   PrefixToIPConverter
	recentlySeen map[uint32]struct {
		Time time.Time
		Type EventType
	}
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
		url: "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream",
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
				Type string `json:"type"`
				Data struct {
					Announcements []struct {
						Prefixes []string `json:"prefixes"`
					} `json:"announcements"`
					Withdrawals []string          `json:"withdrawals"`
					Path        []json.RawMessage `json:"path"`
					Peer        string            `json:"peer"`
				} `json:"data"`
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
}

func (p *BGPProcessor) handleRISMessage(data *struct {
	Announcements []struct {
		Prefixes []string `json:"prefixes"`
	} `json:"announcements"`
	Withdrawals []string          `json:"withdrawals"`
	Path        []json.RawMessage `json:"path"`
	Peer        string            `json:"peer"`
}, pendingWithdrawals map[uint32]struct {
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
