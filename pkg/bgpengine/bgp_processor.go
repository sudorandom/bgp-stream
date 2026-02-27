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
	geo             IPCoordsProvider
	seenDB          *utils.DiskTrie
	onEvent         BGPEventCallback
	prefixToIP      PrefixToIPConverter
	recentlySeen    map[uint32]struct {
		Time time.Time
		Type EventType
	}
	mu              sync.Mutex
	url             string
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

func (p *BGPProcessor) Listen() {
	pendingWithdrawals := make(map[uint32]struct {
		Time   time.Time
		Prefix string
	})

	const dedupeWindow = 15 * time.Second
	const withdrawResolutionWindow = 10 * time.Second

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
				if len(p.recentlySeen) > 500000 {
					for ip, entry := range p.recentlySeen {
						if now.Sub(entry.Time) > 5*time.Minute {
							delete(p.recentlySeen, ip)
						}
					}
				}
			}
			p.mu.Unlock()
		}
	}()

	backoff := 1 * time.Second
	for {
		log.Printf("Connecting to RIS Live: %s", p.url)
		c, _, err := websocket.DefaultDialer.Dial(p.url, nil)
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
					Path        []json.RawMessage `json:"path"`
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
				p.mu.Lock()

				var originASN uint32
				if len(msg.Data.Path) > 0 {
					// Path can contain integers or AS_SET arrays. We only care about simple integer ASNs for now.
					last := msg.Data.Path[len(msg.Data.Path)-1]
					var asn uint32
					if err := json.Unmarshal(last, &asn); err == nil {
						originASN = asn
					}
				}

				for _, prefix := range msg.Data.Withdrawals {
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

				for _, ann := range msg.Data.Announcements {
					for _, prefix := range ann.Prefixes {
						ip := p.prefixToIP(prefix)
						if ip == 0 {
							continue
						}

						if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
							if lat, lng, cc := p.geo(ip); cc != "" {
								p.onEvent(lat, lng, cc, EventUpdate, prefix, originASN)
								p.recentlySeen[ip] = struct {
									Time time.Time
									Type EventType
								}{Time: now, Type: EventUpdate}
							}
							continue
						}

						if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && (last.Type == EventNew || last.Type == EventUpdate || last.Type == EventGossip) {
							if lat, lng, cc := p.geo(ip); cc != "" {
								p.onEvent(lat, lng, cc, EventGossip, prefix, originASN)
							}
							continue
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
					}
				}
				p.mu.Unlock()
			}
		}
		c.Close()
		time.Sleep(time.Second)
	}
}
