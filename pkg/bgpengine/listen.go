package bgpengine

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func (e *Engine) ListenToBGP() {
	url := "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream"
	pendingWithdrawals := make(map[uint32]struct {
		Time   time.Time
		Prefix string
	})
	var mu sync.Mutex

	// De-duplication window: ignore redundant updates for the same prefix within 15 seconds
	const dedupeWindow = 15 * time.Second
	// Withdrawal resolution window: wait this long to see if an announcement follows a withdrawal
	const withdrawResolutionWindow = 10 * time.Second

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		ticks := 0
		for range ticker.C {
			now := time.Now()
			mu.Lock()

			// 1. Process pending withdrawals
			for ip, entry := range pendingWithdrawals {
				if now.After(entry.Time) {
					if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
						e.recordEvent(lat, lng, cc, "with", entry.Prefix)
						e.recentlySeen[ip] = struct {
							Time time.Time
							Type string
						}{Time: now, Type: "with"}
					}
					delete(pendingWithdrawals, ip)
				}
			}

			// 2. Periodically clean up de-duplication cache (every 30s)
			ticks++
			if ticks >= 30 {
				ticks = 0
				if len(e.recentlySeen) > 500000 {
					for ip, entry := range e.recentlySeen {
						if now.Sub(entry.Time) > 5*time.Minute {
							delete(e.recentlySeen, ip)
						}
					}
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

			now := time.Now()
			switch msg.Type {
			case "ris_error":
				log.Printf("[RIS ERROR] %s", string(message))
			case "ris_message":
				mu.Lock()

				// 1. Process Withdrawals
				for _, prefix := range msg.Data.Withdrawals {
					ip := e.prefixToIP(prefix)
					if ip == 0 {
						continue
					}

					// If we've seen a WITHDRAWAL for this prefix very recently, it's gossip
					if last, ok := e.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == "with" {
						if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
							e.recordEvent(lat, lng, cc, "gossip", prefix)
						}
						continue
					}

					pendingWithdrawals[ip] = struct {
						Time   time.Time
						Prefix string
					}{Time: now.Add(withdrawResolutionWindow), Prefix: prefix}
				}

				// 2. Process Announcements
				for _, ann := range msg.Data.Announcements {
					for _, prefix := range ann.Prefixes {
						ip := e.prefixToIP(prefix)
						if ip == 0 {
							continue
						}

						// Retroactive Path Change Detection:
						if last, ok := e.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == "with" {
							if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
								e.recordEvent(lat, lng, cc, "upd", prefix)
								e.recentlySeen[ip] = struct {
									Time time.Time
									Type string
								}{Time: now, Type: "upd"}
							}
							continue
						}

						// Normal Gossip Detection
						if last, ok := e.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && (last.Type == "new" || last.Type == "upd" || last.Type == "gossip") {
							if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
								e.recordEvent(lat, lng, cc, "gossip", prefix)
							}
							continue
						}

						if _, ok := pendingWithdrawals[ip]; ok {
							// Found a matching announcement for a pending withdrawal: this is a Path Change
							delete(pendingWithdrawals, ip)
							if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
								e.recordEvent(lat, lng, cc, "upd", prefix)
								e.recentlySeen[ip] = struct {
									Time time.Time
									Type string
								}{Time: now, Type: "upd"}
							}
						} else {
							// Check if we've EVER seen this prefix before (across sessions)
							isNew := true
							if e.SeenDB != nil {
								// We use the full CIDR string as the key for exact match
								if val, _ := e.SeenDB.Get(prefix); val != nil {
									isNew = false
								}
							}

							if isNew {
								// Truly new announcement (Discovery)
								if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
									e.recordEvent(lat, lng, cc, "new", prefix)
									e.recentlySeen[ip] = struct {
										Time time.Time
										Type string
									}{Time: now, Type: "new"}

									if e.SeenDB != nil {
										if err := e.SeenDB.BatchInsertRaw(map[string][]byte{prefix: []byte{1}}); err != nil {
											log.Printf("Warning: Failed to update seen database: %v", err)
										}
									}
								}
							} else {
								// We've seen this before, so this is just a path change (Re-discovery)
								if lat, lng, cc := e.getPrefixCoords(ip); cc != "" {
									e.recordEvent(lat, lng, cc, "upd", prefix)
									e.recentlySeen[ip] = struct {
										Time time.Time
										Type string
									}{Time: now, Type: "upd"}
								}
							}
						}
					}
				}
				mu.Unlock()
			}
		}
		c.Close()
		time.Sleep(time.Second)
	}
}
