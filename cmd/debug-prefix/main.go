package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

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

type Stats struct {
	mu            sync.Mutex
	Announcements int
	Withdrawals   int
	TotalMessages int
	Peers         map[string]int
	PeerChurn     map[string]*ChurnStats
	PeerLastAttrs map[string]LastAttrs
	StartTime     time.Time
}

func (s *Stats) Record(msg []byte, showJSON bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var res struct {
		Type string `json:"type"`
		Data struct {
			Announcements []struct {
				NextHop  string   `json:"next_hop"`
				Prefixes []string `json:"prefixes"`
			} `json:"announcements"`
			Withdrawals []string        `json:"withdrawals"`
			Peer        string          `json:"peer"`
			Path        []interface{}   `json:"path"`
			Community   [][]interface{} `json:"community"`
			Aggregator  string          `json:"aggregator"`
		} `json:"data"`
	}

	if err := json.Unmarshal(msg, &res); err != nil {
		return
	}

	if res.Type != "ris_message" {
		return
	}

	s.TotalMessages++
	peer := res.Data.Peer
	if peer != "" {
		s.Peers[peer]++
	}

	if len(res.Data.Withdrawals) > 0 {
		s.Withdrawals += len(res.Data.Withdrawals)
		return
	}

	if len(res.Data.Announcements) > 0 {
		s.Announcements += len(res.Data.Announcements)

		pathStr := fmt.Sprintf("%v", res.Data.Path)
		commStr := fmt.Sprintf("%v", res.Data.Community)
		nextHop := res.Data.Announcements[0].NextHop
		agg := res.Data.Aggregator
		pathLen := len(res.Data.Path)

		if last, ok := s.PeerLastAttrs[peer]; ok {
			churn, ok := s.PeerChurn[peer]
			if !ok {
				churn = &ChurnStats{}
				s.PeerChurn[peer] = churn
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

		s.PeerLastAttrs[peer] = LastAttrs{
			Path:        pathStr,
			Communities: commStr,
			NextHop:     nextHop,
			Aggregator:  agg,
		}
	}

	if showJSON {
		var prettyJSON bytes.Buffer
		_ = json.Indent(&prettyJSON, msg, "", "  ")
		fmt.Printf("%s\n\n", prettyJSON.String())
	}
}

func (s *Stats) Report() {
	s.mu.Lock()
	defer s.mu.Unlock()

	elapsed := time.Since(s.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	fmt.Printf("\033[H\033[2J") // Clear screen
	fmt.Printf("BGP Prefix Monitor Stats (Running for %.1fs)\n", elapsed)
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("Announcements: %d (%.2f/s)\n", s.Announcements, float64(s.Announcements)/elapsed)
	fmt.Printf("Withdrawals:   %d (%.2f/s)\n", s.Withdrawals, float64(s.Withdrawals)/elapsed)
	fmt.Printf("Total Msgs:    %d (%.2f/s)\n", s.TotalMessages, float64(s.TotalMessages)/elapsed)
	fmt.Printf("Unique Peers:  %d\n", len(s.Peers))
	fmt.Printf("--------------------------------------------------\n")

	// Aggregate Churn
	totalPath, totalComm, totalHop, totalAgg, totalLen := 0, 0, 0, 0, 0
	for _, c := range s.PeerChurn {
		totalPath += c.PathChanges
		totalComm += c.CommunityChanges
		totalHop += c.NextHopChanges
		totalAgg += c.AggregatorChanges
		totalLen += c.PathLengthChanges
	}

	fmt.Printf("GLOBAL CHURN EVENTS:\n")
	fmt.Printf("  AS-Path Changes:  %d\n", totalPath)
	fmt.Printf("  Community Changes: %d\n", totalComm)
	fmt.Printf("  Next-Hop Changes:  %d\n", totalHop)
	fmt.Printf("  Aggregator Flaps:  %d\n", totalAgg)
	fmt.Printf("  Path Length Flaps: %d\n", totalLen)
	fmt.Printf("--------------------------------------------------\n")

	fmt.Printf("LIKELY CONCLUSIONS:\n")
	conclusions := s.analyze()
	if len(conclusions) == 0 {
		fmt.Printf("  - Routing appears stable (Normal Link)\n")
	} else {
		for _, c := range conclusions {
			fmt.Printf("  - %s\n", c)
		}
	}
	fmt.Printf("--------------------------------------------------\n")

	// Top Peers
	type peerChurn struct {
		IP    string
		Churn int
	}
	var churnList []peerChurn
	for ip, stats := range s.PeerChurn {
		total := stats.PathChanges + stats.CommunityChanges + stats.NextHopChanges + stats.AggregatorChanges
		churnList = append(churnList, peerChurn{ip, total})
	}
	sort.Slice(churnList, func(i, j int) bool {
		return churnList[i].Churn > churnList[j].Churn
	})

	maxPeers := 5
	if len(churnList) < maxPeers {
		maxPeers = len(churnList)
	}
	if maxPeers > 0 {
		fmt.Printf("Top %d Churning Peers:\n", maxPeers)
		for i := 0; i < maxPeers; i++ {
			p := churnList[i]
			fmt.Printf("  %s: %d attribute changes\n", p.IP, p.Churn)
		}
	}
}

func (s *Stats) analyze() []string {
	var results []string
	elapsed := time.Since(s.StartTime).Seconds()
	msgRate := float64(s.TotalMessages) / elapsed

	totalPath, totalComm, totalHop, totalAgg, totalLen := 0, 0, 0, 0, 0
	for _, c := range s.PeerChurn {
		totalPath += c.PathChanges
		totalComm += c.CommunityChanges
		totalHop += c.NextHopChanges
		totalAgg += c.AggregatorChanges
		totalLen += c.PathLengthChanges
	}

	// 1. Check for Anycast
	// If many peers see different NextHops but the path length is stable and rate is low
	uniqueHops := make(map[string]bool)
	for _, attr := range s.PeerLastAttrs {
		if attr.NextHop != "" {
			uniqueHops[attr.NextHop] = true
		}
	}
	if len(uniqueHops) > 5 && msgRate < 1.0 {
		results = append(results, "Signs of Anycast (Multiple entry points detected)")
	}

	// 2. Aggregator Flapping
	if totalAgg > 10 && float64(totalAgg)/elapsed > 0.05 {
		results = append(results, "Aggregator Flapping (Origin router is re-summarizing frequently)")
	}

	// 3. Path Length Oscillation
	if totalLen > 10 && float64(totalLen)/elapsed > 0.05 {
		results = append(results, "Path Length Oscillation (Route is toggling between different path lengths)")
	}

	// 4. Link Flap (High Withdrawal Ratio)
	if s.Withdrawals > 5 && float64(s.Announcements)/float64(s.Withdrawals) < 2.5 {
		results = append(results, "Link Flap (High ratio of withdrawals suggesting physical/session instability)")
	}

	// 5. Path Hunting
	if s.Announcements > 50 && s.Withdrawals < (s.Announcements/10) && totalPath > s.Announcements/2 {
		results = append(results, "Path Hunting (Router is exploring alternative paths after a failure)")
	}

	// 6. BGP Babbling
	if msgRate > 2.0 {
		results = append(results, "BGP Babbling (Excessive update rate detected)")
	}

	return results
}

func main() {
	prefix := flag.String("prefix", "146.66.28.0/22", "BGP prefix to watch")
	timeout := flag.Duration("timeout", 0, "How long to run before exiting (0 for infinite)")
	showJSON := flag.Bool("json", false, "Dump raw JSON instead of showing stats")
	flag.Parse()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	if *timeout > 0 {
		go func() {
			time.Sleep(*timeout)
			log.Printf("Timeout of %v reached, exiting...", *timeout)
			interrupt <- os.Interrupt
		}()
	}

	u := "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream-debug"
	log.Printf("Connecting to %s", u)

	c, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		log.Printf("dial: %v", err)
		return
	}
	defer func() {
		_ = c.Close()
	}()

	stats := &Stats{
		Peers:         make(map[string]int),
		PeerChurn:     make(map[string]*ChurnStats),
		PeerLastAttrs: make(map[string]LastAttrs),
		StartTime:     time.Now(),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				return
			}
			stats.Record(message, *showJSON)
		}
	}()

	// Subscribe to prefix
	subscribeMsg := map[string]interface{}{
		"type": "ris_subscribe",
		"data": map[string]interface{}{
			"prefix":       *prefix,
			"moreSpecific": true,
			"lessSpecific": true,
		},
	}
	subBytes, _ := json.Marshal(subscribeMsg)
	log.Printf("Subscribing to: %s", *prefix)
	err = c.WriteMessage(websocket.TextMessage, subBytes)
	if err != nil {
		log.Printf("subscribe error: %v", err)
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if !*showJSON {
				stats.Report()
			}
		case <-interrupt:
			log.Println("Exiting...")
			if !*showJSON {
				stats.Report()
			}
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}
