package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/bgpengine"
	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgpengine/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/utils"
	"google.golang.org/protobuf/proto"
)

type ReportCmd struct {
	States []string `default:"bgp_hijack,route_leak" sep:"," enum:"flap,path_hunting,traffic_eng,outage,route_leak,discovery,ddos_mitigation,bgp_hijack,bogon_martian" help:"List of states to filter by."`
	DB     string   `default:"./data/prefix-state.db" help:"Path to the prefix state database."`
}

func (c *ReportCmd) Run() error {
	stateMap := map[string]string{
		"flap":            "Flap",
		"path_hunting":    "Path Hunting",
		"traffic_eng":     "Traffic Eng.",
		"outage":          "Outage",
		"route_leak":      "Route Leak",
		"discovery":       "Discovery",
		"ddos_mitigation": "DDoS Mitigation",
		"bgp_hijack":      "BGP Hijack",
		"bogon_martian":   "Bogon/Martian",
	}

	targetStates := make(map[string]bool)
	for _, s := range c.States {
		if internalName, ok := stateMap[s]; ok {
			targetStates[strings.ToLower(internalName)] = true
		}
	}

	if len(targetStates) == 0 {
		return fmt.Errorf("no valid states provided")
	}

	log.Printf("Opening database at %s...", c.DB)
	db, err := utils.OpenDiskTrieReadOnly(c.DB)
	if err != nil {
		return fmt.Errorf("failed to open prefix state database: %v", err)
	}
	defer db.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "PREFIX	STATE	LAST ASN	VICTIM ASN	LEAKER ASN	LAST UPDATE	ACTIVE DURATION	STALE")

	count := 0
	now := time.Now().Unix()

	err = db.ForEach(func(k []byte, v []byte) error {
		if len(k) != 5 {
			return nil
		}
		state := &bgpproto.PrefixState{}
		if err := proto.Unmarshal(v, state); err != nil {
			return nil
		}

		if state.ClassifiedType != 0 {
			className := bgpengine.ClassificationType(state.ClassifiedType).String()

			if targetStates[strings.ToLower(className)] {
				ip := net.IP(k[:4])
				mask := int(k[4])
				prefix := fmt.Sprintf("%s/%d", ip.String(), mask)

				lastUpdate := time.Unix(state.LastUpdateTs, 0)
				duration := time.Duration(now - state.ClassifiedTimeTs) * time.Second
				
				isStale := "No"
				if now - state.LastUpdateTs > 86400 {
					isStale = "Yes (>24h)"
				}

				victimASN := "-"
				if state.VictimAsn != 0 {
					victimASN = fmt.Sprintf("%d", state.VictimAsn)
				}
				
				leakerASN := "-"
				if state.LeakerAsn != 0 {
					leakerASN = fmt.Sprintf("%d", state.LeakerAsn)
				}

				fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
					prefix,
					className,
					state.LastOriginAsn,
					victimASN,
					leakerASN,
					lastUpdate.Format(time.RFC3339),
					duration.String(),
					isStale,
				)
				count++
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error iterating through database: %v", err)
	}

	w.Flush()
	fmt.Printf("\nTotal matched prefixes: %d\n", count)
	return nil
}
