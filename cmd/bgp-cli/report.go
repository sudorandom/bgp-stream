package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/bgp"
	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgp/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/utils"
	"google.golang.org/protobuf/proto"
)

type ReportCmd struct {
	States []string `default:"bgp_hijack,route_leak" sep:"," enum:"flap,path_hunting,traffic_eng,outage,route_leak,discovery,ddos_mitigation,bgp_hijack,bogon_martian" help:"List of states to filter by."`
	DB     string   `default:"./data/prefix-state.db" help:"Path to the prefix state database."`
}

func (c *ReportCmd) Run() error {
	stateMap := map[string]string{
		"flap":            bgp.NameFlap,
		"path_hunting":    bgp.NamePathHunting,
		"traffic_eng":     bgp.NameTrafficEng,
		"outage":          bgp.NameHardOutage,
		"route_leak":      bgp.NameRouteLeak,
		"discovery":       bgp.NameDiscovery,
		"ddos_mitigation": bgp.NameDDoSMitigation,
		"bgp_hijack":      bgp.NameHijack,
		"bogon_martian":   bgp.NameBogon,
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
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Warning: error closing database: %v", err)
		}
	}()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	if _, err := fmt.Fprintln(w, "PREFIX\tSTATE\tLAST ASN\tVICTIM ASN\tLEAKER ASN\tLAST UPDATE\tACTIVE DURATION\tSTALE"); err != nil {
		return err
	}

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

		if state.ClassifiedType == 0 {
			return nil
		}

		className := bgp.ClassificationType(state.ClassifiedType).String()
		if !targetStates[strings.ToLower(className)] {
			return nil
		}

		if err := c.printReportLine(w, k, state, className, now); err != nil {
			return err
		}
		count++
		return nil
	})

	if err != nil {
		return fmt.Errorf("error iterating through database: %v", err)
	}

	if err := w.Flush(); err != nil {
		return err
	}

	fmt.Printf("\nTotal matched prefixes: %d\n", count)
	return nil
}

func (c *ReportCmd) printReportLine(w io.Writer, k []byte, state *bgpproto.PrefixState, className string, now int64) error {
	ip := net.IP(k[:4])
	mask := int(k[4])
	prefix := fmt.Sprintf("%s/%d", ip.String(), mask)

	lastUpdate := time.Unix(state.LastUpdateTs, 0)
	duration := time.Duration(now-state.ClassifiedTimeTs) * time.Second

	isStale := "No"
	if now-state.LastUpdateTs > 86400 {
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

	_, err := fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
		prefix,
		className,
		state.LastOriginAsn,
		victimASN,
		leakerASN,
		lastUpdate.Format(time.RFC3339),
		duration.String(),
		isStale,
	)
	return err
}
