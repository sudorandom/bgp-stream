package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type DebugGeoCmd struct {
	IP   string   `help:"IP address to resolve"`
	MMDB []string `name:"mmdb" help:"Path to an additional .mmdb file (can be specified multiple times)"`
}

func (c *DebugGeoCmd) Run() error {
	// Initialize GeoService
	geo := geoservice.NewGeoService(3840, 2160, 760.0)

	// Open Databases in Read-Only mode
	if err := geo.OpenHintDBs("data", true); err != nil {
		log.Printf("Warning: Failed to open hint databases: %v", err)
	}
	defer func() { _ = geo.Close() }()

	// Load city data
	dm := geoservice.NewDataManager(geo)
	dm.LoadWorldCities()
	if err := dm.LoadRemoteCityData(); err != nil {
		log.Printf("Warning: failed to load remote city data: %v", err)
	}

	for _, path := range c.MMDB {
		if err := geo.AddMMDBReader(path); err != nil {
			log.Printf("Warning: failed to load MMDB database %s: %v", path, err)
		}
	}

	// Load prefix data
	cachePath := "data/prefix-dump-cache.json"
	if data, err := os.ReadFile(cachePath); err == nil {
		var prefixData geoservice.PrefixData
		if err := json.Unmarshal(data, &prefixData); err == nil {
			geo.SetPrefixData(prefixData)
		}
	}
	hubsCachePath := "data/hubs-dump-cache.json"
	if data, err := os.ReadFile(hubsCachePath); err == nil {
		var hubsData geoservice.PrefixData
		if err := json.Unmarshal(data, &hubsData); err == nil {
			geo.SetHubsData(hubsData)
		}
	}

	resolve := func(s string) {
		parsedIP := net.ParseIP(s).To4()
		if parsedIP == nil {
			fmt.Printf("Invalid IPv4: %s\n", s)
			return
		}
		ipUint := utils.IPToUint32(parsedIP)
		lat, lng, cc, city, resType := geo.GetIPCoords(ipUint)

		fmt.Printf("IP: %s\n", s)
		fmt.Printf("  Coords:     %f, %f\n", lat, lng)
		fmt.Printf("  Country:    %s\n", cc)
		fmt.Printf("  City:       %s\n", city)
		fmt.Printf("  Resolution: %s\n", resType)
		fmt.Println("--------------------------------")
	}

	if c.IP != "" {
		resolve(c.IP)
		return nil
	}

	fmt.Println("Enter IPs to resolve (one per line, Ctrl+C to exit):")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			resolve(line)
		}
	}
	return nil
}
