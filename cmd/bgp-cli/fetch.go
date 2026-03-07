package main

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type FetchCmd struct {
	Fresh           bool     `help:"Re-download all source files even if they are already cached."`
	CustomLocations []string `name:"custom-locations" help:"Custom location override (format: CIDR:City,CC, e.g. 1.1.1.0/24:Sydney,AU). Can be specified multiple times."`
}

func (c *FetchCmd) Run() error {
	// Initialize GeoService
	geo := geoservice.NewGeoService(3840, 2160, 760.0)

	// Open Databases in Read-Write mode
	if err := geo.OpenHintDBs("data", false); err != nil {
		log.Fatalf("Failed to open hint databases: %v", err)
	}
	defer func() { _ = geo.Close() }()

	dm := geoservice.NewDataManager(geo)

	if c.Fresh {
		log.Println("Fresh data requested. Clearing caches...")
		// 1. Clear pre-processed cache to force re-generation
		if err := os.Remove("data/prefix-dump-cache.json"); err != nil && !os.IsNotExist(err) {
			log.Printf("Warning: failed to remove prefix cache: %v", err)
		}
		if err := os.Remove("data/hubs-dump-cache.json"); err != nil && !os.IsNotExist(err) {
			log.Printf("Warning: failed to remove hubs cache: %v", err)
		}

		// 2. Clear cached RIR/WHOIS files to force re-download
		cacheDir := filepath.Join("data", "cache")
		files, err := os.ReadDir(cacheDir)
		if err == nil {
			for _, f := range files {
				if !f.IsDir() {
					_ = os.Remove(filepath.Join(cacheDir, f.Name()))
				}
			}
		}

		// 3. Clear Hint DBs
		log.Println("Clearing hint databases...")
		_ = geo.ClearAll()
	}

	// 1. Reference Data (Load these first as others depend on them)
	log.Println("--- Reference Data ---")
	if err := dm.DownloadWorldCities(c.Fresh); err != nil {
		log.Printf("Warning: failed to download worldcities: %v", err)
	}
	dm.LoadWorldCities()

	if err := dm.LoadRemoteCityData(); err != nil {
		log.Printf("Warning: failed to load remote city data: %v", err)
	}

	asn := utils.NewASNMapping()
	if err := asn.Load(); err != nil {
		log.Printf("Warning: failed to load ASN mapping: %v", err)
	}

	// 2. Parallel Processing for main datasets
	var wg sync.WaitGroup

	// Task 2: RIR Data
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("--- RIR / Prefix Data ---")
		if err := dm.ProcessRIRData(); err != nil {
			log.Printf("Error processing RIR data: %v", err)
		}
	}()

	// Task 3: Cloud Data
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("--- Cloud Data ---")
		dm.LoadCloudData()
	}()

	// Task 4: PeeringDB
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("--- PeeringDB ---")
		dm.ProcessPeeringDBData()
	}()

	// Task 5: WHOIS
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("--- WHOIS ---")
		dm.ProcessBulkWhoisData()
	}()

	// Task 7: Custom Hints
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("--- Custom Hints ---")
		dm.ProcessCustomHints(c.CustomLocations)
	}()

	wg.Wait()

	log.Println("Data management tasks complete.")
	return nil
}
