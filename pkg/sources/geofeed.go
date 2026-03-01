// Package sources provides various utility functions and data structures for BGP stream processing.
package sources

import (
	"bufio"
	"encoding/csv"
	"io"
	"net"
	"strings"
)

type GeofeedEntry struct {
	Prefix  *net.IPNet
	Country string
	Region  string
	City    string
}

func ParseGeofeed(r io.Reader) ([]GeofeedEntry, error) {
	var entries []GeofeedEntry
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Geofeed is CSV: prefix,country_code,region_code,city_name,postal_code
		reader := csv.NewReader(strings.NewReader(line))
		record, err := reader.Read()
		if err != nil {
			continue
		}

		if len(record) < 4 {
			continue
		}

		_, ipNet, err := net.ParseCIDR(record[0])
		if err != nil {
			continue
		}

		entries = append(entries, GeofeedEntry{
			Prefix:  ipNet,
			Country: record[1],
			Region:  record[2],
			City:    record[3],
		})
	}
	return entries, scanner.Err()
}
