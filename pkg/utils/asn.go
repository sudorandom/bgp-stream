// Package utils provides various utility functions and data structures for BGP stream processing.
package utils

import (
	"bufio"
	"encoding/json"
	"log"
	"strconv"
	"strings"
)

type ASNInfo struct {
	Name string
	CC   string
}

type ASNMapping struct {
	data map[uint32]ASNInfo
}

func NewASNMapping() *ASNMapping {
	return &ASNMapping{
		data: make(map[uint32]ASNInfo),
	}
}

func (m *ASNMapping) Load() error {
	// 1. Load from APNIC (Thyme) - Good baseline with CCs
	if err := m.loadThyme(); err != nil {
		log.Printf("Warning: Failed to load Thyme ASN mapping: %v", err)
	}

	// 2. Load from PeeringDB - Very comprehensive and modern names
	if err := m.loadPeeringDB(); err != nil {
		log.Printf("Warning: Failed to load PeeringDB ASN mapping: %v", err)
	}

	log.Printf("Loaded %d unique ASN mappings across all sources", len(m.data))
	return nil
}

func (m *ASNMapping) loadThyme() error {
	url := "https://thyme.apnic.net/current/data-used-autnums"
	r, err := GetCachedReader(url, true, "[ASN-THYME]")
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("Error closing Thyme ASN reader: %v", err)
		}
	}()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		asnStr := strings.TrimPrefix(fields[0], "AS")
		asn, err := strconv.ParseUint(asnStr, 10, 32)
		if err != nil {
			continue
		}

		desc := strings.Join(fields[1:], " ")
		cc := ""
		name := desc
		if lastComma := strings.LastIndex(desc, ","); lastComma != -1 {
			cc = strings.TrimSpace(desc[lastComma+1:])
			name = strings.TrimSpace(desc[:lastComma])
		}

		m.data[uint32(asn)] = ASNInfo{Name: name, CC: cc}
	}
	return nil
}

func (m *ASNMapping) loadPeeringDB() error {
	url := "https://www.peeringdb.com/api/net?fields=asn,name"
	r, err := GetCachedReader(url, true, "[ASN-PDB]")
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("Error closing Thyme ASN reader: %v", err)
		}
	}()

	var response struct {
		Data []struct {
			ASN  uint32 `json:"asn"`
			Name string `json:"name"`
		} `json:"data"`
	}

	if err := json.NewDecoder(r).Decode(&response); err != nil {
		return err
	}

	for _, item := range response.Data {
		if item.ASN == 0 || item.Name == "" {
			continue
		}
		info, ok := m.data[item.ASN]
		if !ok {
			info = ASNInfo{CC: ""}
		}
		// Prioritize PeeringDB names as they are usually cleaner
		info.Name = item.Name
		m.data[item.ASN] = info
	}
	return nil
}

func (m *ASNMapping) GetName(asn uint32) string {
	if info, ok := m.data[asn]; ok {
		return info.Name
	}
	return "Unknown"
}

func (m *ASNMapping) GetCC(asn uint32) string {
	if info, ok := m.data[asn]; ok {
		return info.CC
	}
	return ""
}
