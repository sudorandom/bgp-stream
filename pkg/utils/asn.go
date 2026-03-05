// Package utils provides various utility functions and data structures for BGP stream processing.
package utils

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
)

type ASNInfo struct {
	Name  string
	CC    string
	OrgID string
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
	// 1. Load baseline Name/CC from APNIC (Thyme)
	if err := m.loadThyme(); err != nil {
		log.Printf("Warning: Failed to load Thyme ASN mapping: %v", err)
	}

	// 2. Load better names from PeeringDB
	if err := m.loadPeeringDB(); err != nil {
		log.Printf("Warning: Failed to load PeeringDB ASN mapping: %v", err)
	}

	// 3. Load OrgIDs from CAIDA
	if err := m.loadCAIDA(); err != nil {
		log.Printf("Warning: Failed to load CAIDA AS-Org mapping: %v", err)
	}

	// 4. Load custom associations (Manual Overrides/Additions)
	m.loadCustomOrgs()

	log.Printf("Loaded %d unique ASN mappings across all sources", len(m.data))
	return nil
}

func (m *ASNMapping) loadCustomOrgs() {
	// Hardcoded known associations that are often missed or split in CAIDA
	knownSiblings := map[uint32]string{
		// China Telecom / CTGNet / CN2
		23764: "CHINATELECOM",
		4809:  "CHINATELECOM",
		4134:  "CHINATELECOM",
		4812:  "CHINATELECOM",

		// China Mobile
		58453: "CHINAMOBILE",
		58807: "CHINAMOBILE",
		9808:  "CHINAMOBILE",
		9242:  "CHINAMOBILE",

		// China Unicom
		4837:  "CHINAUNICOM",
		9929:  "CHINAUNICOM",
		10010: "CHINAUNICOM",

		// Telstra
		1221: "TELSTRA",
		4637: "TELSTRA",

		// NTT
		2914:  "NTT",
		10103: "NTT",
		2516:  "NTT",
		2907:  "NTT",
		7682:  "NTT",

		// Tata
		6453: "TATA",
		4755: "TATA",

		// GTT
		3257:  "GTT",
		5580:  "GTT",
		4436:  "GTT",
		29686: "GTT",
		225:   "GTT",

		// Lumen / Level 3 / CenturyLink / Global Crossing
		3356:  "LUMEN",
		209:   "LUMEN",
		3549:  "LUMEN",
		22561: "LUMEN",

		// Zayo
		6461:  "ZAYO",
		12008: "ZAYO",

		// Telia
		1299:  "TELIA",
		10492: "TELIA",

		// Verizon / MCI / XO
		701:   "VERIZON",
		702:   "VERIZON",
		703:   "VERIZON",
		18451: "VERIZON",
		2828:  "VERIZON",

		// AT&T
		7018: "ATT",
		2686: "ATT",

		// Orange / OpenTransit
		5511: "ORANGE",
		3215: "ORANGE",

		// Telefonica
		12956: "TELEFONICA",
		6739:  "TELEFONICA",
		22927: "TELEFONICA",
		3326:  "TELEFONICA",
		33667: "TELEFONICA",
		13489: "TELEFONICA",
	}

	for asn, orgID := range knownSiblings {
		info := m.data[asn]
		info.OrgID = orgID
		m.data[asn] = info
	}

	// Also try to load from a local file if users want to maintain their own
	if data, err := os.ReadFile("./data/custom_orgs.json"); err == nil {
		var localMapping map[string]string
		if err := json.Unmarshal(data, &localMapping); err == nil {
			for asnStr, orgID := range localMapping {
				if asn, err := strconv.ParseUint(asnStr, 10, 32); err == nil {
					info := m.data[uint32(asn)]
					info.OrgID = orgID
					m.data[uint32(asn)] = info
				}
			}
			log.Printf("Loaded %d custom ASN-Org associations from ./data/custom_orgs.json", len(localMapping))
		}
	}
}

func (m *ASNMapping) loadCAIDA() error {
	// Using the JSONL 'latest' symlink which is more reliable and easier to parse
	url := "https://publicdata.caida.org/datasets/as-organizations/latest.as-org2info.jsonl.gz"
	r, err := GetCachedReader(url, true, "[ASN-CAIDA]")
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("Error closing CAIDA reader: %v", err)
		}
	}()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var entry struct {
			Type           string `json:"type"`
			ASN            string `json:"asn"`
			OrganizationId string `json:"organizationId"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}

		if strings.EqualFold(entry.Type, "ASN") && entry.ASN != "" && entry.OrganizationId != "" {
			asn, err := strconv.ParseUint(entry.ASN, 10, 32)
			if err != nil {
				continue
			}
			info := m.data[uint32(asn)]
			info.OrgID = entry.OrganizationId
			m.data[uint32(asn)] = info
		}
	}
	return nil
}

func (m *ASNMapping) GetOrgID(asn uint32) string {
	if info, ok := m.data[asn]; ok {
		return info.OrgID
	}
	return ""
}

// SetASNOrgID is a test helper to manually set OrgID for an ASN.
func SetASNOrgID(m *ASNMapping, asn uint32, orgID string) {
	info := m.data[asn]
	info.OrgID = orgID
	m.data[asn] = info
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

		info := m.data[uint32(asn)]
		info.Name = name
		info.CC = cc
		m.data[uint32(asn)] = info
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
			log.Printf("Error closing PeeringDB ASN reader: %v", err)
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
		info := m.data[item.ASN]
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
