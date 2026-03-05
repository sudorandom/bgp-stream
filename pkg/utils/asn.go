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
		23764:  "CHINATELECOM",
		4809:   "CHINATELECOM",
		4134:   "CHINATELECOM",
		4812:   "CHINATELECOM",
		4811:   "CHINATELECOM",
		4813:   "CHINATELECOM",
		4816:   "CHINATELECOM",
		139203: "CHINATELECOM",
		36678:  "CHINATELECOM",

		// China Mobile
		58453: "CHINAMOBILE",
		58807: "CHINAMOBILE",
		9808:  "CHINAMOBILE",
		9242:  "CHINAMOBILE",
		56040: "CHINAMOBILE",
		56041: "CHINAMOBILE",
		56042: "CHINAMOBILE",

		// China Unicom
		4837:  "CHINAUNICOM",
		9929:  "CHINAUNICOM",
		10010: "CHINAUNICOM",
		17622: "CHINAUNICOM",
		18398: "CHINAUNICOM",

		// Telstra
		1221:   "TELSTRA",
		4637:   "TELSTRA",
		137409: "TELSTRA",

		// NTT
		2914:  "NTT",
		10103: "NTT",
		2516:  "NTT",
		2907:  "NTT",
		7682:  "NTT",
		17451: "NTT",

		// Tata
		6453:  "TATA",
		4755:  "TATA",
		17488: "TATA",

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
		286:   "LUMEN",
		2828:  "LUMEN",

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

		// AT&T
		7018: "ATT",
		2686: "ATT",
		7132: "ATT",

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

		// Cogent
		174: "COGENT",

		// Hurricane Electric
		6939: "HE",

		// Cloudflare
		13335: "CLOUDFLARE",

		// Google
		15169: "GOOGLE",
		36040: "GOOGLE",

		// Amazon
		16509: "AMAZON",
		14618: "AMAZON",

		// Microsoft
		8075:  "MICROSOFT",
		12076: "MICROSOFT",

		// Akamai
		20940: "AKAMAI",
		16625: "AKAMAI",
		12241: "AKAMAI",
		34164: "AKAMAI",
		35994: "AKAMAI",
		35993: "AKAMAI",
		18631: "AKAMAI",
		18715: "AKAMAI",

		// Fastly
		54113: "FASTLY",
	}

	for asn, orgID := range knownSiblings {
		info := m.data[asn]
		info.OrgID = orgID
		m.data[asn] = info
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
			Type  string `json:"type"`
			ASN   string `json:"asn"`
			OrgID string `json:"organizationId"`
			Name  string `json:"name"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}

		if entry.Type == "ASN" && entry.ASN != "" && entry.OrgID != "" {
			asn, err := strconv.ParseUint(entry.ASN, 10, 32)
			if err != nil {
				continue
			}

			// Unify regional OrgIDs for major providers
			orgID := entry.OrgID
			normalizedName := strings.ToUpper(entry.Name)
			switch {
			case strings.Contains(orgID, "AKAMAI") || strings.Contains(normalizedName, "AKAMAI"):
				orgID = "AKAMAI"
			case strings.Contains(orgID, "CHINATELECOM") || strings.Contains(normalizedName, "CHINATELECOM"):
				orgID = "CHINATELECOM"
			case strings.Contains(orgID, "CHINAMOBILE") || strings.Contains(normalizedName, "CHINAMOBILE"):
				orgID = "CHINAMOBILE"
			case strings.Contains(orgID, "CHINAUNICOM") || strings.Contains(normalizedName, "CHINAUNICOM"):
				orgID = "CHINAUNICOM"
			case strings.Contains(orgID, "GOOGLE") || strings.Contains(normalizedName, "GOOGLE"):
				orgID = "GOOGLE"
			case strings.Contains(orgID, "AMAZON") || strings.Contains(normalizedName, "AMAZON"):
				orgID = "AMAZON"
			case strings.Contains(orgID, "MICROSOFT") || strings.Contains(normalizedName, "MICROSOFT"):
				orgID = "MICROSOFT"
			case strings.Contains(orgID, "CLOUDFLARE") || strings.Contains(normalizedName, "CLOUDFLARE"):
				orgID = "CLOUDFLARE"
			case strings.Contains(orgID, "TELSTRA") || strings.Contains(normalizedName, "TELSTRA"):
				orgID = "TELSTRA"
			case strings.Contains(orgID, "TATA") || strings.Contains(normalizedName, "TATA"):
				orgID = "TATA"
			case strings.Contains(orgID, "LUMEN") || strings.Contains(normalizedName, "LUMEN") || strings.Contains(normalizedName, "LEVEL3") || strings.Contains(normalizedName, "CENTURYLINK"):
				orgID = "LUMEN"
			case strings.Contains(orgID, "COGENT") || strings.Contains(normalizedName, "COGENT"):
				orgID = "COGENT"
			case strings.Contains(orgID, "VERIZON") || strings.Contains(normalizedName, "VERIZON"):
				orgID = "VERIZON"
			case strings.Contains(orgID, "ATT") || strings.Contains(normalizedName, "AT&T") || strings.Contains(normalizedName, "ATT-"):
				orgID = "ATT"
			case strings.Contains(orgID, "ORANGE") || strings.Contains(normalizedName, "ORANGE"):
				orgID = "ORANGE"
			case strings.Contains(orgID, "TELEFONICA") || strings.Contains(normalizedName, "TELEFONICA"):
				orgID = "TELEFONICA"
			case strings.Contains(orgID, "GTT") || strings.Contains(normalizedName, "GTT-"):
				orgID = "GTT"
			case strings.Contains(orgID, "NTT") || strings.Contains(normalizedName, "NTT-"):
				orgID = "NTT"
			}

			info := m.data[uint32(asn)]
			info.OrgID = orgID
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

// SetASNName is a test helper to manually set name for an ASN.
func SetASNName(m *ASNMapping, asn uint32, name string) {
	info := m.data[asn]
	info.Name = name
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
