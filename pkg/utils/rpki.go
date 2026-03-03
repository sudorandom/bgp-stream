package utils

import (
	"encoding/json"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

type RPKIStatus int

const (
	RPKIUnknown RPKIStatus = iota
	RPKIValid
	RPKIInvalidASN
	RPKIInvalidMaxLength
)

func (s RPKIStatus) String() string {
	switch s {
	case RPKIValid:
		return "Valid"
	case RPKIInvalidASN:
		return "InvalidASN"
	case RPKIInvalidMaxLength:
		return "InvalidMaxLength"
	default:
		return "Unknown"
	}
}

type VRP struct {
	Prefix    string `json:"prefix"`
	MaxLength int    `json:"maxLength"`
	ASN       uint32 `json:"asn"`
}

type RPKIManager struct {
	trie *DiskTrie
	mu   sync.RWMutex
}

func NewRPKIManager(dbPath string) (*RPKIManager, error) {
	trie, err := OpenDiskTrie(dbPath)
	if err != nil {
		return nil, err
	}
	return &RPKIManager{trie: trie}, nil
}

func (m *RPKIManager) Close() error {
	return m.trie.Close()
}

func (m *RPKIManager) Sync() error {
	// Using a reliable public VRP export
	url := "https://console.rpki-client.org/vrps.json"
	log.Printf("[RPKI] Syncing VRPs from %s", url)
	
	r, err := GetCachedReader(url, true, "[RPKI]")
	if err != nil {
		return err
	}
	defer r.Close()

	var data struct {
		VRPs []struct {
			ASN    interface{} `json:"asn"`
			Prefix string      `json:"prefix"`
			MaxLen int         `json:"maxLength"`
		} `json:"roas"`
	}

	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return err
	}

	vrpMap := make(map[string][]VRP)
	for _, raw := range data.VRPs {
		var asn uint32
		switch v := raw.ASN.(type) {
		case float64:
			asn = uint32(v)
		case string:
			a, _ := strconv.ParseUint(strings.TrimPrefix(v, "AS"), 10, 32)
			asn = uint32(a)
		}

		if asn == 0 || raw.Prefix == "" {
			continue
		}

		v := VRP{
			Prefix:    raw.Prefix,
			MaxLength: raw.MaxLen,
			ASN:       asn,
		}
		vrpMap[raw.Prefix] = append(vrpMap[raw.Prefix], v)
	}

	encodedMap := make(map[string][]byte)
	for prefix, vrps := range vrpMap {
		b, _ := json.Marshal(vrps)
		encodedMap[prefix] = b
	}

	if err := m.trie.Clear(); err != nil {
		return err
	}
	if err := m.trie.BatchInsert(encodedMap); err != nil {
		return err
	}

	log.Printf("[RPKI] Loaded %d prefixes with ROAs", len(vrpMap))
	return nil
}

// SetVRPInTrie is a test helper to manually set VRP data.
func SetVRPInTrie(m *RPKIManager, prefix string, data []byte) error {
	return m.trie.BatchInsert(map[string][]byte{prefix: data})
}

func (m *RPKIManager) Validate(prefix string, originASN uint32) (RPKIStatus, error) {
	_, ipNet, err := net.ParseCIDR(prefix)
	if err != nil {
		return RPKIUnknown, err
	}
	ones, _ := ipNet.Mask.Size()

	vals, err := m.trie.LookupAll(ipNet.IP)
	if err != nil {
		return RPKIUnknown, err
	}
	if len(vals) == 0 {
		return RPKIUnknown, nil
	}

	hasCoveringROA := false
	hasMatchingASN := false
	for _, val := range vals {
		var vrps []VRP
		if err := json.Unmarshal(val, &vrps); err != nil {
			continue
		}

		for _, vrp := range vrps {
			hasCoveringROA = true
			if vrp.ASN == originASN {
				hasMatchingASN = true
				if ones <= vrp.MaxLength {
					return RPKIValid, nil
				}
			}
		}
	}

	if !hasCoveringROA {
		return RPKIUnknown, nil
	}
	if hasMatchingASN {
		return RPKIInvalidMaxLength, nil
	}
	return RPKIInvalidASN, nil
}
