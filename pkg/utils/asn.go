package utils

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"
)

type ASNMapping struct {
	names map[uint32]string
}

func NewASNMapping() *ASNMapping {
	return &ASNMapping{
		names: make(map[uint32]string),
	}
}

func (m *ASNMapping) Load() error {
	url := "https://thyme.apnic.net/current/data-used-autnums"
	r, err := GetCachedReader(url, true, "[ASN]")
	if err != nil {
		return fmt.Errorf("failed to fetch ASN mapping: %v", err)
	}
	defer r.Close()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		asnStr := strings.TrimPrefix(parts[0], "AS")
		asn, err := strconv.ParseUint(asnStr, 10, 32)
		if err != nil {
			continue
		}

		name := strings.Join(parts[1:], " ")
		m.names[uint32(asn)] = name
	}

	log.Printf("Loaded %d ASN mappings", len(m.names))
	return nil
}

func (m *ASNMapping) GetName(asn uint32) string {
	if name, ok := m.names[asn]; ok {
		return name
	}
	return "Unknown"
}
