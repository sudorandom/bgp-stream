package utils

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"sync"
)

type CloudPrefix struct {
	Prefix  *net.IPNet
	Region  string
	Service string
}

// AWS IP Ranges Format
type AWSRanges struct {
	Prefixes []struct {
		IPPrefix string `json:"ip_prefix"`
		Region   string `json:"region"`
		Service  string `json:"service"`
	} `json:"prefixes"`
}

func ParseAWSRanges(r io.Reader) ([]CloudPrefix, error) {
	var aws AWSRanges
	if err := json.NewDecoder(r).Decode(&aws); err != nil {
		return nil, err
	}

	var results []CloudPrefix
	for _, p := range aws.Prefixes {
		_, ipNet, err := net.ParseCIDR(p.IPPrefix)
		if err != nil {
			continue
		}
		results = append(results, CloudPrefix{
			Prefix:  ipNet,
			Region:  p.Region,
			Service: p.Service,
		})
	}
	return results, nil
}

// Google Cloud IP Ranges Format
type GoogleRanges struct {
	Prefixes []struct {
		IPv4Prefix string `json:"ipv4Prefix"`
		IPv6Prefix string `json:"ipv6Prefix"`
		Location   string `json:"location"`
	} `json:"prefixes"`
}

func ParseGoogleRanges(r io.Reader) ([]CloudPrefix, error) {
	var goog GoogleRanges
	if err := json.NewDecoder(r).Decode(&goog); err != nil {
		return nil, err
	}

	var results []CloudPrefix
	for _, p := range goog.Prefixes {
		prefix := p.IPv4Prefix
		if prefix == "" {
			continue
		}
		_, ipNet, err := net.ParseCIDR(prefix)
		if err != nil {
			continue
		}
		results = append(results, CloudPrefix{
			Prefix: ipNet,
			Region: p.Location,
		})
	}
	return results, nil
}

// Azure IP Ranges Format (JSON)
type AzureRanges struct {
	Values []struct {
		Name       string `json:"name"`
		Properties struct {
			Region          string   `json:"region"`
			AddressPrefixes []string `json:"addressPrefixes"`
		} `json:"properties"`
	} `json:"values"`
}

func ParseAzureRanges(r io.Reader) ([]CloudPrefix, error) {
	var azure AzureRanges
	if err := json.NewDecoder(r).Decode(&azure); err != nil {
		return nil, err
	}

	var results []CloudPrefix
	for _, v := range azure.Values {
		for _, prefix := range v.Properties.AddressPrefixes {
			_, ipNet, err := net.ParseCIDR(prefix)
			if err != nil {
				continue
			}
			results = append(results, CloudPrefix{
				Prefix:  ipNet,
				Region:  v.Properties.Region,
				Service: v.Name,
			})
		}
	}
	return results, nil
}

// Azure IP Ranges Format (XML - Legacy)
type AzureXMLRanges struct {
	Regions []struct {
		Name     string `xml:"Name,attr"`
		IpRanges []struct {
			Subnet string `xml:"Subnet,attr"`
		} `xml:"IpRange"`
	} `xml:"Region"`
}

func ParseAzureXMLRanges(r io.Reader) ([]CloudPrefix, error) {
	var azure AzureXMLRanges
	if err := xml.NewDecoder(r).Decode(&azure); err != nil {
		return nil, err
	}

	var results []CloudPrefix
	for _, reg := range azure.Regions {
		for _, ipr := range reg.IpRanges {
			_, ipNet, err := net.ParseCIDR(ipr.Subnet)
			if err != nil {
				continue
			}
			results = append(results, CloudPrefix{
				Prefix:  ipNet,
				Region:  reg.Name,
				Service: "AzureCloud",
			})
		}
	}
	return results, nil
}

// Oracle Cloud IP Ranges Format
type OracleRanges struct {
	Regions []struct {
		Region string `json:"region"`
		CIDRs  []struct {
			CIDR string `json:"cidr"`
			Tags []string `json:"tags"`
		} `json:"cidrs"`
	} `json:"regions"`
}

func ParseOracleRanges(r io.Reader) ([]CloudPrefix, error) {
	var oracle OracleRanges
	if err := json.NewDecoder(r).Decode(&oracle); err != nil {
		return nil, err
	}

	var results []CloudPrefix
	for _, reg := range oracle.Regions {
		for _, c := range reg.CIDRs {
			_, ipNet, err := net.ParseCIDR(c.CIDR)
			if err != nil {
				continue
			}
			results = append(results, CloudPrefix{
				Prefix:  ipNet,
				Region:  reg.Region,
				Service: "OCI",
			})
		}
	}
	return results, nil
}

func ParseDigitalOceanRanges(r io.Reader) ([]CloudPrefix, error) {
	reader := csv.NewReader(r)
	var results []CloudPrefix
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(record) < 3 {
			continue
		}
		// Format: prefix,country,region,city,postal
		prefix := record[0]
		_, ipNet, err := net.ParseCIDR(prefix)
		if err != nil {
			continue
		}
		// We'll use the city|country format as the "region" for DO since it's already granular
		results = append(results, CloudPrefix{
			Prefix: ipNet,
			Region: fmt.Sprintf("%s|%s", record[3], record[1]),
			Service: "DigitalOcean",
		})
	}
	return results, nil
}

// Map of Cloud Regions to City|Country
// This is a partial list of major hubs.
var CloudRegionToCity = map[string]string{
	// AWS
	"us-east-1":      "Ashburn|US",
	"us-east-2":      "Columbus|US",
	"us-west-1":      "San Francisco|US",
	"us-west-2":      "Portland|US",
	"af-south-1":     "Cape Town|ZA",
	"ap-east-1":      "Hong Kong|HK",
	"ap-south-1":     "Mumbai|IN",
	"ap-northeast-3": "Osaka|JP",
	"ap-northeast-2": "Seoul|KR",
	"ap-southeast-1": "Singapore|SG",
	"ap-southeast-2": "Sydney|AU",
	"ap-northeast-1": "Tokyo|JP",
	"ca-central-1":   "Montreal|CA",
	"eu-central-1":   "Frankfurt|DE",
	"eu-west-1":      "Dublin|IE",
	"eu-west-2":      "London|GB",
	"eu-south-1":     "Milan|IT",
	"eu-west-3":      "Paris|FR",
	"eu-north-1":     "Stockholm|SE",
	"me-south-1":     "Manama|BH",
	"sa-east-1":      "São Paulo|BR",

	// Google Cloud
	"asia-east1":              "Changhua County|TW",
	"asia-east2":              "Hong Kong|HK",
	"asia-northeast1":         "Tokyo|JP",
	"asia-northeast2":         "Osaka|JP",
	"asia-northeast3":         "Seoul|KR",
	"asia-south1":             "Mumbai|IN",
	"asia-southeast1":         "Jurong West|SG",
	"asia-southeast2":         "Jakarta|ID",
	"australia-southeast1":    "Sydney|AU",
	"europe-central2":         "Warsaw|PL",
	"europe-north1":           "Hamina|FI",
	"europe-west1":            "St. Ghislain|BE",
	"europe-west2":            "London|GB",
	"europe-west3":            "Frankfurt|DE",
	"europe-west4":            "Eemshaven|NL",
	"europe-west6":            "Zurich|CH",
	"northamerica-northeast1": "Montreal|CA",
	"southamerica-east1":      "São Paulo|BR",
	"us-central1":             "Council Bluffs|US",
	"us-east1":                "Moncks Corner|US",
	"us-east4":                "Ashburn|US",
	"us-west1":                "The Dalles|US",
	"us-west2":                "Los Angeles|US",
	"us-west3":                "Salt Lake City|US",
	"us-west4":                "Las Vegas|US",

	// Azure
	"eastus":             "Ashburn|US",
	"eastus2":            "Virginia|US",
	"westus":             "San Francisco|US",
	"westus2":            "Quincy|US",
	"centralus":          "Des Moines|US",
	"southcentralus":     "San Antonio|US",
	"northcentralus":     "Chicago|US",
	"westcentralus":      "Cheyenne|US",
	"canadacentral":      "Toronto|CA",
	"canadaeast":         "Quebec City|CA",
	"northeurope":        "Dublin|IE",
	"westeurope":         "Amsterdam|NL",
	"uksouth":            "London|GB",
	"ukwest":             "Cardiff|GB",
	"francecentral":      "Paris|FR",
	"francesouth":        "Marseille|FR",
	"germanywestcentral": "Frankfurt|DE",
	"germanynorth":       "Berlin|DE",
	"norwayeast":         "Oslo|NO",
	"norwaywest":         "Stavanger|NO",
	"swedencentral":      "Gävle|SE",
	"swedensouth":        "Lund|SE",
	"denmarkeast":        "Copenhagen|DK",
	"italynorth":         "Milan|IT",
	"polandcentral":      "Warsaw|PL",
	"switzerlandnorth":   "Zurich|CH",
	"switzerlandwest":    "Geneva|CH",
	"japaneast":          "Tokyo|JP",
	"japanwest":          "Osaka|JP",
	"koreacentral":       "Seoul|KR",
	"koreasouth":         "Busan|KR",
	"southeastasia":      "Singapore|SG",
	"eastasia":           "Hong Kong|HK",
	"australiaeast":      "Sydney|AU",
	"australiasoutheast": "Melbourne|AU",
	"australiacentral":   "Canberra|AU",
	"centralindia":       "Pune|IN",
	"southindia":         "Chennai|IN",
	"westindia":          "Mumbai|IN",
	"brazilsouth":        "São Paulo|BR",
	"brazilsoutheast":    "Rio de Janeiro|BR",
	"southafricanorth":   "Johannesburg|ZA",
	"southafricawest":    "Cape Town|ZA",
	"uaenorth":           "Dubai|AE",
	"uaecentral":         "Abu Dhabi|AE",
	"qatarcentral":       "Doha|QA",
	"israelcentral":      "Tel Aviv|IL",

	// Legacy Azure XML region names
	"useast":            "Ashburn|US",
	"useast2":           "Virginia|US",
	"uswest":            "San Francisco|US",
	"uswest2":           "Quincy|US",
	"uscentral":         "Des Moines|US",
	"ussouthcentral":    "San Antonio|US",
	"usnorthcentral":    "Chicago|US",
	"uswestcentral":     "Cheyenne|US",
	"europewest":        "Amsterdam|NL",
	"europenorth":       "Dublin|IE",
	"asiaeast":          "Hong Kong|HK",
	"asiasoutheast":     "Singapore|SG",
	"australiacentral2": "Canberra|AU",
	"indiawest":         "Mumbai|IN",
	"indiacentral":      "Pune|IN",
	"indiasouth":        "Chennai|IN",

	// Oracle Cloud (OCI)
	"us-ashburn-1":      "Ashburn|US",
	"us-phoenix-1":      "Phoenix|US",
	"us-chicago-1":      "Chicago|US",
	"eu-frankfurt-1":    "Frankfurt|DE",
	"eu-amsterdam-1":    "Amsterdam|NL",
	"eu-madrid-1":       "Madrid|ES",
	"eu-paris-1":        "Paris|FR",
	"uk-london-1":       "London|GB",
	"ap-tokyo-1":        "Tokyo|JP",
	"ap-osaka-1":        "Osaka|JP",
	"ap-seoul-1":        "Seoul|KR",
	"ap-singapore-1":    "Singapore|SG",
	"ap-mumbai-1":       "Mumbai|IN",
	"ap-hyderabad-1":    "Hyderabad|IN",
	"ap-sydney-1":       "Sydney|AU",
	"ap-melbourne-1":    "Melbourne|AU",
	"sa-saopaulo-1":     "São Paulo|BR",
}

type CloudTrie struct {
	// maps per mask length (0 to 32)
	// key is uint32 (IPv4)
	masks [33]map[uint32]string
	cache sync.Map
}

func NewCloudTrie(prefixes []CloudPrefix) *CloudTrie {
	ct := &CloudTrie{}
	for i := 0; i < 33; i++ {
		ct.masks[i] = make(map[uint32]string)
	}

	for _, p := range prefixes {
		ip := p.Prefix.IP.To4()
		if ip == nil {
			continue
		}
		ones, _ := p.Prefix.Mask.Size()

		// For DigitalOcean, the region is already city|country
		if p.Service == "DigitalOcean" {
			ct.masks[ones][binary.BigEndian.Uint32(ip)] = p.Region
			continue
		}

		if city, ok := CloudRegionToCity[p.Region]; ok {
			ct.masks[ones][binary.BigEndian.Uint32(ip)] = city
		}
	}
	return ct
}

func (ct *CloudTrie) Lookup(ip net.IP) (string, bool) {
	target := ip.To4()
	if target == nil {
		return "", false
	}

	targetInt := binary.BigEndian.Uint32(target)
	if v, ok := ct.cache.Load(targetInt); ok {
		if v == nil {
			return "", false
		}
		return v.(string), true
	}

	for maskLen := 32; maskLen >= 0; maskLen-- {
		var mask uint32
		if maskLen > 0 {
			mask = uint32(0xFFFFFFFF) << (32 - maskLen)
		} else {
			mask = 0
		}

		prefixIP := targetInt & mask
		if city, ok := ct.masks[maskLen][prefixIP]; ok {
			ct.cache.Store(targetInt, city)
			return city, true
		}
	}

	ct.cache.Store(targetInt, nil)
	return "", false
}
