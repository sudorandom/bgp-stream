package utils

// ExcludedASNs maps ASNs to their exclusion category
var ExcludedASNs = map[uint32]string{
	// 1. DoD Honeypot Operations
	749:    "DoD Honeypot",
	8003:   "DoD Honeypot",

	// 2. BGP Research and Beacons
	12654:  "BGP Research",
	6447:   "BGP Research",

	// 3. Dedicated Security Scanners
	398324: "Security Scanner",
	398722: "Security Scanner",
	398705: "Security Scanner",
	22168:  "Security Scanner",
	10439:  "Security Scanner",
}

func GetExcludedASNCategory(asn uint32) (string, bool) {
	cat, ok := ExcludedASNs[asn]
	return cat, ok
}

func IsExcludedASN(asn uint32) bool {
	_, ok := ExcludedASNs[asn]
	return ok
}
