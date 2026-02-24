package utils

var BeaconPrefixes = map[string]bool{
	"84.205.65.0/24":   true, // Beacon, RRC01
	"84.205.81.0/24":   true, // Anchor, RRC01
	"84.205.67.0/24":   true, // Beacon, RRC03
	"84.205.64.0/24":   true, // Beacon (Anycast), RRC00, RRC25
	"84.205.80.0/24":   true, // Anchor (Anycast), RRC00, RRC25
	"84.205.69.0/24":   true, // Beacon (Anycast), RRC04, 05, 07, 10, 13, 18, 20, 21, 22, 26
	"84.205.85.0/24":   true, // Anchor (Anycast), RRC04, 05, 07, 10, 13, 18, 20, 21, 22, 26
	"84.205.70.0/24":   true, // Beacon (Anycast), RRC06, RRC23
	"84.205.86.0/24":   true, // Anchor (Anycast), RRC06, RRC23
	"84.205.75.0/24":   true, // Beacon (Anycast), RRC11, RRC14, RRC16
	"84.205.91.0/24":   true, // Anchor (Anycast), RRC11, RRC14, RRC16
	"84.205.82.0/24":   true, // Beacon (Anycast), RRC19
	"84.205.83.0/24":   true, // Anchor, RRC03
	"84.205.76.0/24":   true, // Beacon, RRC12
	"84.205.92.0/24":   true, // Anchor, RRC12
	"84.205.88.0/24":   true, // Anchor, Anycast AFRINIC
	"93.175.153.0/24":  true, // Beacon, Anycast LACNIC
	"93.175.152.0/24":  true, // Anchor, Anycast LACNIC
	"93.175.154.0/25":  true, // Anchor, Long Prefix
	"93.175.154.128/28": true, // Anchor, Long Prefix
	"84.205.66.0/24":   true, // Beacon, Fast Paced
	"93.175.146.0/24":  true, // RPKI Valid
	"93.175.147.0/24":  true, // RPKI Invalid
}

func IsBeaconPrefix(prefix string) bool {
	_, ok := BeaconPrefixes[prefix]
	return ok
}
