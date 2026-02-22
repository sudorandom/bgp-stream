package bgpengine

import _ "embed"

//go:embed data/world.geo.json
var worldGeoJSON []byte

//go:embed data/ipinfo_lite.mmdb
var geoIPDB []byte

//go:embed data/worldcities.csv
var worldCitiesCSV []byte
