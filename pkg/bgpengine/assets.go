package bgpengine

import _ "embed"

//go:embed data/world.geo.json
var worldGeoJSON []byte

//go:embed data/ipinfo_lite.mmdb
var geoIPDB []byte

//go:embed fonts/Inter/static/Inter_24pt-Medium.ttf
var fontInter []byte

//go:embed fonts/Roboto_Mono/static/RobotoMono-Medium.ttf
var fontMono []byte

// We no longer embed worldcities.csv to reduce binary size and allow for a better source.
// This is downloaded on startup if missing.
var worldCitiesCSV []byte
