package geoservice

import _ "embed"

// We no longer embed worldcities.csv to reduce binary size and allow for a better source.
// This is downloaded on startup if missing.
var worldCitiesCSV []byte
