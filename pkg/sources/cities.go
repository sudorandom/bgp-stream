package sources

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func DownloadWorldCities(dest string) error {
	return utils.DownloadFile(WorldCitiesURL, dest)
}

type CityDominance struct {
	Country             string
	Coordinates         []float64
	LogicalDominanceIPs float64 `json:"logical_dominance_ips"`
}

func FetchCityDominance() ([]CityDominance, error) {
	resp, err := http.Get(CityDominanceMetaURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var meta struct {
		MaxYear int `json:"max_year"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, err
	}

	resp2, err := http.Get(fmt.Sprintf(CityDominanceDataURL, meta.MaxYear))
	if err != nil {
		return nil, err
	}
	defer resp2.Body.Close()

	var cities []CityDominance
	if err := json.NewDecoder(resp2.Body).Decode(&cities); err != nil {
		return nil, err
	}

	return cities, nil
}
