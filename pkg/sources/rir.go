package sources

import (
	"fmt"
	"io"

	"github.com/sudorandom/bgp-stream/pkg/utils"
)

var RIRURLs = map[string]string{
	"APNIC":   APNICDelegatedURL,
	"RIPE":    RIPEDelegatedURL,
	"AFRINIC": AFRINICDelegatedURL,
	"LACNIC":  LACNICDelegatedURL,
	"ARIN":    ARINDelegatedURL,
}

func GetRIRReader(name string) (io.ReadCloser, error) {
	url, ok := RIRURLs[name]
	if !ok {
		return nil, fmt.Errorf("unknown RIR: %s", name)
	}
	return utils.GetCachedReader(url, true, "[RIR-"+name+"]")
}
