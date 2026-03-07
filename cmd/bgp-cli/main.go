package main

import (
	"github.com/alecthomas/kong"
)

type CLI struct {
	Fetch       FetchCmd       `cmd:"" help:"Download and process required data for the engine."`
	Report      ReportCmd      `cmd:"" help:"Generate a report of current BGP prefixes in specific states."`
	Analyze     AnalyzeCmd     `cmd:"" help:"Analyze MRT files and generate a state transition report."`
	DebugGeo    DebugGeoCmd    `cmd:"" help:"Debug geolocation lookups for an IP address."`
	DebugPrefix DebugPrefixCmd `cmd:"" help:"Watch a specific BGP prefix stream for debugging."`
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name("bgp-cli"),
		kong.Description("A collection of CLI tools for BGP stream processing and analysis."),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
			Summary: true,
		}))

	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}
