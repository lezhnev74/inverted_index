package main

import (
	"fmt"
	"github.com/lezhnev74/inverted-index/single"
	"github.com/urfave/cli/v2"

	"log"
	"os"
)

func main() {
	app := &cli.App{
		Name:  "inspect",
		Usage: "Show one index file's summary",
		Action: func(cCtx *cli.Context) error {
			filename := cCtx.Args().First()
			if len(filename) == 0 {
				return fmt.Errorf("provide the index file as the first argument")
			}
			return single.PrintSummary(filename, os.Stdout)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
