// Command go-facade wraps alef's generated Go API with checked native ownership.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	root := flag.String("root", ".", "repository root")
	flag.Parse()
	if err := run(*root); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if err := generateFacade(root); err != nil {
		return err
	}
	return generateTaxonomy(root)
}
