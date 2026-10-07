// Command check-go-version validates a release's Go module path and prints its tag.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

const moduleDir = "crates/edax-go"
const moduleBase = "github.com/haskian/edax/" + moduleDir

func main() {
	root := flag.String("root", "..", "repository root")
	flag.Parse()
	if err := run(*root, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "version:check-go:", err)
		os.Exit(1)
	}
}

func run(root string, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: mise run version:check-go -- VERSION")
	}
	source, err := os.ReadFile(filepath.Join(root, moduleDir, "go.mod"))
	if err != nil {
		return err
	}
	tag, err := moduleTag(args[0], source)
	if err != nil {
		return err
	}
	fmt.Println(tag)
	return nil
}

func moduleTag(version string, source []byte) (string, error) {
	canonical := "v" + version
	// A release tag must carry all three version components, with no build metadata.
	if !semver.IsValid(canonical) || semver.Canonical(canonical) != canonical {
		return "", fmt.Errorf("%q is not a canonical Go release version", version)
	}
	parsed, err := modfile.Parse("go.mod", source, nil)
	if err != nil {
		return "", err
	}
	if parsed.Module == nil {
		return "", fmt.Errorf("go.mod has no module declaration")
	}
	path := parsed.Module.Mod.Path
	prefix, _, ok := module.SplitPathVersion(path)
	if !ok || prefix != moduleBase {
		return "", fmt.Errorf("unexpected Go module path %q; want %s with the release's major suffix", path, moduleBase)
	}
	if err := module.Check(path, canonical); err != nil {
		return "", fmt.Errorf("update go.mod, imports, alef.toml and examples before releasing %s: %w", canonical, err)
	}
	return moduleDir + "/" + canonical, nil
}
