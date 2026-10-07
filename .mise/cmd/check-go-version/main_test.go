package main

import (
	"fmt"
	"testing"
)

func TestModuleTag(t *testing.T) {
	for _, tc := range []struct{ name, version, declaration, want string }{
		{"current", "0.6.0", "module " + moduleBase, moduleDir + "/v0.6.0"},
		{"quoted and commented", "1.2.3", fmt.Sprintf("module %q // module identity", moduleBase), moduleDir + "/v1.2.3"},
		{"next major", "2.0.0", "module " + moduleBase + "/v2", moduleDir + "/v2.0.0"},
		{"prerelease", "2.0.0-rc.1", "module " + moduleBase + "/v2", moduleDir + "/v2.0.0-rc.1"},
		{"major mismatch", "2.0.0", "module " + moduleBase, ""},
		{"leading zero version", "02.0.0", "module " + moduleBase + "/v2", ""},
		{"leading zero prerelease", "2.0.0-01", "module " + moduleBase + "/v2", ""},
		{"incomplete version", "1.2", "module " + moduleBase, ""},
		{"build metadata", "1.2.3+build", "module " + moduleBase, ""},
		{"incompatible bypass", "2.0.0+incompatible", "module " + moduleBase, ""},
		{"foreign module", "1.2.3", "module github.com/haskian/alef", ""},
		{"missing module", "1.2.3", "", ""},
		{"invalid module syntax", "1.2.3", "module \"unfinished", ""},
		{"invalid path suffix", "2.0.0", "module " + moduleBase + "/v02", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := []byte(tc.declaration + "\n\ngo 1.26.0\n")
			got, err := moduleTag(tc.version, source)
			if tc.want == "" {
				if err == nil || got != "" {
					t.Fatalf("got (%q, %v), want rejection", got, err)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("got (%q, %v), want %q", got, err, tc.want)
			}
		})
	}
}
