#!/usr/bin/env python3
"""Rewrite every version literal in the repo. Run by `cog bump` through `pre_bump_hooks`.

Two files carry one, and only two:

* `Cargo.toml`: `[workspace.package] version`, which every member crate inherits with
  `version.workspace = true`, including `edax-py`.
* `crates/edax-abi/Cargo.toml`: a literal, because alef reads the generated binding's version
  straight out of that file (`version_from` in `alef.toml`), so it cannot be `version.workspace`.

`crates/edax-py/pyproject.toml` declares `dynamic = ["version"]`, so maturin reads the Cargo version
and there is nothing to rewrite there. `crates/edax-abi-py/` is alef-generated and outside the
workspace, so it is regenerated rather than bumped.

Usage: `set_version.py 0.1.0`. Idempotent, and fails loudly if a file it expects to change does not.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (path, the line that carries the version), anchored so a dependency pin cannot match by accident.
TARGETS = [
    (ROOT / "Cargo.toml", re.compile(r'^version = "[^"]+"$', re.MULTILINE)),
    (ROOT / "crates/edax-abi/Cargo.toml", re.compile(r'^version = "[^"]+"$', re.MULTILINE)),
]

SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.\-]+)?$")


def main(version: str) -> int:
    if not SEMVER.match(version):
        print(f"set_version: {version!r} is not a semantic version", file=sys.stderr)
        return 2
    for path, pattern in TARGETS:
        text = path.read_text()
        replaced, count = pattern.subn(f'version = "{version}"', text)
        if count != 1:
            print(f"set_version: {path} matched {count} version lines, expected 1", file=sys.stderr)
            return 1
        path.write_text(replaced)
        print(f"set_version: {path.relative_to(ROOT)} -> {version}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
