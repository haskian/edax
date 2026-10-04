#!/usr/bin/env bash
# Regenerate the alef-owned native binding from `crates/edax-abi`, then refresh the shipped stub.
#
# `--clean` is the only safe mode here (see `alef.toml`). alef writes the stub into the throwaway
# `crates/edax-py/.alef/`; the shipped copy is its body under a fixed PEP 561 header, without alef's
# `# alef:hash:` lines, so alef's stale-file cleanup leaves the shipped copy alone. CI runs this and
# then `git diff --exit-code` on both outputs, so a forgotten regeneration fails the build.
set -euo pipefail
cd "$(dirname "$0")/.."

mise exec -- alef generate --lang python --clean

shipped=crates/edax-py/python/edax/_edax.pyi
{
  cat <<'HEADER'
# Committed type stub for the alef-generated `_edax` extension (PEP 561, beside `_edax.abi3.so`
# + `py.typed`). Gives the message records their field types; the native classes have none.
# Copy of `crates/edax-py/.alef/_edax.pyi` with the alef header stripped so alef cleanup leaves it.
# Never edit by hand: regenerate with `scripts/regenerate_binding.sh`.

HEADER
  tail -n +6 crates/edax-py/.alef/_edax.pyi
} > "$shipped.tmp"
mv "$shipped.tmp" "$shipped"
