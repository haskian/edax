#!/usr/bin/env python3
"""Fail on documentation regressions: long sentences, line citations, idioms.

`--extract DIR` writes every fenced Python block marked `<!-- doclint: check -->` into DIR as a
module, for a type checker to run over. Adopters copy documentation snippets verbatim, so a snippet
that no longer type-checks against the SDK is a defect in the SDK's front door. The marker is opt-in
because most fences here are fragments by design: they elide with `...`, or name a store the
surrounding prose defines.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

MAX_WORDS = 30
IDIOMS = [
    "load-bearing",
    "thundering herd",
    "spin the broker",
    "merely slow",
    "baked around",
    "burning attempts",
    "sail past",
    "clobber",
    "knob",
    "plumbing",
    "the far end",
    "un-send",
    "stands in for",
    "the very bug",
    "drop-in swap",
    "worth a second look",
    "does not pretend",
    "footprint",
    "mid-flight",
    "sidestep",
    "engineer away",
    "in four beats",
    "honour",
    "gotcha",
]
CHECK_MARKER = "<!-- doclint: check -->"
# A walkthrough builds up over several fences: this one is checked with every marked fence above it
# in the same file prepended, so it resolves the names those defined.
CONTINUES_MARKER = "<!-- doclint: continues -->"
CITATION = re.compile(r"crates/[\w./-]+:\d+")
IDIOM = re.compile(r"(?i)(?<![a-z])(" + "|".join(map(re.escape, IDIOMS)) + r")")
SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
ROOT = Path(__file__).resolve().parent.parent


def check(path: Path) -> list[str]:
    """Report every problem in one Markdown file, skipping code fences and tables."""
    name = path.relative_to(ROOT)
    problems: list[str] = []
    paragraph: list[str] = []
    start = 0
    fenced = False
    for number, line in enumerate([*path.read_text(encoding="utf-8").split("\n"), ""], 1):
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if fenced or line.lstrip().startswith(("|", "#", ">")):
            continue
        if line.strip():
            start = start or number
            paragraph.append(line.strip())
            problems += [f"{name}:{number}: line-number citation `{h}`" for h in CITATION.findall(line)]
            problems += [f"{name}:{number}: idiom '{h}'" for h in IDIOM.findall(line)]
            continue
        for sentence in SENTENCE_END.split(" ".join(paragraph)):
            words = len(sentence.split())
            if words > MAX_WORDS:
                problems.append(f"{name}:{start}: sentence of {words} words, limit {MAX_WORDS}")
        paragraph, start = [], 0
    return problems


def snippets(path: Path) -> list[tuple[int, str]]:
    """Every fenced Python block the file marks for type-checking, as (line of the fence, source)."""
    found: list[tuple[int, str]] = []
    marker = ""
    opened = 0
    body: list[str] = []
    preceding = ""  # what the marked fences above this one in the file defined
    for number, line in enumerate(path.read_text(encoding="utf-8").split("\n"), 1):
        if opened:
            if line.startswith("```"):
                source = "\n".join(body) + "\n"
                preceding = source if marker == CHECK_MARKER else preceding + source
                found.append((opened, preceding if marker == CONTINUES_MARKER else source))
                opened, body, marker = 0, [], ""
            else:
                body.append(line)
        elif line.strip() in (CHECK_MARKER, CONTINUES_MARKER):
            marker = line.strip()
        elif marker and line.startswith("```python"):
            opened = number
        elif line.strip():
            marker = ""  # the marker applies to the fence that follows it, nothing else
    return found


def extract(files: list[Path], into: Path) -> int:
    """Write the marked snippets into `into` as modules. Print the count; empty means the marker moved."""
    into.mkdir(parents=True, exist_ok=True)
    written = 0
    for path in files:
        stem = re.sub(r"\W", "_", str(path.relative_to(ROOT).with_suffix("")))
        for line, source in snippets(path):
            (into / f"{stem}_{line}.py").write_text(source, encoding="utf-8")
            written += 1
    print(f"{written} snippets to {into}", file=sys.stderr)
    if written == 0:
        print(f"doclint: no snippet carries `{CHECK_MARKER}`", file=sys.stderr)
    return 1 if written == 0 else 0


def main(argv: list[str]) -> int:
    files = [*sorted(ROOT.joinpath("docs").rglob("*.md")), ROOT / "README.md", ROOT / "examples/py/README.md"]
    if argv[:1] == ["--extract"]:
        if len(argv) != 2:
            print("usage: doclint.py [--extract DIR]", file=sys.stderr)
            return 2
        return extract(files, Path(argv[1]))
    problems = [problem for file in files for problem in check(file)]
    print("\n".join(problems))
    print(f"{len(problems)} problems in {len(files)} files", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
