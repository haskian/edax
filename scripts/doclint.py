#!/usr/bin/env python3
"""Fail on documentation regressions: long sentences, line citations, idioms."""

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


def main() -> int:
    files = [*sorted(ROOT.joinpath("docs").rglob("*.md")), ROOT / "README.md"]
    problems = [problem for file in files for problem in check(file)]
    print("\n".join(problems))
    print(f"{len(problems)} problems in {len(files)} files", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
