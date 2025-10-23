#!/usr/bin/env python3
"""
Fetch every EMP feed listed in data/sources.txt, store raw copies,
and rebuild the processed stats + site data.
"""
from __future__ import annotations

import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable, Tuple

from build_stats import main as build_stats_main

ROOT = Path(__file__).resolve().parents[1]
SOURCES_FILE = ROOT / "data" / "sources.txt"
RAW_DIR = ROOT / "data" / "raw"

EMP_PATTERN = re.compile(r"/emp/(\d+)/")


def read_sources() -> Iterable[str]:
    if not SOURCES_FILE.exists():
        raise SystemExit(f"Sources file not found: {SOURCES_FILE}")

    with SOURCES_FILE.open("r", encoding="utf-8") as handle:
        for line in handle:
            cleaned = line.strip()
            if not cleaned or cleaned.startswith("#"):
                continue
            yield cleaned


def parse_match_id(url: str) -> int:
    match = EMP_PATTERN.search(url)
    if not match:
        raise ValueError(f"Could not extract match id from URL: {url}")
    return int(match.group(1))


def fetch_feed(url: str, match_id: int) -> Tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": "kog-stats-fetcher/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP error {exc.code} for {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to fetch {url}: {exc.reason}") from exc

    if not data:
        raise RuntimeError(f"No data returned for {url}")

    return match_id, data


def write_raw_feed(match_id: int, data: bytes) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    target = RAW_DIR / f"game_{match_id}.json"
    target.write_bytes(data)
    return target


def main() -> None:
    fetched: list[Path] = []
    for url in read_sources():
        try:
            match_id = parse_match_id(url)
        except ValueError as exc:
            print(f"[WARN] {exc}")
            continue

        cached_path = RAW_DIR / f"game_{match_id}.json"
        if cached_path.exists():
            print(f"Skipping game {match_id}; cached feed found at {cached_path}")
            continue

        try:
            match_id, payload = fetch_feed(url, match_id)
        except RuntimeError as exc:
            print(f"[WARN] {exc}")
            continue

        path = write_raw_feed(match_id, payload)
        fetched.append(path)
        print(f"Saved game {match_id} -> {path}")

    if fetched:
        print("Fetched new feeds; regenerating outputs…")
    else:
        print("No new feeds fetched; using cached data.")

    print("Rebuilding processed stats…")
    build_stats_main()
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
