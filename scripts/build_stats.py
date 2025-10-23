#!/usr/bin/env python3
"""
Utility for transforming raw Profixio EMP feeds into site-ready JSON.

Usage:
    python scripts/build_stats.py

The script expects raw game feeds inside data/raw/ named like game_<id>.json.
It will write per-game summaries into data/processed/ and publish the
aggregated Kungsholmen OG player stats to docs/data/kog_players.json.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple

# Team id for Kungsholmen OG in Profixio
KOG_TEAM_ID = 1403069

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
SITE_DATA_DIR = ROOT / "docs" / "data"


@dataclass
class PlayerTotals:
    name: str
    numbers: set[str] = field(default_factory=set)
    games_played: int = 0
    free_throws: int = 0
    two_pointers: int = 0
    three_pointers: int = 0
    fouls: int = 0

    def register_game(
        self,
        number: str,
        free_throws: int,
        two_pointers: int,
        three_pointers: int,
        fouls: int,
        counted_as_played: bool,
    ) -> None:
        number = (number or "").strip()
        if number:
            self.numbers.add(number)

        if counted_as_played:
            self.games_played += 1

        self.free_throws += free_throws
        self.two_pointers += two_pointers
        self.three_pointers += three_pointers
        self.fouls += fouls

    @property
    def total_points(self) -> int:
        return self.free_throws + self.two_pointers * 2 + self.three_pointers * 3

    def as_row(self) -> Dict[str, object]:
        number = sorted(self.numbers, key=lambda n: (len(n), n))[0] if self.numbers else ""
        ppg = round(self.total_points / self.games_played, 1) if self.games_played else 0

        return {
            "name": self.name,
            "number": number,
            "gamesPlayed": self.games_played,
            "freeThrowsMade": self.free_throws,
            "fieldGoalsMade": self.two_pointers + self.three_pointers,
            "threePointsMade": self.three_pointers,
            "foulsMade": self.fouls,
            "totalPoints": self.total_points,
            "pointsPerGame": ppg,
        }


def load_raw_games() -> Iterable[Tuple[int, dict]]:
    pattern = re.compile(r"game_(\d+)\.json$")
    for raw_file in sorted(RAW_DIR.glob("game_*.json")):
        match = pattern.search(raw_file.name)
        if not match:
            continue

        with raw_file.open("r", encoding="utf-8") as handle:
            yield int(match.group(1)), json.load(handle)


def build_team_structures(game: dict) -> Dict[int, dict]:
    teams: Dict[int, dict] = {}

    for member in game.get("lineup", []):
        team_id = member.get("webTeamId")
        if team_id is None:
            continue

        team = teams.setdefault(
            team_id,
            {
                "teamId": team_id,
                "teamName": None,
                "roster": [],
                "_index": {},
            },
        )

        player_entry = {
            "playerId": member.get("id"),
            "personId": member.get("personId"),
            "number": (member.get("number") or "").strip(),
            "name": (member.get("name") or "").strip(),
            "type": member.get("type"),
            "starter": member.get("starter", False),
            "played": member.get("played", False),
            "stats": {
                "points": 0,
                "onePointMade": 0,
                "twoPointMade": 0,
                "threePointMade": 0,
                "fouls": 0,
            },
        }

        team["_index"][player_entry["playerId"]] = player_entry
        team["roster"].append(player_entry)

    points_events = {
        106: ("onePointMade", 1),
        104: ("twoPointMade", 2),
        103: ("threePointMade", 3),
    }

    for event in game.get("events", []):
        team_id = event.get("teamId")
        person = event.get("person") or {}
        player_id = person.get("id")

        team = teams.get(team_id)
        if team:
            if not team["teamName"]:
                team_name = (event.get("teamName") or "").strip()
                if team_name:
                    team["teamName"] = team_name

            player_entry = team["_index"].get(player_id)
            if player_entry:
                event_type = event.get("eventTypeId")
                if event_type in points_events and event.get("goals"):
                    key, value = points_events[event_type]
                    raw_goals = event.get("goals", 0) or 0
                    made = int(raw_goals // value) if value else int(raw_goals)
                    if made:
                        player_entry["stats"][key] += made
                        player_entry["stats"]["points"] += value * made

                if event_type == 109:  # personal foul
                    player_entry["stats"]["fouls"] += 1

    # Remove helper index before returning
    for team in teams.values():
        team.pop("_index", None)

    return teams


def write_game_summary(game_id: int, game: dict, teams: Dict[int, dict]) -> None:
    summary = {
        "gameId": game_id,
        "finalScore": game.get("gamestate", {}).get("currentScore"),
        "periodsPlayed": game.get("gamestate", {}).get("period"),
        "teamStats": list(teams.values()),
    }

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = PROCESSED_DIR / f"game_{game_id}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    pretty_raw_path = PROCESSED_DIR / f"game_{game_id}.pretty.json"
    pretty_raw_path.write_text(json.dumps(game, indent=2), encoding="utf-8")


def aggregate_kog_players(game_id: int, teams: Dict[int, dict], totals: Dict[str, PlayerTotals]) -> None:
    kog_team = teams.get(KOG_TEAM_ID)
    if not kog_team:
        return

    for player in kog_team["roster"]:
        if player.get("type") != "player":
            continue

        name = player["name"]
        stats = player["stats"]
        counted_as_played = bool(
            player.get("played")
            or player.get("starter")
            or stats["points"]
            or stats["fouls"]
            or stats["onePointMade"]
            or stats["twoPointMade"]
            or stats["threePointMade"]
        )

        record = totals.setdefault(name, PlayerTotals(name=name))
        record.register_game(
            number=player.get("number", ""),
            free_throws=stats["onePointMade"],
            two_pointers=stats["twoPointMade"],
            three_pointers=stats["threePointMade"],
            fouls=stats["fouls"],
            counted_as_played=counted_as_played,
        )


def publish_kog_player_feed(totals: Dict[str, PlayerTotals]) -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = [player.as_row() for player in totals.values() if player.games_played]
    rows.sort(key=lambda r: (-r["totalPoints"], r["name"]))

    feed_path = SITE_DATA_DIR / "kog_players.json"
    feed_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def publish_metadata(game_ids: Iterable[int], totals: Dict[str, PlayerTotals]) -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    metadata = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "gamesProcessed": sorted(set(game_ids)),
        "playersTracked": sum(1 for player in totals.values() if player.games_played),
    }
    meta_path = SITE_DATA_DIR / "last_updated.json"
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def main() -> None:
    if not RAW_DIR.exists():
        raise SystemExit("No raw data found. Add EMP feeds to data/raw/ first.")

    kog_totals: Dict[str, PlayerTotals] = {}
    processed_games: list[int] = []

    for game_id, game in load_raw_games():
        teams = build_team_structures(game)
        write_game_summary(game_id, game, teams)
        aggregate_kog_players(game_id, teams, kog_totals)
        processed_games.append(game_id)

    publish_kog_player_feed(kog_totals)
    publish_metadata(processed_games, kog_totals)


if __name__ == "__main__":
    main()
