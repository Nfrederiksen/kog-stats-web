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

import csv
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Tuple
from zoneinfo import ZoneInfo

# Team id for Kungsholmen OG in Profixio
KOG_TEAM_ID = 1403069

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
SITE_DATA_DIR = ROOT / "docs" / "data"
SCHEDULE_PATH = ROOT / "data" / "schedule.csv"
LINKS_PATH = ROOT / "data" / "links.txt"

# Static season details. Update SEASON_START_YEAR when rolling into a new campaign.
SEASON_START_MONTH = 9  # September
SEASON_START_YEAR = 2025
SCHEDULE_TZ = ZoneInfo("Europe/Stockholm")


@dataclass
class PlayerTotals:
    name: str
    numbers: set[str] = field(default_factory=set)
    last_number: str = ""
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
            self.last_number = number

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
        if self.last_number:
            number = self.last_number
        else:
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


def parse_schedule_datetime(raw_value: str) -> datetime | None:
    raw_value = " ".join((raw_value or "").strip().split())
    if not raw_value:
        return None

    try:
        parsed = datetime.strptime(raw_value, "%a %d.%b %H:%M")
    except ValueError:
        return None

    year = SEASON_START_YEAR
    if parsed.month < SEASON_START_MONTH:
        year += 1

    return parsed.replace(year=year, tzinfo=SCHEDULE_TZ)


def to_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def load_schedule() -> Dict[int, dict]:
    if not SCHEDULE_PATH.exists():
        return {}

    schedule: Dict[int, dict] = {}
    with SCHEDULE_PATH.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw_id = (row.get("matchId") or "").strip()
            if not raw_id:
                continue
            try:
                match_id = int(raw_id)
            except ValueError:
                continue

            home_or_away = (row.get("homeOrAway") or "").strip().lower()
            raw_date = " ".join((row.get("date") or "").strip().split())
            tipoff = parse_schedule_datetime(raw_date)
            home_score = to_int(row.get("homeScore"))
            away_score = to_int(row.get("awayScore"))
            status = "played" if home_score is not None and away_score is not None else "upcoming"

            entry = {
                "matchId": match_id,
                "homeOrAway": "home" if home_or_away == "home" else "away",
                "opponent": (row.get("opponents") or "").strip(),
                "location": (row.get("location") or "").strip(),
                "dateLabel": raw_date,
                "tipoff": tipoff,
                "homeScore": home_score,
                "awayScore": away_score,
                "status": status,
                "kogScore": None,
                "opponentScore": None,
                "pointDiff": None,
                "result": None,
                "hasStats": False,
            }

            if home_score is not None and away_score is not None:
                if entry["homeOrAway"] == "home":
                    entry["kogScore"] = home_score
                    entry["opponentScore"] = away_score
                else:
                    entry["kogScore"] = away_score
                    entry["opponentScore"] = home_score
                entry["pointDiff"] = entry["kogScore"] - entry["opponentScore"]
                if entry["pointDiff"] > 0:
                    entry["result"] = "win"
                elif entry["pointDiff"] < 0:
                    entry["result"] = "loss"
                else:
                    entry["result"] = "draw"

            schedule[match_id] = entry

    return schedule


def apply_metrics_to_schedule(schedule: Dict[int, dict], metrics: Dict[str, object]) -> None:
    match_id = metrics.get("gameId")
    if match_id is None:
        return

    entry = schedule.get(match_id)
    if not entry:
        return

    kog_points = metrics.get("kogPoints")
    opponent_points = metrics.get("opponentPoints")
    if kog_points is None or opponent_points is None:
        return

    entry["kogScore"] = int(kog_points)
    entry["opponentScore"] = int(opponent_points)
    entry["pointDiff"] = entry["kogScore"] - entry["opponentScore"]
    if entry["pointDiff"] > 0:
        entry["result"] = "win"
    elif entry["pointDiff"] < 0:
        entry["result"] = "loss"
    else:
        entry["result"] = "draw"
    entry["status"] = "played"
    entry["hasStats"] = True

    if entry["homeOrAway"] == "home":
        entry["homeScore"] = entry["kogScore"]
        entry["awayScore"] = entry["opponentScore"]
    else:
        entry["homeScore"] = entry["opponentScore"]
        entry["awayScore"] = entry["kogScore"]

    if "opponentTeamId" in metrics:
        entry["opponentTeamId"] = metrics["opponentTeamId"]

    opponent_name = (metrics.get("opponent") or "").strip()
    if opponent_name and not entry.get("opponent"):
        entry["opponent"] = opponent_name


def publish_schedule(schedule: Dict[int, dict]) -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    def sort_key(item: dict) -> datetime:
        tipoff = item.get("tipoff")
        if isinstance(tipoff, datetime):
            return tipoff
        return datetime.max.replace(tzinfo=timezone.utc)

    games = sorted(schedule.values(), key=sort_key)
    payload = []
    for game in games:
        serialized = dict(game)
        tipoff = serialized.get("tipoff")
        if isinstance(tipoff, datetime):
            serialized["tipoff"] = tipoff.isoformat()
        else:
            serialized["tipoff"] = None if not tipoff else str(tipoff)
        payload.append(serialized)

    schedule_path = SITE_DATA_DIR / "kog_schedule.json"
    schedule_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_links() -> list[dict]:
    if not LINKS_PATH.exists():
        return []

    links: list[dict] = []
    with LINKS_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [part.strip() for part in line.split(",", 1)]
            if len(parts) != 2:
                continue
            label, url = parts
            links.append({"label": label, "url": url})
    return links


def publish_links(links: list[dict]) -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    links_path = SITE_DATA_DIR / "kog_links.json"
    links_path.write_text(json.dumps(links, indent=2), encoding="utf-8")


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


def compute_game_metrics(teams: Dict[int, dict], game_id: int | None = None) -> Dict[str, object] | None:
    kog_team = teams.get(KOG_TEAM_ID)
    if not kog_team:
        return None

    opponents = [team for team_id, team in teams.items() if team_id != KOG_TEAM_ID]
    if not opponents:
        return None

    opponent = opponents[0]

    def team_points(team: dict) -> int:
        return sum(player["stats"]["points"] for player in team["roster"] if player.get("type") == "player")

    kog_points = team_points(kog_team)
    opponent_points = team_points(opponent)

    return {
        "gameId": game_id,
        "opponent": (opponent.get("teamName") or "Opponent").strip() or "Opponent",
        "opponentTeamId": opponent.get("teamId"),
        "kogPoints": kog_points,
        "opponentPoints": opponent_points,
        "pointDiff": kog_points - opponent_points,
    }


def publish_kog_player_feed(totals: Dict[str, PlayerTotals]) -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = [player.as_row() for player in totals.values() if player.games_played]
    rows.sort(key=lambda r: r["name"].lower())

    feed_path = SITE_DATA_DIR / "kog_players.json"
    feed_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def publish_metadata(
    game_ids: Iterable[int],
    totals: Dict[str, PlayerTotals],
    game_metrics: Iterable[Dict[str, object]],
) -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    metadata = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "gamesProcessed": sorted(set(game_ids)),
        "playersTracked": sum(1 for player in totals.values() if player.games_played),
        "teamRecords": None,
    }
    metrics = list(game_metrics)
    if metrics:
        highest_score = max(metrics, key=lambda m: m["kogPoints"])
        positive = [m for m in metrics if m["pointDiff"] > 0]
        negative = [m for m in metrics if m["pointDiff"] < 0]

        metadata["teamRecords"] = {
            "highestScore": highest_score,
            "biggestWin": max(positive, key=lambda m: m["pointDiff"]) if positive else None,
            "toughestLoss": min(negative, key=lambda m: m["pointDiff"]) if negative else None,
        }
    meta_path = SITE_DATA_DIR / "last_updated.json"
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def main() -> None:
    if not RAW_DIR.exists():
        raise SystemExit("No raw data found. Add EMP feeds to data/raw/ first.")

    schedule = load_schedule()
    links = load_links()

    kog_totals: Dict[str, PlayerTotals] = {}
    processed_games: list[int] = []
    game_metrics: list[Dict[str, object]] = []

    for game_id, game in load_raw_games():
        teams = build_team_structures(game)
        write_game_summary(game_id, game, teams)
        aggregate_kog_players(game_id, teams, kog_totals)
        processed_games.append(game_id)
        metrics = compute_game_metrics(teams, game_id=game_id)
        if metrics:
            game_metrics.append(metrics)
            apply_metrics_to_schedule(schedule, metrics)

    publish_kog_player_feed(kog_totals)
    publish_metadata(processed_games, kog_totals, game_metrics)
    publish_schedule(schedule)
    publish_links(links)


if __name__ == "__main__":
    main()
