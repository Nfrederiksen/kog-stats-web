#!/usr/bin/env python3
"""
Utility for transforming raw Profixio EMP feeds into site-ready JSON.

Usage:
    python scripts/build_stats.py

The script discovers seasons from data/sources_XX-YY.txt and
data/schedule_XX-YY.csv files.  Each season's output lands in
docs/data/<season>/ and a manifest is written to docs/data/seasons.json.
"""
from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo

# Default team id for Kungsholmen OG in Profixio (25-26 onward)
KOG_TEAM_ID = 1403069

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
SITE_DATA_DIR = ROOT / "docs" / "data"
LINKS_PATH = ROOT / "data" / "links.txt"

SCHEDULE_TZ = ZoneInfo("Europe/Stockholm")

# Season month where a new campaign starts (September).
SEASON_START_MONTH = 9

# Ordered list of seasons – latest first.  Each entry maps a season key
# (e.g. "25-26") to its start year and a display label.
SEASONS: list[dict] = [
    {"key": "25-26", "startYear": 2025, "label": "2025-26", "teamId": 1403069},
    {"key": "24-25", "startYear": 2024, "label": "2024-25", "teamId": 1264914},
    # Add more seasons here as needed, newest first.
]

EMP_PATTERN = re.compile(r"/emp/(\d+)/")


# ── Season discovery ────────────────────────────────────────────────────────

def discover_seasons() -> list[dict]:
    """Return list of season configs that have at least a schedule file."""
    found: list[dict] = []
    for season in SEASONS:
        key = season["key"]
        schedule_path = ROOT / "data" / f"schedule_{key}.csv"
        sources_path = ROOT / "data" / f"sources_{key}.txt"
        if not schedule_path.exists():
            continue
        found.append({
            **season,
            "schedulePath": schedule_path,
            "sourcesPath": sources_path if sources_path.exists() else None,
        })
    return found


def game_ids_for_season(season_cfg: dict) -> set[int]:
    """Extract the set of game IDs listed in a season's sources file."""
    sources_path = season_cfg.get("sourcesPath")
    if not sources_path or not sources_path.exists():
        return set()

    ids: set[int] = set()
    with sources_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            cleaned = line.strip()
            if not cleaned or cleaned.startswith("#"):
                continue
            match = EMP_PATTERN.search(cleaned)
            if match:
                ids.add(int(match.group(1)))
    return ids


# ── Player totals ───────────────────────────────────────────────────────────

@dataclass
class PlayerTotals:
    name: str
    numbers: set[str] = field(default_factory=set)
    last_number: str = ""
    last_number_seen_ts: float = -1.0
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
        tipoff_ts: float | None,
    ) -> None:
        number = (number or "").strip()
        if number:
            self.numbers.add(number)
            ts = tipoff_ts if tipoff_ts is not None else -1.0
            if ts >= self.last_number_seen_ts:
                self.last_number_seen_ts = ts
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


# ── Raw game loading ────────────────────────────────────────────────────────

def load_raw_games(allowed_ids: set[int] | None = None) -> Iterable[Tuple[int, dict]]:
    pattern = re.compile(r"game_(\d+)\.json$")
    for raw_file in sorted(RAW_DIR.glob("game_*.json")):
        match = pattern.search(raw_file.name)
        if not match:
            continue

        game_id = int(match.group(1))
        if allowed_ids is not None and game_id not in allowed_ids:
            continue

        with raw_file.open("r", encoding="utf-8") as handle:
            yield game_id, json.load(handle)


# ── Schedule parsing ────────────────────────────────────────────────────────

def parse_schedule_datetime(raw_value: str, start_year: int) -> datetime | None:
    raw_value = " ".join((raw_value or "").strip().split())
    if not raw_value:
        return None

    try:
        parsed = datetime.strptime(raw_value, "%a %d.%b %H:%M")
    except ValueError:
        return None

    year = start_year
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


def load_schedule(schedule_path: Path, start_year: int) -> Dict[int, dict]:
    if not schedule_path.exists():
        return {}

    schedule: Dict[int, dict] = {}
    with schedule_path.open("r", encoding="utf-8") as handle:
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
            tipoff = parse_schedule_datetime(raw_date, start_year)
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


# ── Play-by-play ────────────────────────────────────────────────────────────

def format_clock(seconds: int | None) -> str | None:
    if seconds is None or seconds < 0:
        return None
    mins, secs = divmod(int(seconds), 60)
    return f"{mins:02d}:{secs:02d}"


def build_play_by_play(
    events: list[dict],
    schedule_entry: dict,
    opponent_team_id: int | None,
    kog_team_id: int = KOG_TEAM_ID,
) -> list[dict]:
    kog_home = (schedule_entry.get("homeOrAway") or "").lower() == "home"
    opponent_name = schedule_entry.get("opponent") or "Opponent"

    def resolve_side(team_id: int | None, team_name: str | None) -> str | None:
        if team_id is None:
            return None
        if team_id == kog_team_id:
            return "KOG"
        if opponent_team_id and team_id == opponent_team_id:
            return "Opponent"
        return team_name or "Opponent"

    def extract_score(event: dict) -> tuple[int | None, int | None]:
        score = event.get("currentScore") or {}
        home = score.get("home")
        away = score.get("away")
        if not isinstance(home, int) or not isinstance(away, int):
            return None, None
        return (home, away)

    def map_score_to_kog(raw_score: tuple[int | None, int | None]) -> tuple[int | None, int | None]:
        home, away = raw_score
        if home is None or away is None:
            return None, None
        if kog_home:
            return home, away
        return away, home

    def score_line(event: dict) -> str | None:
        raw_home, raw_away = extract_score(event)
        if raw_home is None or raw_away is None:
            return None
        kog_score, opp_score = map_score_to_kog((raw_home, raw_away))
        if not isinstance(kog_score, int) or not isinstance(opp_score, int):
            return None
        return f"{kog_score}-{opp_score}"

    timeline: list[dict] = []

    sort_key = lambda e: (
        e.get("period") or 0,
        e.get("secondsSinceStartOfPeriod") if e.get("secondsSinceStartOfPeriod") is not None else 0,
        e.get("sortOrder") or 0,
        e.get("id") or 0,
    )

    period_state: dict[int, dict[str, tuple[int, int] | None]] = {}
    summarized_periods: set[int] = set()
    previous_end: tuple[int, int] | None = None

    def summarize_period(period: int, prev_end: tuple[int, int] | None) -> tuple[str | None, tuple[int, int] | None]:
        state = period_state.get(period, {})
        start_score = state.get("start") or prev_end or (0, 0)
        end_score = state.get("end") or start_score
        detail = None
        if start_score and end_score and None not in start_score and None not in end_score:
            start_kog, start_opp = start_score
            end_kog, end_opp = end_score
            quarter_kog = end_kog - start_kog
            quarter_opp = end_opp - start_opp
            lead_diff = end_kog - end_opp
            lead_label = f"+{lead_diff}" if lead_diff > 0 else f"{lead_diff}"

            if quarter_kog > quarter_opp:
                detail = f"OG wins QTR {quarter_kog}-{quarter_opp}, lead {lead_label}"
            elif quarter_opp > quarter_kog:
                detail = f"{opponent_name} wins QTR {quarter_opp}-{quarter_kog}, lead {lead_label}"
            else:
                detail = f"QTR tied {quarter_kog}-{quarter_opp}, lead {lead_label}"

        next_prev_end = end_score if end_score != (None, None) else prev_end
        return detail, next_prev_end

    for event in sorted(events or [], key=sort_key):
        etype = event.get("eventTypeId")
        period = event.get("period") or 0
        clock = format_clock(event.get("secondsSinceStartOfPeriod"))
        side = resolve_side(event.get("teamId"), event.get("teamName"))

        raw_score = extract_score(event)
        if raw_score != (None, None):
            kog_score, opp_score = map_score_to_kog(raw_score)
            if kog_score is not None and opp_score is not None:
                state = period_state.setdefault(period, {"start": None, "end": None})
                if state["start"] is None:
                    state["start"] = (kog_score, opp_score)
                state["end"] = (kog_score, opp_score)

        base = {
            "period": period,
            "clock": clock,
            "teamId": event.get("teamId"),
            "teamName": event.get("teamName") or opponent_name,
            "player": (event.get("person") or {}).get("name", "").strip(),
            "playerNumber": (event.get("person") or {}).get("number", ""),
            "score": score_line(event),
            "rawType": etype,
            "side": side,
        }

        if etype == 97:  # start period
            timeline.append({**base, "kind": "period", "label": f"Start Period {period or ''}", "emoji": "⏱️"})
            continue
        if etype == 98:  # period break
            detail, previous_end = summarize_period(period, previous_end)
            summarized_periods.add(period)
            timeline.append({**base, "kind": "period", "label": f"End Period {period or ''}", "emoji": "🔔", "detail": detail})
            continue
        if etype == 99:  # start period (later)
            timeline.append({**base, "kind": "period", "label": f"Start Period {period or ''}", "emoji": "⏱️"})
            continue
        if etype == 100:  # full time
            final_period = period or 4
            if final_period not in summarized_periods:
                detail, previous_end = summarize_period(final_period, previous_end)
                summarized_periods.add(final_period)
                timeline.append({
                    **base,
                    "kind": "period",
                    "label": f"End Period {final_period or ''}",
                    "emoji": "🔔",
                    "detail": detail,
                })
            score = event.get("currentScore") or {}
            winner_label = "Final Buzzer"
            winner_side = None
            home = score.get("home")
            away = score.get("away")
            if isinstance(home, int) and isinstance(away, int):
                kog_score = home if kog_home else away
                opp_score = away if kog_home else home
                if kog_score > opp_score:
                    winner_label = "Final Buzzer — Kungsholmen OG"
                    winner_side = "KOG"
                elif opp_score > kog_score:
                    winner_label = f"Final Buzzer — {opponent_name}"
                    winner_side = "Opponent"
                else:
                    winner_label = "Final Buzzer — Draw"
            timeline.append({**base, "kind": "period", "label": winner_label, "emoji": "🏁", "side": winner_side})
            continue
        if etype == 108:  # timeout
            timeline.append({**base, "kind": "timeout", "label": "Timeout", "emoji": "🛑"})
            continue
        if etype in (109, 111):  # fouls
            label = "Personal Foul" if etype == 109 else "Unsportsmanlike Foul"
            emoji = "🥊"
            timeline.append({**base, "kind": "foul", "label": label, "emoji": emoji})
            continue
        if etype in (103, 104, 106):  # scoring
            if etype == 103:
                label, emoji = "3PT Made", "🎯"
            elif etype == 104:
                label, emoji = "2PT Made", "🏀"
            else:
                label, emoji = "FT Made", "🎫"

            timeline.append({
                **base,
                "kind": "score",
                "label": label,
                "emoji": emoji,
            })
            continue

    return timeline


def publish_play_by_play(game_id: int, game: dict, schedule_entry: dict, opponent_team_id: int | None, play_by_play_dir: Path, kog_team_id: int = KOG_TEAM_ID) -> None:
    events = game.get("events")
    if not events:
        return

    timeline = build_play_by_play(events, schedule_entry, opponent_team_id, kog_team_id=kog_team_id)
    if not timeline:
        return

    play_by_play_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "matchId": game_id,
        "opponent": schedule_entry.get("opponent"),
        "homeOrAway": schedule_entry.get("homeOrAway"),
        "location": schedule_entry.get("location"),
        "dateLabel": schedule_entry.get("dateLabel"),
        "timeline": timeline,
    }
    target = play_by_play_dir / f"game_{game_id}.json"
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ── Metrics helpers ─────────────────────────────────────────────────────────

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


def publish_schedule(schedule: Dict[int, dict], site_dir: Path) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)

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

    schedule_path = site_dir / "kog_schedule.json"
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


def aggregate_kog_players(
    game_id: int,
    teams: Dict[int, dict],
    totals: Dict[str, PlayerTotals],
    tipoff_ts: float | None = None,
    kog_team_id: int = KOG_TEAM_ID,
) -> None:
    kog_team = teams.get(kog_team_id)
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
            tipoff_ts=tipoff_ts,
        )


def compute_game_metrics(teams: Dict[int, dict], game_id: int | None = None, kog_team_id: int = KOG_TEAM_ID) -> Dict[str, object] | None:
    kog_team = teams.get(kog_team_id)
    if not kog_team:
        return None

    opponents = [team for team_id, team in teams.items() if team_id != kog_team_id]
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


def publish_kog_player_feed(totals: Dict[str, PlayerTotals], site_dir: Path) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)
    rows = [player.as_row() for player in totals.values() if player.games_played]
    rows.sort(key=lambda r: r["name"].lower())

    feed_path = site_dir / "kog_players.json"
    feed_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def update_player_records(
    teams: Dict[int, dict],
    schedule: Dict[int, dict],
    player_records: Dict[str, dict],
    game_id: int | None = None,
    kog_team_id: int = KOG_TEAM_ID,
) -> None:
    kog_team = teams.get(kog_team_id)
    if not kog_team:
        return

    opponents = [team for team_id, team in teams.items() if team_id != kog_team_id]
    opponent = opponents[0] if opponents else {}
    opponent_name = (opponent.get("teamName") or "Opponent").strip() or "Opponent"
    schedule_row = schedule.get(game_id or 0, {})

    tipoff = schedule_row.get("tipoff")
    tipoff_value = tipoff.isoformat() if hasattr(tipoff, "isoformat") else tipoff

    for player in kog_team.get("roster", []):
        if player.get("type") != "player":
            continue

        threes = player.get("stats", {}).get("threePointMade", 0)
        points = player.get("stats", {}).get("points", 0) or 0
        current_three = player_records.get("mostThreesInGame")
        if not current_three or threes > current_three["threePointers"]:
            player_records["mostThreesInGame"] = {
                "gameId": game_id,
                "player": player.get("name") or "",
                "threePointers": threes,
                "opponent": opponent_name,
                "opponentTeamId": opponent.get("teamId"),
                "dateLabel": schedule_row.get("dateLabel"),
                "tipoff": tipoff_value,
            }

        points_current = player_records.get("mostPointsInGame")
        if not points_current or points > points_current["points"]:
            player_records["mostPointsInGame"] = {
                "gameId": game_id,
                "player": player.get("name") or "",
                "points": points,
                "opponent": opponent_name,
                "opponentTeamId": opponent.get("teamId"),
                "dateLabel": schedule_row.get("dateLabel"),
                "tipoff": tipoff_value,
            }


def publish_metadata(
    game_ids: Iterable[int],
    totals: Dict[str, PlayerTotals],
    game_metrics: Iterable[Dict[str, object]],
    player_records: Dict[str, dict] | None,
    site_dir: Path,
) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "gamesProcessed": sorted(set(game_ids)),
        "playersTracked": sum(1 for player in totals.values() if player.games_played),
        "teamRecords": None,
        "playerRecords": player_records or None,
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
    meta_path = site_dir / "last_updated.json"
    meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


# ── Per-season build ────────────────────────────────────────────────────────

def build_season(season_cfg: dict) -> dict | None:
    """Process one season and publish its data.  Returns season manifest entry or None."""
    key = season_cfg["key"]
    label = season_cfg["label"]
    start_year = season_cfg["startYear"]
    schedule_path = season_cfg["schedulePath"]
    kog_team_id = season_cfg.get("teamId", KOG_TEAM_ID)

    season_site_dir = SITE_DATA_DIR / key
    play_by_play_dir = season_site_dir / "playbyplay"

    schedule = load_schedule(schedule_path, start_year)
    if not schedule:
        return None

    allowed_ids = game_ids_for_season(season_cfg)

    kog_totals: Dict[str, PlayerTotals] = {}
    processed_games: list[int] = []
    game_metrics: list[Dict[str, object]] = []
    player_records: dict[str, dict] = {}
    has_stats = False

    for game_id, game in load_raw_games(allowed_ids or None):
        # Only process games that are in this season's schedule or source list
        if allowed_ids and game_id not in allowed_ids:
            continue
        has_stats = True

        teams = build_team_structures(game)
        write_game_summary(game_id, game, teams)

        tipoff = None
        schedule_entry = schedule.get(game_id)
        if schedule_entry:
            tipoff_val = schedule_entry.get("tipoff")
            if isinstance(tipoff_val, datetime):
                tipoff = tipoff_val.timestamp()
            elif isinstance(tipoff_val, str):
                try:
                    tip_dt = datetime.fromisoformat(tipoff_val)
                    tipoff = tip_dt.timestamp()
                except ValueError:
                    tipoff = None

        aggregate_kog_players(game_id, teams, kog_totals, tipoff_ts=tipoff, kog_team_id=kog_team_id)
        processed_games.append(game_id)
        metrics = compute_game_metrics(teams, game_id=game_id, kog_team_id=kog_team_id)
        update_player_records(teams, schedule, player_records, game_id=game_id, kog_team_id=kog_team_id)
        if metrics:
            game_metrics.append(metrics)
            apply_metrics_to_schedule(schedule, metrics)

        opponent_team_id = None
        for team_id in teams:
            if team_id != kog_team_id:
                opponent_team_id = team_id
                break
        if schedule_entry:
            publish_play_by_play(game_id, game, schedule_entry, opponent_team_id, play_by_play_dir, kog_team_id=kog_team_id)

    publish_kog_player_feed(kog_totals, season_site_dir)
    publish_metadata(processed_games, kog_totals, game_metrics, player_records, season_site_dir)
    publish_schedule(schedule, season_site_dir)

    played = [g for g in schedule.values() if g.get("status") == "played"]
    wins = sum(1 for g in played if g.get("result") == "win")
    losses = sum(1 for g in played if g.get("result") == "loss")

    return {
        "key": key,
        "label": label,
        "gamesPlayed": len(played),
        "gamesScheduled": len(schedule),
        "hasStats": has_stats,
        "record": f"{wins}W-{losses}L",
    }


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    seasons = discover_seasons()
    if not seasons:
        raise SystemExit("No seasons found. Add schedule_XX-YY.csv files to data/.")

    links = load_links()
    manifest: list[dict] = []

    for season_cfg in seasons:
        print(f"Building season {season_cfg['label']}…")
        entry = build_season(season_cfg)
        if entry:
            manifest.append(entry)
            print(f"  → {entry['gamesPlayed']} played, stats={'yes' if entry['hasStats'] else 'no'}")

    # Write seasons manifest
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = SITE_DATA_DIR / "seasons.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Seasons manifest: {len(manifest)} season(s)")

    publish_links(links)


if __name__ == "__main__":
    main()
