# KOG Stats Web — Agent Handoff

## Purpose and publishing

This repository is the static Kungsholmen OG basketball stats site. The public
site is GitHub Pages, served from `docs/`; the site fetches JSON from
`docs/data/`. Work on the current branch, verify the rendered data, then commit
and push to `main` when the user asks for a live update.

The `Update Stats` GitHub Actions workflow runs on every push to `main`. It
rebuilds generated data and can create a follow-up `chore: refresh stats`
commit. A Pages deployment then publishes the site. Before pushing after a
long-running change, fetch/rebase on `origin/main` to account for that bot
commit.

## 2026–27 current context

Treat the source files below as authoritative, rather than any prose in this
document. At the time this handoff was written, the season has two played games
and a 1–1 record:

| Date | Match ID | Result |
| --- | ---: | --- |
| 19 Sep 2026 | `32726810` | KOG 74–67 Skuru Basket |
| 10 Oct 2026 | `32726822` | Tumba Basket 68–55 KOG |

The next listed fixture is away to Odysseas BF (`32726807`) on 25 Oct 2026.
Always read `data/schedule_26-27.csv` again before reporting an upcoming game;
it is the live schedule source.

- Season key: `26-27`; KOG Profixio team ID: `1403069`.
- `scripts/build_stats.py` has the ordered `SEASONS` configuration. Add a new
  season there as well as its schedule when the time comes.
- This season currently has no `data/sources_26-27.txt`: the two game updates
  came from official PDF protocols, not Profixio EMP feeds.
- Player jersey numbers can change between protocols. The current published
  number must be the latest official protocol number, not a guessed historic
  number.

## Data ownership

| Need | Edit this source | Generated / displayed result |
| --- | --- | --- |
| Fixture, venue, final score | `data/schedule_26-27.csv` | `docs/data/26-27/kog_schedule.json` |
| Which PDF-protocol games have player stats | `data/manual_stats_26-27.json` | season metadata, schedule `hasStats` |
| Current cumulative player totals from PDFs | `data/overrides_26-27.json` | `docs/data/26-27/kog_players.json` |
| Protocol play-by-play without EMP | `docs/data/26-27/playbyplay/game_<matchId>.json` | Game-card Play-by-play panel |
| KOG post-game coach debrief | `docs/data/game_reviews.json` | Second game-card info button |
| Opponent pre-game scout | `docs/data/opponent_scouts.json` | First game-card info button |
| Footer links | `data/links.txt` | `docs/data/kog_links.json` |
| UI / mobile behaviour | `docs/index.html` | GitHub Pages UI |

Do not hand-edit generated `docs/data/<season>/kog_players.json`,
`kog_schedule.json`, `last_updated.json`, `docs/data/seasons.json`, or
`docs/data/kog_links.json`. Rebuild them from the source files. The exception
is a deliberately hand-authored manual protocol ledger in the season's
`playbyplay/` directory.

## Adding a completed 2026–27 game from a PDF protocol

1. Find the correct row and match ID in `data/schedule_26-27.csv`. Enter the
   official home and away final scores; do not reverse them for an away KOG
   game.
2. Read the protocol's roster totals and action ledger. Preserve leading-zero
   jersey numbers as strings (for example, `"00"`). Reconcile the protocol:
   player points must total the KOG score and player fouls must total the KOG
   team-foul total.
3. Add the match ID to `data/manual_stats_26-27.json` and update every affected
   cumulative row in `data/overrides_26-27.json`. Add a row for any new player.
   `fieldGoalsMade` includes both made 2s and made 3s. Validate each row with:
   `totalPoints = freeThrowsMade + 2 × (fieldGoalsMade - threePointsMade) + 3 × threePointsMade`.
   Recalculate `pointsPerGame` from `totalPoints / gamesPlayed`.
4. Add `docs/data/26-27/playbyplay/game_<matchId>.json` when the PDF records
   the action ledger. Use the existing Skuru/Tumba files as the exact compact
   schema. Event forms are `[period, score, playerNumber, "KOG" | "Opponent",
   action]` plus `["start", period]`. Supported protocol actions are `2p`,
   `3p`, `Mål` (made free throw), `P`, `T`, `DI`, and `Timeout`.
   Map the protocol's home team to `Opponent` or `KOG` according to the actual
   fixture—not merely by `Lag A`/`Lag B`. Include rosters and the note that PDF
   protocols record event order and score but may not record game clocks.
5. Add an evidence-led, constructive KOG review to `docs/data/game_reviews.json`.
   Match both `seasonKey: "26-27"` and the numeric `matchId`. Use only facts
   the protocol supports (made baskets, free throws, fouls, scores and quarter
   splits). State the data limits: protocols do not supply misses, rebounds,
   turnovers or minutes.
6. Run `python3 scripts/build_stats.py`. This marks manual games as having
   stats and regenerates all season feeds.

For a PDF with no action ledger, still add the score and player totals, but do
not invent play-by-play or possession metrics.

## Coach reports and scouting

Post-game reviews should be upbeat without hiding the result: identify a
concrete strength, quote the supporting stat, and turn it into a practical
next-game takeaway. The Tumba review is a useful model: it credits the 32–32
second half and 16–14 fourth quarter while accurately recording the 55–68 loss.

For a pre-game scout, add one object to `docs/data/opponent_scouts.json` with
the exact opponent text used in the schedule. State the sample size and dates,
flag roster/jersey uncertainty, and avoid claims beyond the supplied reports.
The Tumba scout demonstrates a three-game sample; the Skuru scout demonstrates
a one-game sample. Keep coach advice specific and actionable.

## Profixio EMP-feed workflow

When a valid EMP URL is available, append it to the appropriate
`data/sources_XX-YY.txt` file and run:

```bash
python3 scripts/update_stats.py
```

It downloads uncached feeds to `data/raw/`, writes summaries to
`data/processed/`, and runs the builder. It deliberately skips an existing
`data/raw/game_<id>.json`; do not delete or replace historical raw feeds merely
because a signed URL has expired. `python3 scripts/build_stats.py` is the
offline deterministic rebuild command when all needed raw data is already
present.

## Verification and safe handoff

Before committing a content update, run a fresh rebuild and inspect the output:

```bash
python3 scripts/build_stats.py
jq -e . docs/data/26-27/kog_schedule.json >/dev/null
jq -e . docs/data/26-27/kog_players.json >/dev/null
jq -e . docs/data/game_reviews.json >/dev/null
git diff --check
```

For a new manual ledger, also programmatically total its scoring and fouls and
compare them to the PDF final score and team-foul totals. Confirm the final
event shows the official final score and all four periods are present.

Keep changes narrowly scoped. This repo can have user-owned untracked raw or
processed files; never delete, reset, or stage them automatically. If the main
checkout is dirty, use a clean worktree for the task. In the final handoff,
state the result, source used, verification performed, commit hash, and whether
the Pages deployment completed.
