# KOG Stats Web

Static site plus scripts for tracking Kungsholmen OG game data from Profixio EMP feeds.

## Repository Layout

- `docs/` – publishable site (GitHub Pages compatible). Fetches its data from `docs/data/`.
- `data/raw/` – raw EMP responses (`game_<id>.json`).
- `data/processed/` – auto-generated summaries & pretty dumps.
- `data/sources.txt` – list of EMP URLs to fetch.
- `scripts/` – automation helpers (`update_stats.py`, `build_stats.py`).
- `NOTES.md` – extra references.

## Updating Stats Manually

1. Append the new game feed URL to `data/sources.txt` (one per line).
2. Run the fetch + build step:

   ```bash
   python3 scripts/update_stats.py
   ```

   This will:
   - download each URL to `data/raw/game_<id>.json` (skipping URLs whose match is already cached)
   - regenerate summaries & pretty JSON in `data/processed/`
   - update `docs/data/kog_players.json`
   - write metadata (`docs/data/last_updated.json`)
3. Commit the updated files and push. GitHub Pages (docs/) will automatically display the new stats and “Last Updated” badge after deployment.

## Suggested GitHub Actions Workflow

You can automate the above by adding `.github/workflows/update-stats.yml`:

```yaml
name: Update Stats

on:
  push:
    branches: ["main"]

permissions:
  contents: write

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - run: python3 scripts/update_stats.py

      - name: Commit changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add data/raw data/processed docs/data
          git diff --cached --quiet || git commit -m "chore: refresh stats"
          git push
```

If you use GitHub Pages with the `docs/` folder, this commit is enough to retrigger the site build.
