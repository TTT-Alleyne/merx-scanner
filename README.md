# Merx Scanner — Alleyne Group

Scans Merx for opportunities, scores them against your criteria, saves results.

## First time setup

1. Copy the env template and fill in your password:
   ```
   copy .env.template .env
   ```
   Then open `.env` in Notepad and replace `your_password_here` with your real Merx password.

2. Build and run:
   ```
   docker compose up --build
   ```

## Every scan after that
```
docker compose up
```

## Results
Results are saved in the `results` folder:
- `merx_scan_*.json` — full data for each opportunity (used by next agents)
- `merx_summary_*.txt` — human-readable report you can read right away

## Settings (in .env file)
- `MIN_DAYS_TO_BID` — ignore opportunities closing sooner than this many days (default: 21)
- `MAX_RESULTS` — how many opportunities to process per scan (default: 50)
- `COMPANY` — which company's criteria to use (default: Alleyne Inc.)

## Scoring
Each opportunity is scored 0-100:
- 70-100: STRONG FIT — pursue
- 40-69:  POSSIBLE FIT — review manually
- 0-39:   WEAK FIT — skip

## Important
- Never share your `.env` file — it contains your password
- The `results` folder is on your computer, safe even if Docker is removed
- Run this daily or weekly to catch new opportunities
