# Scraping Service

This service scrapes energy market data using `ve_energy_scrapers` based on `config.json`.

## Binaries

This project includes three binaries:
- `scraping_service`: Continuous scraping service that runs scrapers on schedule
- `backfill`: One-time tool for backfilling historical data
- `verify-uploads`: Verification tool to check if local files are uploaded to S3

## Setup

1.  Ensure you have Rust installed.
2.  The project depends on `ve_energy_scrapers` from GitHub.

## Configuration

The service reads `config.json` in the current directory.

## Running

### Scraping Service

```bash
cargo run --bin scraping_service
```

### Backfill Tool

```bash
cargo run --bin backfill -- <scraper_name> <start_date> <end_date>
```

Example:
```bash
cargo run --bin backfill -- apg_at_cz_exchange 2025-01-01 2025-01-31
```

Parameters:
- `scraper_name`: Name of the scraper from config.json
- `start_date`: Start date in YYYY-MM-DD format
- `end_date`: End date in YYYY-MM-DD format

**Note:** The backfill tool preserves `scraped_at` as null to distinguish backfilled data from real-time scraped data. Real-time scraped data has a `scraped_at` timestamp indicating when it was collected.

### Verify Uploads Tool

```bash
cargo run --bin verify-uploads -- <scraper_name|all> <start_date> <end_date>
```

Examples:
```bash
# Check one scraper
cargo run --bin verify-uploads -- apg_imb_15min 2025-01-01 2026-01-05

# Check all scrapers
cargo run --bin verify-uploads -- all 2025-01-01 2026-01-05
```

This tool checks S3 for missing dates in a date range. It will:
- Check each day in the specified date range
- Verify if data files exist in S3 for each day
- Show a progress bar during verification
- Report which dates are missing from S3
- Work for a single scraper or all scrapers

Useful after running backfills to ensure all dates have been uploaded successfully.

## Output

Data is saved to the `data/` directory in CSV format.
Files are named after the scraper name (e.g., `data/apg_imb_15min.csv`).
Only new data points are appended to the files.
