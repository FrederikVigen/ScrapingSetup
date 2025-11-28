# Scraping Service

This service scrapes energy market data using `ve_energy_scrapers` based on `config.json`.

## Setup

1.  Ensure you have Rust installed.
2.  The project depends on `ve_energy_scrapers` from GitHub.

## Configuration

The service reads `config.json` in the current directory.

## Running

```bash
cargo run
```

## Output

Data is saved to the `data/` directory in CSV format.
Files are named after the scraper name (e.g., `data/apg_imb_15min.csv`).
Only new data points are appended to the files.

## Notes

-   For cross-border flows (e.g., `de_to_at`), the code currently uses `APGAction::ATImb15Min` as a placeholder because the correct variant for `CrossBorderPhysicalFlow` could not be determined from the dependency. Please update `src/main.rs` with the correct `APGAction` variant if known.
