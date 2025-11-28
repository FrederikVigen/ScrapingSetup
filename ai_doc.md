# Energy Strategy Scrapers Documentation

This document provides technical details on how to use the `energy_strategy_scrapers` crate. It is intended for AI assistants and developers integrating this library.

## Overview

`energy_strategy_scrapers` is a Rust library for scraping energy market data from various sources (currently APG and ENTSO-E). It defines a common `Scraper` trait and provides implementations for specific data providers.

## Installation

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
energy_strategy_scrapers = { path = "../energy_strategy_scrapers" } # Or git/crates.io reference
anyhow = "1.0"
async-trait = "0.1"
chrono = "0.4"
serde_json = "1.0"
tokio = { version = "1.0", features = ["full"] }
```

## Core Components

### `Scraper` Trait

The core interface is the `Scraper` trait, defined in `src/scraper.rs`.

```rust
#[async_trait]
pub trait Scraper: Send + Sync {
    fn get_config(&self) -> &StrategyInformationScraperConfig;
    async fn scrape_data(&self) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>, f64)>>;
}
```

- `get_config()`: Returns the configuration used by the scraper.
- `scrape_data()`: Asynchronously fetches data and returns a vector of tuples: `(start_time, end_time, value)`.

### `StrategyInformationScraperConfig`

Configuration struct used to initialize scrapers. It supports dynamic key-value pairs via a flattened HashMap.

```rust
pub struct StrategyInformationScraperConfig {
    pub name: String,
    pub workers: u32,
    pub task_generator_delay_ms: u32,
    pub values: HashMap<String, Value>, // Dynamic configuration values
}
```

## Implementations

### 1. APG Information Scraper

Scrapes data from the Austrian Power Grid (APG).

**Module**: `energy_strategy_scrapers::apg_information_scraper`
**Struct**: `APGInformationScraper`

**Initialization**:
```rust
use energy_strategy_scrapers::apg_information_scraper::APGInformationScraper;
use energy_strategy_scrapers::models::apg_actions::APGAction;

let scraper = APGInformationScraper::new(config, APGAction::ATImb15Min)?;
```

**Required Config Keys**:
- `url`: Base URL for the APG API.
- `value_column`: The internal name of the column to extract from the response.

**Supported Actions (`APGAction`)**:
- `ATImb15Min`
- `ATImb5Min`
- `ATImbPrice15Min`
- `CrossBorderPhysicalFlow`

### 2. ENTSO-E Information Scraper

Scrapes data from the ENTSO-E Transparency Platform.

**Module**: `energy_strategy_scrapers::entsoe_information_scraper`
**Struct**: `EntsoeInformationScraper`

**Initialization**:
```rust
use energy_strategy_scrapers::entsoe_information_scraper::EntsoeInformationScraper;

let scraper = EntsoeInformationScraper::new(config)?;
```

**Required Config Keys**:
- `url`: Base URL for the ENTSO-E API.
- `token`: Security token for authentication.

## Usage Example

```rust
use std::collections::HashMap;
use serde_json::Value;
use energy_strategy_scrapers::models::strategy_information_scraper_config::StrategyInformationScraperConfig;
use energy_strategy_scrapers::apg_information_scraper::APGInformationScraper;
use energy_strategy_scrapers::models::apg_actions::APGAction;
use energy_strategy_scrapers::scraper::Scraper;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Create Configuration
    let mut values = HashMap::new();
    values.insert("url".to_string(), Value::String("https://api.apg.at/...".to_string()));
    values.insert("value_column".to_string(), Value::String("CB_AT_DE".to_string()));

    let config = StrategyInformationScraperConfig {
        name: "APG_Scraper_Test".to_string(),
        workers: 1,
        task_generator_delay_ms: 1000,
        values,
    };

    // 2. Initialize Scraper
    let scraper = APGInformationScraper::new(config, APGAction::CrossBorderPhysicalFlow)?;

    // 3. Scrape Data
    let data = scraper.scrape_data().await?;

    for (start, end, value) in data {
        println!("{} - {}: {}", start, end, value);
    }

    Ok(())
}
```
