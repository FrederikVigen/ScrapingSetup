use anyhow::Result;
use ve_energy_scrapers::scraper::Scraper;
use ve_energy_scrapers::apg_information_scraper::APGInformationScraper;
use ve_energy_scrapers::entsoe_information_scraper::EntsoeInformationScraper;
use ve_energy_scrapers::models::strategy_information_scraper_config::StrategyInformationScraperConfig;

pub fn create_scraper(config: &StrategyInformationScraperConfig) -> Result<Box<dyn Scraper>> {
    if let Some(url) = config.values.get("url").and_then(|v| v.as_str()) {
        if url.contains("entsoe") {
            Ok(Box::new(EntsoeInformationScraper::new(config.clone())?))
        } else if url.contains("apg") {
            Ok(Box::new(APGInformationScraper::new(config.clone())?))
        } else {
            Err(anyhow::anyhow!("Unknown scraper URL type: {}", url))
        }
    } else {
        Err(anyhow::anyhow!("Missing URL in config for {}", config.name))
    }
}
