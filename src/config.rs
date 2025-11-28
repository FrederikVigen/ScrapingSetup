use serde::{Deserialize, Serialize};
use ve_energy_scrapers::models::strategy_information_scraper_config::StrategyInformationScraperConfig;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ScraperConfig {
    #[serde(flatten)]
    pub scraper_config: StrategyInformationScraperConfig,
    pub sub_data_folder: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AppConfig {
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub scrapers: Vec<ScraperConfig>,
    pub retention_days: Option<u64>,
}

pub fn load_config(path: &str) -> anyhow::Result<AppConfig> {
    let content = std::fs::read_to_string(path)?;
    let config: AppConfig = serde_json::from_str(&content)?;
    Ok(config)
}
