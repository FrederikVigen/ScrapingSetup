use anyhow::{Context, Result};
use chrono::{NaiveDate, Duration};
use std::env;
use std::sync::Arc;
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
use indicatif::{ProgressBar, ProgressStyle};

use scraping_service::{config, storage, scraper_factory, uploader};
use config::load_config;
use storage::Storage;
use uploader::Uploader;

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file in debug builds only
    #[cfg(debug_assertions)]
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        )
        .init();

    let args: Vec<String> = env::args().collect();
    
    if args.len() < 4 {
        eprintln!("Usage: {} <scraper_name> <start_date> <end_date>", args[0]);
        eprintln!("  scraper_name: Name of the scraper from config.json");
        eprintln!("  start_date: Start date in YYYY-MM-DD format");
        eprintln!("  end_date: End date in YYYY-MM-DD format");
        eprintln!("\nExample: {} apg_at_cz_exchange 2025-01-01 2025-01-31", args[0]);
        std::process::exit(1);
    }

    let scraper_name = &args[1];
    let start_date_str = &args[2];
    let end_date_str = &args[3];

    // Parse dates
    let start_date = NaiveDate::parse_from_str(start_date_str, "%Y-%m-%d")
        .context("Failed to parse start_date. Use YYYY-MM-DD format")?;
    
    let end_date = NaiveDate::parse_from_str(end_date_str, "%Y-%m-%d")
        .context("Failed to parse end_date. Use YYYY-MM-DD format")?;

    // Calculate total days
    let total_days = (end_date - start_date).num_days() + 1;
    
    if total_days <= 0 {
        eprintln!("Error: end_date must be equal to or after start_date");
        std::process::exit(1);
    }

    info!("Starting backfill for {} from {} to {} ({} days)", 
        scraper_name, start_date, end_date, total_days);

    // Load config
    let config = load_config("config.json").context("Failed to load config.json")?;
    
    // Find the scraper config
    let scraper_config = config.scrapers.iter()
        .find(|s| s.scraper_config.name == *scraper_name)
        .context(format!("Scraper '{}' not found in config.json", scraper_name))?;

    // Set up uploader if S3 is configured
    let mut dirty_files_handle = None;
    let mut uploader_handle = None;
    
    if let Some(bucket) = config.get_s3_bucket() {
        info!("S3 bucket configured: {}, setting up uploader", bucket);
        let uploader = Uploader::new(
            bucket,
            config.get_s3_region(),
            config.get_s3_endpoint(),
            config.get_s3_prefix(),
        ).await?;
        dirty_files_handle = Some(uploader.get_pending_files_handle());
        
        let handle = tokio::spawn(async move {
            uploader.run().await;
        });
        uploader_handle = Some(handle);
    } else {
        info!("No S3 bucket configured, data will only be stored locally");
    }

    // Create storage with uploader support
    let storage = Arc::new(Storage::new("data", dirty_files_handle));

    // Create scraper
    let scraper = scraper_factory::create_scraper(&scraper_config.scraper_config)?;
    
    // Create progress bar with known length
    let pb = ProgressBar::new(total_days as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} days ({eta})\n{msg}")
            .unwrap()
            .progress_chars("#>-")
    );
    
    let mut total_records = 0;
    let mut days_with_data = 0;
    let mut current_date = start_date;
    
    // Process each day
    for _ in 0..total_days {
        // Use same approach as main service: query a window around the target date
        // This ensures we get all data for the day even with timezone variations
        let target_datetime = current_date.and_hms_opt(12, 0, 0)
            .context("Invalid time")?
            .and_utc();
        let day_start = target_datetime - Duration::days(1); // Day before
        let day_end = target_datetime + Duration::days(1);   // Day after
        
        pb.set_message(format!("Processing {}", current_date));
        
        // Perform the scrape for this day
        match scraper.scrape_data(day_start, day_end).await {
            Ok(data) => {
                if !data.is_empty() {
                    info!("Scraped {} records for {}", data.len(), current_date);
                    match storage.save_backfill(
                        &scraper_config.scraper_config.name,
                        scraper_config.sub_data_folder.as_deref(),
                        &data
                    ).await {
                        Ok(saved) => {
                            if saved {
                                total_records += data.len();
                                days_with_data += 1;
                            } else {
                                pb.println(format!("  {} - {} records (already exists)", current_date, data.len()));
                            }
                        }
                        Err(e) => {
                            pb.println(format!("⚠ Failed to save data for {}: {:?}", current_date, e));
                            error!("Failed to save data for {}: {:?}", current_date, e);
                        }
                    }
                } else {
                    pb.println(format!("  {} - No data returned", current_date));
                }
            }
            Err(e) => {
                pb.println(format!("⚠ Failed to scrape {}: {:?}", current_date, e));
                error!("Failed to scrape {}: {:?}", current_date, e);
            }
        }
        
        pb.inc(1);
        current_date = current_date + Duration::days(1);
    }
    
    pb.finish_with_message(format!("✓ Completed: {} records from {} days with data", 
        total_records, days_with_data));

    // Wait for uploader to process remaining files
    if uploader_handle.is_some() {
        info!("Waiting for S3 uploads to complete...");
        info!("The uploader processes files every 60 seconds.");
        // Wait at least 90 seconds to ensure one full upload cycle completes
        tokio::time::sleep(tokio::time::Duration::from_secs(90)).await;
    }

    Ok(())
}
