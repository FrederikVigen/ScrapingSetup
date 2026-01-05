use anyhow::{Context, Result};
use chrono::{NaiveDate, Duration, Datelike};
use std::env;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
use indicatif::{ProgressBar, ProgressStyle};

use scraping_service::config;
use config::load_config;

use aws_config;
use aws_sdk_s3::Client;

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
        eprintln!("Usage: {} <scraper_name|all> <start_date> <end_date>", args[0]);
        eprintln!("  scraper_name: Name of the scraper from config.json, or 'all' for all scrapers");
        eprintln!("  start_date: Start date in YYYY-MM-DD format");
        eprintln!("  end_date: End date in YYYY-MM-DD format");
        eprintln!("\nExample: {} apg_imb_15min 2025-01-01 2026-01-05", args[0]);
        eprintln!("Example: {} all 2025-01-01 2026-01-05", args[0]);
        std::process::exit(1);
    }

    let scraper_filter = &args[1];
    let start_date_str = &args[2];
    let end_date_str = &args[3];

    // Parse dates
    let start_date = NaiveDate::parse_from_str(start_date_str, "%Y-%m-%d")
        .context("Failed to parse start_date. Use YYYY-MM-DD format")?;
    
    let end_date = NaiveDate::parse_from_str(end_date_str, "%Y-%m-%d")
        .context("Failed to parse end_date. Use YYYY-MM-DD format")?;

    let total_days = (end_date - start_date).num_days() + 1;
    
    if total_days <= 0 {
        eprintln!("Error: end_date must be equal to or after start_date");
        std::process::exit(1);
    }

    // Load config
    let config = load_config("config.json").context("Failed to load config.json")?;
    
    let bucket = config.get_s3_bucket().context("No S3 bucket configured")?;
    let prefix = config.get_s3_prefix();
    let s3_endpoint = config.get_s3_endpoint();
    let s3_region = config.get_s3_region();
    
    info!("Verifying date range in S3 bucket: {}", bucket);
    info!("Date range: {} to {} ({} days)", start_date, end_date, total_days);
    
    // Filter scrapers (clone to avoid move issues)
    let scrapers_to_check: Vec<_> = if scraper_filter == "all" {
        config.scrapers.clone()
    } else {
        config.scrapers.clone().into_iter()
            .filter(|s| s.scraper_config.name == *scraper_filter)
            .collect()
    };
    
    if scrapers_to_check.is_empty() {
        eprintln!("Error: No matching scrapers found for '{}'", scraper_filter);
        std::process::exit(1);
    }
    
    info!("Checking {} scraper(s)", scrapers_to_check.len());
    
    // Set up AWS S3 client with same credential logic as uploader
    let region = s3_region.unwrap_or_else(|| "eu-central".to_string());
    
    let mut s3_config_builder = aws_sdk_s3::config::Builder::new()
        .region(aws_sdk_s3::config::Region::new(region))
        .behavior_version_latest();
    
    // For S3-compatible services
    if let Some(endpoint_url) = s3_endpoint {
        info!("Using custom S3 endpoint: {}", endpoint_url);
        s3_config_builder = s3_config_builder
            .endpoint_url(endpoint_url)
            .force_path_style(true);
    }
    
    // Try custom S3_* env vars first, then fall back to AWS_* env vars
    let access_key = env::var("S3_ACCESS_KEY")
        .or_else(|_| env::var("AWS_ACCESS_KEY_ID"));
    let secret_key = env::var("S3_SECRET_KEY")
        .or_else(|_| env::var("AWS_SECRET_ACCESS_KEY"));
    
    if let (Ok(access), Ok(secret)) = (access_key, secret_key) {
        info!("Using S3 credentials from environment variables");
        let credentials = aws_sdk_s3::config::Credentials::new(access, secret, None, None, "env");
        s3_config_builder = s3_config_builder.credentials_provider(credentials);
    } else {
        info!("Using default AWS credential chain");
        let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        if let Some(credentials_provider) = shared_config.credentials_provider() {
            s3_config_builder = s3_config_builder.credentials_provider(credentials_provider);
        }
    }
    
    let client = Client::from_conf(s3_config_builder.build());
    
    // Check each scraper
    for scraper_config in &scrapers_to_check {
        println!("\n=== Checking {} ===", scraper_config.scraper_config.name);
        
        // Construct the path the same way storage does
        let base_folder = if let Some(sub) = &scraper_config.sub_data_folder {
            sub.clone()
        } else {
            scraper_config.scraper_config.name.clone()
        };
        
        // The S3 key is: prefix + base_folder + /year=.../month=.../day=.../data.parquet
        // This matches how the uploader constructs keys from local files
        
        // Create progress bar
        let pb = ProgressBar::new(total_days as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} days\n{msg}")
                .unwrap()
                .progress_chars("#>-")
        );
        
        let mut missing_dates = Vec::new();
        let mut current_date = start_date;
        
        for _ in 0..total_days {
            let year = current_date.year();
            let month = current_date.month();
            let day = current_date.day();
            
            // Construct S3 key: prefix + base_folder + partition path
            let s3_key = format!("{}{}/year={}/month={:02}/day={:02}/data.parquet", 
                prefix, base_folder, year, month, day);
            
            info!("Checking S3 key: {}", s3_key);
            pb.set_message(format!("Checking {}", current_date));
            
            // Check if file exists in S3
            match client
                .head_object()
                .bucket(&bucket)
                .key(&s3_key)
                .send()
                .await
            {
                Ok(_) => {
                    info!("Found: {}", s3_key);
                    // File exists
                }
                Err(e) => {
                    info!("Not found: {} - Error: {:?}", s3_key, e);
                    missing_dates.push(current_date);
                    pb.println(format!("  ⚠ Missing: {}", current_date));
                }
            }
            
            pb.inc(1);
            current_date = current_date + Duration::days(1);
        }
        
        pb.finish_and_clear();
        
        // Print summary for this scraper
        if missing_dates.is_empty() {
            println!("✓ All {} days present in S3", total_days);
        } else {
            println!("⚠ Missing {} of {} days:", missing_dates.len(), total_days);
            for date in &missing_dates {
                println!("  - {}", date);
            }
        }
    }
    
    Ok(())
}
