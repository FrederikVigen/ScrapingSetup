use anyhow::{Context, Result};
use tracing::{info, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use chrono::{Duration as ChronoDuration, Utc};

use scraping_service::{config, storage, uploader, scraper_factory};
use config::{load_config, ScraperConfig};
use storage::Storage;
use uploader::Uploader;

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file in debug builds only
    #[cfg(debug_assertions)]
    dotenvy::dotenv().ok();

    let file_appender = tracing_appender::rolling::daily("logs", "service.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        )
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(non_blocking)
                .with_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        )
        .init();

    let config = load_config("config.json").context("Failed to load config.json")?;
    
    let mut dirty_files_handle = None;
    
    // Use env vars with fallback to config file values
    if let Some(bucket) = config.get_s3_bucket() {
        let uploader = Uploader::new(
            bucket,
            config.get_s3_region(),
            config.get_s3_endpoint(),
            config.get_s3_prefix(),
        ).await?;
        dirty_files_handle = Some(uploader.get_pending_files_handle());
        
        tokio::spawn(async move {
            uploader.run().await;
        });
    }

    let storage = Arc::new(Storage::new("data", dirty_files_handle));

    if let Some(retention_days) = config.retention_days {
        let storage_cleanup = storage.clone();
        tokio::spawn(async move {
            info!("Starting cleanup task with retention of {} days", retention_days);
            loop {
                if let Err(e) = storage_cleanup.cleanup(retention_days).await {
                    error!("Cleanup failed: {:?}", e);
                }
                sleep(Duration::from_secs(24 * 60 * 60)).await;
            }
        });
    }

    for scraper_config in config.scrapers {
        let storage_clone = storage.clone();
        if let Err(e) = start_scraper_pool(scraper_config, storage_clone).await {
            error!("Failed to start scraper pool: {:?}", e);
        }
    }

    // Keep the main thread alive
    tokio::signal::ctrl_c().await?;
    info!("Shutting down");

    Ok(())
}

async fn start_scraper_pool(config: ScraperConfig, storage: Arc<Storage>) -> Result<()> {
    let name = config.scraper_config.name.clone();
    let workers = config.scraper_config.workers;
    let delay = config.scraper_config.task_generator_delay_ms as u64;
    let subfolder = config.sub_data_folder.clone();

    let scraper = scraper_factory::create_scraper(&config.scraper_config)?;
    let scraper = Arc::new(scraper);
    
    // Create a channel for tasks. The buffer size can be adjusted.
    // Using a buffer of workers * 2 to allow some queuing but provide backpressure if workers are slow.
    let buffer_size = if workers > 0 { workers as usize * 2 } else { 10 };
    let (tx, rx) = mpsc::channel::<()>(buffer_size);
    let rx = Arc::new(Mutex::new(rx));

    info!("Starting scraper pool for {}: {} workers, {}ms delay", name, workers, delay);

    // Task Generator
    let name_gen = name.clone();
    tokio::spawn(async move {
        loop {
            if tx.send(()).await.is_err() {
                error!("Receiver dropped for {}, stopping generator", name_gen);
                break;
            }
            sleep(Duration::from_millis(delay)).await;
        }
    });

    // Workers
    for i in 0..workers {
        let rx = rx.clone();
        let scraper = scraper.clone();
        let storage = storage.clone();
        let worker_name = format!("{}-worker-{}", name, i);
        let scraper_name = name.clone();
        let subfolder = subfolder.clone();

        tokio::spawn(async move {
            loop {
                // Acquire lock just to get the task
                {
                    let mut lock = rx.lock().await;
                    if lock.recv().await.is_none() {
                        break; // Channel closed
                    }
                } // Lock released here

                // Define date range: yesterday, today, and tomorrow
                let now = Utc::now();
                let start_date = now - ChronoDuration::days(1); // Yesterday
                let end_date = now + ChronoDuration::days(1);   // Tomorrow

                // Perform the scrape
                match scraper.scrape_data(start_date, end_date).await {
                    Ok(data) => {
                        if !data.is_empty() {
                            match storage.save_if_new(&scraper_name, subfolder.as_deref(), &data).await {
                                Ok(saved) => {
                                    if saved {
                                        info!("[{}] Saved new data", worker_name);
                                    }
                                }
                                Err(e) => error!("[{}] Failed to save data: {:?}", worker_name, e),
                            }
                        }
                    }
                    Err(e) => {
                        error!("[{}] Error scraping: {:?}", worker_name, e);
                    }
                }
            }
        });
    }

    Ok(())
}

