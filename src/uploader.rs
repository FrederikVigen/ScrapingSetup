use anyhow::Result;
use aws_sdk_s3::Client;
use aws_config::Region;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};

pub struct Uploader {
    client: Client,
    bucket: String,
    pending_files: Arc<Mutex<HashSet<String>>>,
}

impl Uploader {
    pub async fn new(bucket: String, region: Option<String>) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let config = if let Some(region) = region {
            config.into_builder().region(Region::new(region)).build()
        } else {
            config
        };
        
        let client = Client::new(&config);
        
        Ok(Self {
            client,
            bucket,
            pending_files: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub fn get_pending_files_handle(&self) -> Arc<Mutex<HashSet<String>>> {
        self.pending_files.clone()
    }

    pub async fn run(&self) {
        info!("Starting S3 uploader for bucket: {}", self.bucket);
        
        loop {
            sleep(Duration::from_secs(60)).await;
            
            let files_to_upload = {
                let mut pending = self.pending_files.lock().await;
                let files: Vec<String> = pending.drain().collect();
                files
            };

            if files_to_upload.is_empty() {
                continue;
            }

            info!("Uploading {} files to S3", files_to_upload.len());

            let mut failed_uploads = Vec::new();

            for file_path in files_to_upload {
                if let Err(e) = self.upload_file(&file_path).await {
                    warn!("Failed to upload {}: {:?}. Will retry in next cycle.", file_path, e);
                    failed_uploads.push(file_path);
                }
            }

            if !failed_uploads.is_empty() {
                let mut pending = self.pending_files.lock().await;
                for file_path in failed_uploads {
                    pending.insert(file_path);
                }
            }
        }
    }

    async fn upload_file(&self, file_path: &str) -> Result<()> {
        let path = Path::new(file_path);
        let relative_path = path.strip_prefix("data/")?.to_string_lossy();
        let key = format!("data/{}", relative_path);
        
        let body = aws_sdk_s3::primitives::ByteStream::from_path(path).await?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await?;

        info!("Uploaded {}", key);
        Ok(())
    }
}
