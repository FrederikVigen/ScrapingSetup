use anyhow::Result;
use chrono::{DateTime, Utc, Datelike, TimeZone};
use chrono_tz::Europe::Vienna;
use std::fs::File;
use std::path::Path;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use arrow::array::{Float64Array, TimestampMicrosecondArray, Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

pub struct Storage {
    base_path: String,
    dirty_files: Option<Arc<Mutex<HashSet<String>>>>,
}

impl Storage {
    pub fn new(base_path: &str, dirty_files: Option<Arc<Mutex<HashSet<String>>>>) -> Self {
        Self {
            base_path: base_path.to_string(),
            dirty_files,
        }
    }

    pub async fn save_if_new(&self, name: &str, subfolder: Option<&str>, data: &[(DateTime<Utc>, DateTime<Utc>, f64)]) -> Result<bool> {
        let mut saved_any = false;
        let mut groups: HashMap<(i32, u32, u32), Vec<(DateTime<Utc>, DateTime<Utc>, f64)>> = HashMap::new();

        for (start, end, value) in data {
            let start_cet = start.with_timezone(&Vienna);
            let year = start_cet.year();
            let month = start_cet.month();
            let day = start_cet.day();
            groups.entry((year, month, day)).or_default().push((*start, *end, *value));
        }

        for ((year, month, day), group_data) in groups {
            let folder_path = if let Some(sub) = subfolder {
                format!("{}/{}", self.base_path, sub)
            } else {
                format!("{}/{}", self.base_path, name)
            };

            let file_path = format!("{}/year={}/month={:02}/day={:02}/data.parquet", folder_path, year, month, day);
            if self.process_partition(&file_path, &group_data)? {
                saved_any = true;
                if let Some(dirty) = &self.dirty_files {
                    dirty.lock().await.insert(file_path);
                }
            }
        }

        Ok(saved_any)
    }

    pub async fn cleanup(&self, retention_days: u64) -> Result<()> {
        let cutoff = Utc::now() - chrono::Duration::days(retention_days as i64);
        info!("Cleaning up files older than {} days (cutoff: {})", retention_days, cutoff);
        
        let base = Path::new(&self.base_path);
        if base.exists() {
            self.cleanup_recursive(base, cutoff)?;
        }
        Ok(())
    }

    fn cleanup_recursive(&self, path: &Path, cutoff: DateTime<Utc>) -> Result<()> {
        if path.is_dir() {
            // Check if this is a 'day=DD' directory
            if let Some(day_val) = self.extract_date_part(path, "day=") {
                if let Some(parent) = path.parent() {
                    if let Some(month_val) = self.extract_date_part(parent, "month=") {
                        if let Some(grandparent) = parent.parent() {
                            if let Some(year_val) = self.extract_date_part(grandparent, "year=") {
                                if let Some(date) = Vienna.with_ymd_and_hms(year_val, month_val as u32, day_val as u32, 0, 0, 0).single() {
                                     let cutoff_cet = cutoff.with_timezone(&Vienna);
                                     // Compare dates only
                                     if date.date_naive() < cutoff_cet.date_naive() {
                                         info!("Deleting old data: {:?}", path);
                                         std::fs::remove_dir_all(path)?;
                                         return Ok(()); 
                                     }
                                }
                            }
                        }
                    }
                }
            }
            
            // Read dir again in case we deleted it (though we return above)
            if path.exists() {
                for entry in std::fs::read_dir(path)? {
                    let entry = entry?;
                    self.cleanup_recursive(&entry.path(), cutoff)?;
                }
                
                // Try to remove empty directories
                let _ = std::fs::remove_dir(path);
            }
        }
        Ok(())
    }
    
    fn extract_date_part(&self, path: &Path, prefix: &str) -> Option<i32> {
        path.file_name()
            .and_then(|n| n.to_str())
            .and_then(|s| s.strip_prefix(prefix))
            .and_then(|s| s.parse().ok())
    }

    fn process_partition(&self, file_path: &str, data: &[(DateTime<Utc>, DateTime<Utc>, f64)]) -> Result<bool> {
        let path = Path::new(file_path);

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut latest_values: HashMap<(i64, i64), u64> = HashMap::new();
        let mut existing_batches = Vec::new();
        
        // Define the target schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("start", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("end", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("value", DataType::Float64, false),
            Field::new("scraped_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        ]));

        if path.exists() {
            let file = File::open(path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let mut reader = builder.build()?;
            
            while let Some(batch) = reader.next() {
                let batch = batch?;
                
                // Extract data for deduplication
                let start_col = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let end_col = batch.column(1).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let value_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
                
                for i in 0..start_col.len() {
                    let start = start_col.value(i);
                    let end = end_col.value(i);
                    let value = value_col.value(i);
                    latest_values.insert((start, end), value.to_bits());
                }

                // Normalize batch to new schema if needed
                if batch.schema().fields().len() < 4 {
                    // Missing scraped_at, append nulls
                    let len = batch.num_rows();
                    let scraped_at_col = TimestampMicrosecondArray::from(vec![None; len]).with_timezone("UTC");
                    
                    let new_batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            batch.column(0).clone(),
                            batch.column(1).clone(),
                            batch.column(2).clone(),
                            Arc::new(scraped_at_col),
                        ],
                    )?;
                    existing_batches.push(new_batch);
                } else {
                    existing_batches.push(batch);
                }
            }
        }

        let mut new_starts = Vec::new();
        let mut new_ends = Vec::new();
        let mut new_values = Vec::new();
        let mut new_scraped_ats = Vec::new();
        
        let now_micros = Utc::now().timestamp_micros();

        for (start, end, value) in data {
            let start_micros = start.timestamp_micros();
            let end_micros = end.timestamp_micros();
            let value_bits = value.to_bits();
            
            let is_changed = match latest_values.get(&(start_micros, end_micros)) {
                Some(&last_value_bits) => last_value_bits != value_bits,
                None => true,
            };
            
            if is_changed {
                new_starts.push(start_micros);
                new_ends.push(end_micros);
                new_values.push(*value);
                new_scraped_ats.push(now_micros);
                
                // Update latest values to handle multiple updates in the same scrape batch
                latest_values.insert((start_micros, end_micros), value_bits);
            }
        }

        if new_starts.is_empty() {
            return Ok(false);
        }

        let start_array = TimestampMicrosecondArray::from(new_starts).with_timezone("UTC");
        let end_array = TimestampMicrosecondArray::from(new_ends).with_timezone("UTC");
        let value_array = Float64Array::from(new_values);
        let scraped_at_array = TimestampMicrosecondArray::from(new_scraped_ats).with_timezone("UTC");

        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(start_array),
                Arc::new(end_array),
                Arc::new(value_array),
                Arc::new(scraped_at_array),
            ],
        )?;

        // Write everything back to a temp file first for atomic updates
        let tmp_path = format!("{}.tmp", file_path);
        let file = File::create(&tmp_path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;

        for batch in existing_batches {
            writer.write(&batch)?;
        }
        writer.write(&new_batch)?;

        writer.close()?;
        
        // Atomic rename
        std::fs::rename(&tmp_path, path)?;
        
        Ok(true)
    }
}
