use anyhow::Result;
use chrono::{DateTime, Utc, Datelike, TimeZone};
use chrono_tz::Europe::Vienna;
use std::fs::File;
use std::path::Path;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use arrow::array::{Float64Array, TimestampMicrosecondArray, Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use ve_energy_scrapers::models::scraper_data::{ScraperData, ScraperPayload, Bid};

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

    pub async fn save_if_new(&self, name: &str, subfolder: Option<&str>, data: &[ScraperData]) -> Result<bool> {
        self.save_with_scraped_at(name, subfolder, data, true).await
    }

    pub async fn save_backfill(&self, name: &str, subfolder: Option<&str>, data: &[ScraperData]) -> Result<bool> {
        self.save_with_scraped_at(name, subfolder, data, false).await
    }

    async fn save_with_scraped_at(&self, name: &str, subfolder: Option<&str>, data: &[ScraperData], set_scraped_at: bool) -> Result<bool> {
        let mut saved_any = false;
        
        // Separate data by type
        let mut values_data: Vec<(DateTime<Utc>, DateTime<Utc>, HashMap<String, f64>)> = Vec::new();
        let mut bids_data: Vec<(DateTime<Utc>, DateTime<Utc>, Bid)> = Vec::new();

        for item in data {
            match &item.payload {
                ScraperPayload::Values(map) => {
                    values_data.push((item.delivery_from, item.delivery_to, map.clone()));
                }
                ScraperPayload::Bids(bids) => {
                    for bid in bids {
                        bids_data.push((item.delivery_from, item.delivery_to, bid.clone()));
                    }
                }
            }
        }

        if !values_data.is_empty() {
            let mut groups: HashMap<(i32, u32, u32), Vec<(DateTime<Utc>, DateTime<Utc>, HashMap<String, f64>)>> = HashMap::new();
            for (start, end, map) in values_data {
                let start_cet = start.with_timezone(&Vienna);
                let year = start_cet.year();
                let month = start_cet.month();
                let day = start_cet.day();
                groups.entry((year, month, day)).or_default().push((start, end, map));
            }

            for ((year, month, day), group_data) in groups {
                let folder_path = if let Some(sub) = subfolder {
                    format!("{}/{}", self.base_path, sub)
                } else {
                    format!("{}/{}", self.base_path, name)
                };

                let file_path = format!("{}/year={}/month={:02}/day={:02}/data.parquet", folder_path, year, month, day);
                if self.process_values_partition(&file_path, &group_data, set_scraped_at)? {
                    saved_any = true;
                    if let Some(dirty) = &self.dirty_files {
                        dirty.lock().await.insert(file_path);
                    }
                }
            }
        }

        if !bids_data.is_empty() {
             let mut groups: HashMap<(i32, u32, u32), Vec<(DateTime<Utc>, DateTime<Utc>, Bid)>> = HashMap::new();
            for (start, end, bid) in bids_data {
                let start_cet = start.with_timezone(&Vienna);
                let year = start_cet.year();
                let month = start_cet.month();
                let day = start_cet.day();
                groups.entry((year, month, day)).or_default().push((start, end, bid));
            }

            for ((year, month, day), group_data) in groups {
                let folder_path = if let Some(sub) = subfolder {
                    format!("{}/{}", self.base_path, sub)
                } else {
                    format!("{}/{}", self.base_path, name)
                };

                let file_path = format!("{}/year={}/month={:02}/day={:02}/data.parquet", folder_path, year, month, day);
                if self.process_bids_partition(&file_path, &group_data, set_scraped_at)? {
                    saved_any = true;
                    if let Some(dirty) = &self.dirty_files {
                        dirty.lock().await.insert(file_path);
                    }
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

    fn process_values_partition(&self, file_path: &str, data: &[(DateTime<Utc>, DateTime<Utc>, HashMap<String, f64>)], set_scraped_at: bool) -> Result<bool> {
        let path = Path::new(file_path);

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut all_rows: HashMap<(i64, i64), (i64, HashMap<String, f64>)> = HashMap::new();
        let mut all_columns: HashSet<String> = HashSet::new();

        if path.exists() {
            let file = File::open(path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let mut reader = builder.build()?;
            
            while let Some(batch) = reader.next() {
                let batch = batch?;
                let schema = batch.schema();
                
                let start_col = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let end_col = batch.column(1).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                let scraped_at_idx = schema.index_of("scraped_at").ok();
                let scraped_at_col = if let Some(idx) = scraped_at_idx {
                    Some(batch.column(idx).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap())
                } else {
                    None
                };

                // Identify value columns
                let mut value_cols = Vec::new();
                for (i, field) in schema.fields().iter().enumerate() {
                    let name = field.name();
                    if name != "start" && name != "end" && name != "scraped_at" {
                        all_columns.insert(name.clone());
                        value_cols.push((name.clone(), batch.column(i).as_any().downcast_ref::<Float64Array>().unwrap()));
                    }
                }

                for i in 0..start_col.len() {
                    let start = start_col.value(i);
                    let end = end_col.value(i);
                    let scraped_at = scraped_at_col.map(|c| c.value(i)).unwrap_or(0);
                    
                    let entry = all_rows.entry((start, end)).or_insert((scraped_at, HashMap::new()));
                    
                    for (name, col) in &value_cols {
                        if !col.is_null(i) {
                            entry.1.insert(name.clone(), col.value(i));
                        }
                    }
                }
            }
        }

        let now_micros = if set_scraped_at {
            Utc::now().timestamp_micros()
        } else {
            0 // null for backfilled data
        };
        let mut has_changes = false;

        for (start, end, new_values) in data {
            let start_micros = start.timestamp_micros();
            let end_micros = end.timestamp_micros();
            
            for k in new_values.keys() {
                all_columns.insert(k.clone());
            }

            let entry = all_rows.entry((start_micros, end_micros)).or_insert((0, HashMap::new()));
            let (existing_scraped_at, existing_values) = entry;

            let mut changed = false;
            if *existing_scraped_at == 0 {
                changed = true;
            } else {
                for (k, v) in new_values {
                    match existing_values.get(k) {
                        Some(old_v) => {
                            if (old_v - v).abs() > f64::EPSILON {
                                changed = true;
                            }
                        }
                        None => changed = true,
                    }
                }
            }

            if changed {
                has_changes = true;
                *existing_scraped_at = now_micros;
                for (k, v) in new_values {
                    existing_values.insert(k.clone(), *v);
                }
            }
        }

        if !has_changes {
            return Ok(false);
        }

        let mut sorted_columns: Vec<String> = all_columns.into_iter().collect();
        sorted_columns.sort();

        let mut fields = vec![
            Field::new("start", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("end", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("scraped_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        ];
        for col in &sorted_columns {
            fields.push(Field::new(col, DataType::Float64, true));
        }
        let schema = Arc::new(Schema::new(fields));

        let mut sorted_rows: Vec<_> = all_rows.into_iter().collect();
        sorted_rows.sort_by_key(|((start, _), _)| *start);

        let mut start_builder = TimestampMicrosecondArray::builder(sorted_rows.len());
        let mut end_builder = TimestampMicrosecondArray::builder(sorted_rows.len());
        let mut scraped_at_builder = TimestampMicrosecondArray::builder(sorted_rows.len());
        
        let mut value_builders: Vec<arrow::array::Float64Builder> = Vec::with_capacity(sorted_columns.len());
        for _ in 0..sorted_columns.len() {
            value_builders.push(arrow::array::Float64Builder::new());
        }

        for ((start, end), (scraped_at, values)) in sorted_rows {
            start_builder.append_value(start);
            end_builder.append_value(end);
            scraped_at_builder.append_value(scraped_at);

            for (i, col_name) in sorted_columns.iter().enumerate() {
                if let Some(val) = values.get(col_name) {
                    value_builders[i].append_value(*val);
                } else {
                    value_builders[i].append_null();
                }
            }
        }

        let mut columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(start_builder.finish().with_timezone("UTC")),
            Arc::new(end_builder.finish().with_timezone("UTC")),
            Arc::new(scraped_at_builder.finish().with_timezone("UTC")),
        ];
        for mut builder in value_builders {
            columns.push(Arc::new(builder.finish()));
        }

        let batch = RecordBatch::try_new(schema.clone(), columns)?;

        let tmp_path = format!("{}.tmp", file_path);
        let file = File::create(&tmp_path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        
        std::fs::rename(&tmp_path, path)?;
        
        Ok(true)
    }

    fn process_bids_partition(&self, file_path: &str, data: &[(DateTime<Utc>, DateTime<Utc>, Bid)], set_scraped_at: bool) -> Result<bool> {
        let path = Path::new(file_path);

        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut latest_values: HashMap<(i64, i64, String, String, i32), (Option<f64>, Option<f64>)> = HashMap::new();
        let mut existing_batches = Vec::new();
        
        // Define the target schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("start", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("end", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("bid_type", DataType::Utf8, false),
            Field::new("direction", DataType::Utf8, false),
            Field::new("rank", DataType::Int32, false),
            Field::new("price", DataType::Float64, true),
            Field::new("volume", DataType::Float64, true),
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
                let bid_type_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
                let direction_col = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
                let rank_col = batch.column(4).as_any().downcast_ref::<Int32Array>().unwrap();
                let price_col = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
                let volume_col = batch.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
                
                for i in 0..start_col.len() {
                    let start = start_col.value(i);
                    let end = end_col.value(i);
                    let bid_type = bid_type_col.value(i).to_string();
                    let direction = direction_col.value(i).to_string();
                    let rank = rank_col.value(i);
                    let price = if price_col.is_null(i) { None } else { Some(price_col.value(i)) };
                    let volume = if volume_col.is_null(i) { None } else { Some(volume_col.value(i)) };
                    
                    latest_values.insert((start, end, bid_type, direction, rank), (price, volume));
                }
                existing_batches.push(batch);
            }
        }

        let mut new_starts = Vec::new();
        let mut new_ends = Vec::new();
        let mut new_bid_types = Vec::new();
        let mut new_directions = Vec::new();
        let mut new_ranks = Vec::new();
        let mut new_prices = Vec::new();
        let mut new_volumes = Vec::new();
        let mut new_scraped_ats = Vec::new();
        
        let now_micros = if set_scraped_at {
            Utc::now().timestamp_micros()
        } else {
            0 // null for backfilled data
        };

        for (start, end, bid) in data {
            let start_micros = start.timestamp_micros();
            let end_micros = end.timestamp_micros();
            let bid_type = format!("{:?}", bid.bid_type);
            let direction = format!("{:?}", bid.direction);
            let rank = bid.rank;
            let price = bid.price;
            let volume = bid.volume;
            
            let is_changed = match latest_values.get(&(start_micros, end_micros, bid_type.clone(), direction.clone(), rank)) {
                Some((last_price, last_volume)) => {
                    let price_changed = match (last_price, price) {
                        (Some(lp), Some(p)) => (lp - p).abs() > f64::EPSILON,
                        (None, None) => false,
                        _ => true,
                    };
                    let volume_changed = match (last_volume, volume) {
                        (Some(lv), Some(v)) => (lv - v).abs() > f64::EPSILON,
                        (None, None) => false,
                        _ => true,
                    };
                    price_changed || volume_changed
                },
                None => true,
            };
            
            if is_changed {
                new_starts.push(start_micros);
                new_ends.push(end_micros);
                new_bid_types.push(bid_type.clone());
                new_directions.push(direction.clone());
                new_ranks.push(rank);
                new_prices.push(price);
                new_volumes.push(volume);
                new_scraped_ats.push(now_micros);
                
                latest_values.insert((start_micros, end_micros, bid_type, direction, rank), (price, volume));
            }
        }

        if new_starts.is_empty() {
            return Ok(false);
        }

        let start_array = TimestampMicrosecondArray::from(new_starts).with_timezone("UTC");
        let end_array = TimestampMicrosecondArray::from(new_ends).with_timezone("UTC");
        let bid_type_array = StringArray::from(new_bid_types);
        let direction_array = StringArray::from(new_directions);
        let rank_array = Int32Array::from(new_ranks);
        let price_array = Float64Array::from(new_prices);
        let volume_array = Float64Array::from(new_volumes);
        let scraped_at_array = TimestampMicrosecondArray::from(new_scraped_ats).with_timezone("UTC");

        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(start_array),
                Arc::new(end_array),
                Arc::new(bid_type_array),
                Arc::new(direction_array),
                Arc::new(rank_array),
                Arc::new(price_array),
                Arc::new(volume_array),
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
