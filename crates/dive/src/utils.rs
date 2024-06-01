use reqwest::{self};
use serde_json::Value;

pub async fn get_height(archive_url: &str) -> Result<String, reqwest::Error> {
    let url = format!("{}/height", archive_url);
    let body = reqwest::get(&url).await?.text().await?;
    Ok(body)
}

pub async fn get_worker(archive_url: &str, first_block: &str) -> Result<String, reqwest::Error> {
    let url: String = format!("{}/{}/worker", archive_url, first_block);
    let body = reqwest::get(&url).await?.text().await?;
    Ok(body)
}

pub fn add_from_block(mut json_value: Value, from_block_value: u64) -> Value {
    if let Value::Object(ref mut map) = json_value {
        map.insert("fromBlock".to_string(), from_block_value.into());
    }
    json_value
}

pub fn compute_chunk_ranges(start: u64, end: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    // Divide the total range into smaller ranges of chunk_size
    (start..end)
        .step_by(chunk_size as usize)
        .map(|start| (start, std::cmp::min(start + chunk_size, end)))
        .collect()
}
