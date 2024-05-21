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
