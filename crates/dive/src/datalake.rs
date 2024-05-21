use std::time::Duration;

//only request response logic
use crate::utils::{add_from_block, get_worker};
use anyhow::Error;
use reqwest::Client;
use serde_json::{json, Map, Value};
use tokio::time::sleep;
pub async fn get_chunk(
    query: Value,
    start_block: u64,
    client: &Client,
) -> Result<(Vec<Value>, u64), Error> {
    //add block range to query

    let worker = get_worker(
        "https://v2.archive.subsquid.io/network/ethereum-mainnet",
        &start_block.to_string(),
    )
    .await?;

    let json_query = add_from_block(query, &start_block.to_string());

    let result: String = client
        .post(worker)
        .json(&json_query)
        .send()
        .await?
        .text()
        .await?;

    let blocks_value: Value = serde_json::from_str(&result)
        .map_err(|e| Error::msg(format!("Error parsing JSON: {}", e)))?;

    let blocks = match blocks_value.as_array() {
        Some(blocks) => blocks,
        None => {
            //println!("Error fetching");
            return Err(Error::msg("Invalid JSON format: Expected an array"));
        }
    };

    let next_block = blocks
        .last()
        .and_then(|b| b["header"]["number"].as_u64())
        .ok_or_else(|| {
            Error::msg("Invalid block data format: 'number' field missing or not a u64")
        })?;
    //println!("Fetched {:?} blocks from {:?}", blocks.len(), start_block);
    Ok((blocks.to_vec(), next_block))
}

pub async fn get_block_range(
    query: Value,
    client: Client,
    start_block: u64,
    end_block: u64,
    //stats_tx: &Sender<u64>,
) -> Result<Vec<Value>, Error> {
    let mut current_start = start_block;
    let mut attempt = 0;
    let mut backoff = Duration::from_millis(100);
    let mut fetched_blocks = Vec::new();
    while current_start < end_block {
        match get_chunk(query.clone(), start_block, &client).await {
            Ok((chunk, next_block)) => {
                fetched_blocks.extend(chunk);
                current_start = next_block;
                attempt = 0;
                //stats_tx.send(fetched_blocks.len()).unwrap();
            }
            Err(e) => {
                eprintln!(
                    "Error fetching blocks starting at {}: {}. Retrying in {:?}",
                    current_start, e, backoff
                );
                if attempt > 5 {
                    return Err(Error::msg("Too many retries"));
                }
                attempt += 1;
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
    }

    Ok(fetched_blocks)
}
