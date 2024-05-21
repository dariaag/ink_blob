use crate::utils;
use anyhow::Error;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
    RateLimiter,
};
use polars::prelude::*;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;
use utils::add_from_block;
#[derive(Clone, Debug)]
pub struct SubsquidApiConfig {
    pub base_url: String,
    pub max_concurrent_requests: usize,
    pub rate_limiter:
        Option<Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>>,
    pub semaphore: Option<Arc<Semaphore>>,
}

impl SubsquidApiConfig {
    pub fn new(base_url: String, max_concurrent_requests: usize) -> Self {
        let semaphore = Arc::new(Semaphore::new(max_concurrent_requests));
        let rate_limiter = None; // Configure as needed
        Self {
            base_url,
            max_concurrent_requests,
            rate_limiter,
            semaphore: Some(semaphore),
        }
    }
}

pub struct SubsquidApi {
    client: Client,
    config: SubsquidApiConfig,
}

impl SubsquidApi {
    pub fn new(config: SubsquidApiConfig) -> Self {
        let client = Client::new();
        Self { client, config }
    }

    pub async fn get_dataset_height(&self) -> Result<u64, Error> {
        let url = format!("{}/height", self.config.base_url);

        let response: Value = self.client.get(&url).send().await?.json().await?;

        response
            .as_u64()
            .ok_or_else(|| Error::msg("Invalid response format"))
    }

    pub async fn get_worker_url(&self, block_number: u64) -> Result<String, Error> {
        let url = format!("{}/{}/worker", self.config.base_url, block_number);

        let response: String = self.client.get(&url).send().await?.text().await?;
        response
            .parse()
            .map_err(|e| Error::msg(format!("Error parsing worker URL: {}", e)))
    }
    pub async fn fetch_data(
        &self,
        from_block: u64,
        worker_url: &str,
        query: Value,
    ) -> Result<(Vec<Value>, u64), Error> {
        let json_query = add_from_block(query, from_block);
        let response: String = self
            .client
            .post(worker_url)
            .json(&json_query)
            .send()
            .await?
            .text()
            .await?;
        let data: Value = serde_json::from_str(&response)?;

        let blocks = data
            .as_array()
            .ok_or_else(|| Error::msg("Invalid JSON format: Expected an array"))?;
        let last_block = blocks
            .last()
            .and_then(|b| b["header"]["number"].as_u64())
            .ok_or_else(|| {
                Error::msg("Invalid block data format: 'number' field missing or not a u64")
            })?;
        Ok((blocks.to_vec(), last_block))
    }

    async fn acquire_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        if let Some(semaphore) = &self.config.semaphore {
            semaphore.clone().acquire_owned().await.ok()
        } else {
            None
        }
    }

    async fn check_rate_limit(&self) {
        if let Some(rate_limiter) = &self.config.rate_limiter {
            rate_limiter.until_ready().await;
        }
    }

    pub async fn get_data_in_range(
        &self,
        query: Value,
        start_block: u64,
        end_block: u64,
    ) -> Result<Vec<Value>, Error> {
        let mut current_block = start_block;
        let mut all_data = Vec::new();

        while current_block <= end_block {
            self.check_rate_limit().await;
            let _permit = self.acquire_permit().await;

            let worker_url = self.get_worker_url(current_block).await?;

            let (data, last_block) = self
                .fetch_data(current_block, &worker_url, query.clone())
                .await?;
            all_data.extend(data);
            current_block = last_block + 1;
        }

        Ok(all_data)
    }

    /*   pub async fn get_as_df(
        &self,
        query: Value,
        start_block: u64,
        end_block: u64,
    ) -> Result<DataFrame, Error> {
        let data = self
            .get_data_in_range(query, start_block, end_block)
            .await?;
        let df =

    } */
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::runtime::Runtime;

    const BASE_URL: &str = "https://v2.archive.subsquid.io/network/ethereum-mainnet";

    #[tokio::test]
    async fn test_get_dataset_height() {
        let config = SubsquidApiConfig::new(BASE_URL.to_string(), 10);
        let api = SubsquidApi::new(config);

        let height = api.get_dataset_height().await;
        assert!(height.is_ok());
        let height = height.unwrap();
        assert!(height > 0, "Dataset height should be greater than 0");
    }

    #[tokio::test]
    async fn test_get_worker_url() {
        let config = SubsquidApiConfig::new(BASE_URL.to_string(), 10);
        let api = SubsquidApi::new(config);

        let block_number = 1;
        let worker_url = api.get_worker_url(block_number).await;
        assert!(worker_url.is_ok());
        let worker_url = worker_url.unwrap();
        assert!(
            worker_url.starts_with("http"),
            "Worker URL should start with http"
        );
    }

    #[tokio::test]
    async fn test_fetch_data() {
        let config = SubsquidApiConfig::new(BASE_URL.to_string(), 10);
        let api = SubsquidApi::new(config);

        let worker_url = api.get_worker_url(1).await.unwrap();
        let query = json!({});
        let (data, last_block) = api.fetch_data(1, &worker_url, query).await.unwrap();

        assert!(!data.is_empty(), "Data should not be empty");
        assert!(last_block > 0, "Last block should be greater than 0");
    }

    #[tokio::test]
    async fn test_get_data_in_range() {
        let config = SubsquidApiConfig::new(BASE_URL.to_string(), 10);
        let api = SubsquidApi::new(config);

        let query = json!({});
        let start_block = 1;
        let end_block = 10;
        let data = api
            .get_data_in_range(query, start_block, end_block)
            .await
            .unwrap();

        assert!(!data.is_empty(), "Data should not be empty");
        let last_block = data.last().unwrap()["header"]["number"].as_u64().unwrap();
        assert!(
            last_block >= end_block,
            "Last block should be at least the end block"
        );
    }
}
