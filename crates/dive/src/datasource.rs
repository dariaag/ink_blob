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

/// Configuration for the `Datasource` which includes base URL, maximum concurrent requests,
/// rate limiter, and semaphore for limiting concurrent operations.
#[derive(Clone, Debug)]
pub struct DatasourceConfig {
    pub base_url: String,
    pub max_concurrent_requests: usize,
    pub rate_limiter:
        Option<Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>>,
    pub semaphore: Option<Arc<Semaphore>>,
}

impl DatasourceConfig {
    /// Creates a new `DatasourceConfig` with the specified base URL and maximum concurrent requests.
    ///
    /// # Examples
    ///
    /// no_run
    /// let config = DatasourceConfig::new("https://api.example.com".to_string(), 10);
    ///
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

/// Datasource struct to interact with the API, perform rate-limited requests,
/// and fetch data as JSON or Polars DataFrame.
pub struct Datasource {
    client: Client,
    config: DatasourceConfig,
}

impl Datasource {
    /// Creates a new `Datasource` with the specified configuration.
    ///
    /// # Examples
    ///
    /// no_run
    /// let config = DatasourceConfig::new("https://api.example.com".to_string(), 10);
    /// let datasource = Datasource::new(config);
    ///
    pub fn new(config: DatasourceConfig) -> Self {
        let client = Client::new();
        Self { client, config }
    }

    /// Retrieves the current dataset height from the API.
    ///
    /// # Examples
    ///
    /// no_run
    /// let height = datasource.get_dataset_height().await?;
    ///
    pub async fn get_dataset_height(&self) -> Result<u64, Error> {
        let url = format!("{}/height", self.config.base_url);

        let response: Value = self.client.get(&url).send().await?.json().await?;

        response
            .as_u64()
            .ok_or_else(|| Error::msg("Invalid response format"))
    }

    /// Retrieves the worker URL for a specific block number.
    ///
    /// # Examples
    ///
    /// no_run
    /// let worker_url = datasource.get_worker_url(12345).await?;
    ///
    pub async fn get_worker_url(&self, block_number: u64) -> Result<String, Error> {
        let url = format!("{}/{}/worker", self.config.base_url, block_number);

        let response: String = self.client.get(&url).send().await?.text().await?;
        response
            .parse()
            .map_err(|e| Error::msg(format!("Error parsing worker URL: {}", e)))
    }

    /// Fetches data from the specified block using the worker URL and query.
    ///
    /// # Examples
    ///
    /// no_run
    /// let (data, last_block) = datasource.fetch_data(12345, "https://worker.url", query).await?;
    ///
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
        /* if data.as_array().is_none() {
            println!("DATA: {:?}", data);
        } */
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

    /// Acquires a permit for making a request, respecting the semaphore limits.
    async fn acquire_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        if let Some(semaphore) = &self.config.semaphore {
            semaphore.clone().acquire_owned().await.ok()
        } else {
            None
        }
    }

    /// Checks the rate limiter and waits if necessary.
    async fn check_rate_limit(&self) {
        if let Some(rate_limiter) = &self.config.rate_limiter {
            rate_limiter.until_ready().await;
        }
    }

    /// Retrieves data in the specified block range.
    ///
    /// # Examples
    ///
    /// no_run
    /// let data = datasource.get_data_in_range(query, 100, 200).await?;
    ///
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

    /// Retrieves data in the specified block range and converts it to a Polars DataFrame.
    ///
    /// # Examples
    ///
    /// no_run
    /// let df = datasource.get_as_df(query, 100, 200).await?;
    ///
    pub async fn get_as_df(
        &self,
        query: Value,
        start_block: u64,
        end_block: u64,
    ) -> Result<DataFrame, Error> {
        let data = self
            .get_data_in_range(query.clone(), start_block, end_block)
            .await?;
        //println!("DATA: {:?}", data);
        let fields = to_df::fields::extract_fields(&query);
        let dataset = to_df::fields::get_dataset(&query);

        let df = to_df::to_df(dataset, data, fields).unwrap();
        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use crate::query_builder::{
        LogFields, LogRequest, QueryBuilder, TransactionFields, TransactionRequest,
    };

    use super::*;
    use serde_json::json;
    use tokio::runtime::Runtime;

    const BASE_URL: &str = "https://v2.archive.subsquid.io/network/ethereum-mainnet";

    #[tokio::test]
    async fn test_get_dataset_height() {
        let config = DatasourceConfig::new(BASE_URL.to_string(), 10);
        let api = Datasource::new(config);

        let height = api.get_dataset_height().await;
        assert!(height.is_ok());
        let height = height.unwrap();
        assert!(height > 0, "Dataset height should be greater than 0");
    }

    #[tokio::test]
    async fn test_get_worker_url() {
        let config = DatasourceConfig::new(BASE_URL.to_string(), 10);
        let api = Datasource::new(config);

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
        let config = DatasourceConfig::new(BASE_URL.to_string(), 10);
        let api = Datasource::new(config);

        let worker_url = api.get_worker_url(1).await.unwrap();
        let query = json!({});
        let (data, last_block) = api.fetch_data(1, &worker_url, query).await.unwrap();

        assert!(!data.is_empty(), "Data should not be empty");
        assert!(last_block > 0, "Last block should be greater than 0");
    }

    #[tokio::test]
    async fn test_get_data_in_range() {
        let config = DatasourceConfig::new(BASE_URL.to_string(), 10);
        let api = Datasource::new(config);

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

    #[tokio::test]
    async fn test_get_as_df() {
        let config = DatasourceConfig::new(BASE_URL.to_string(), 10);
        let api = Datasource::new(config);

        let query = json!({"logs": [
              {
                "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
                "topic0": [
                  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                ],
                "transaction": true
              }
            ],
            "fields": {

              "log": {
                "address": true,
                "topics": true,
                "data": true
              }

            },
        });
        println!("GOOD QUERY: {:?}", query);
        let start_block = 14000000;
        let end_block = 14000001;
        let df = api.get_as_df(query, start_block, end_block).await.unwrap();
    }

    #[tokio::test]
    async fn test_with_querybuilder() {
        let config = DatasourceConfig::new(BASE_URL.to_string(), 10);
        let api = Datasource::new(config);

        let log_request = LogRequest {
            address: Some(vec![
                "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string()
            ]),
            topic0: Some(vec![
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string(),
            ]),
            ..Default::default()
        };
        let log_fields = LogFields {
            address: true,
            topics: true,
            data: true,
            log_index: true,
            ..Default::default()
        };

        let mut query_builder = QueryBuilder::new();
        query_builder
            .select_log_fields(log_fields)
            .add_log(log_request);

        let query = query_builder.build();
        let start_block = 14000005;
        let end_block = 14000006;

        println!("QUERY: {:?}", query);
        let df = api.get_as_df(query, start_block, end_block).await.unwrap();

        println!("{:?}", df);
    }

    #[tokio::test]
    async fn test_tx_with_querybuilder() {
        let config = DatasourceConfig::new(BASE_URL.to_string(), 10);
        let api = Datasource::new(config);

        let tx_request = TransactionRequest {
            to: Some(vec![
                "0x0000000000000000000000000000000000000000".to_string()
            ]),
            ..Default::default()
        };

        let tx_fields = TransactionFields {
            from: true,
            to: true,
            value: true,
            ..Default::default()
        };

        let mut query_builder = QueryBuilder::new();
        query_builder
            .add_transaction(tx_request)
            .select_tx_fields(tx_fields);

        let query = query_builder.build();
        let start_block = 14000005;
        let end_block = 14000006;

        println!("QUERY: {:?}", query);
        let df = api.get_as_df(query, start_block, end_block).await.unwrap();
        println!("TXS");
        println!("{:?}", df);
    }
}
