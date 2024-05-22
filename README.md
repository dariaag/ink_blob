# Datasource Library

This Rust library provides a way to interact with [Subsquid](docs.subsquid.io) datalake API to fetch blockchain data, with support for rate limiting and concurrency control. It includes tools to build complex queries and convert the data into a Polars DataFrame. The library is a work in progeress. Feel free to raise an issue or drop a line on Twitter if you have any problems or suggestions.

## Features

- Fetch data from Subsquid data-lake API with rate limiting and concurrency control.
- Build complex queries using a query builder.
- Convert fetched data into a Polars DataFrame.

Currently supports:

- logs
- transactions

## Datasets

Logs have the following fields:

- address
- topics
- data
- log_index
- block
- transaction_hash
- transaction_index

and support the following filters:

- address
- topic0
- topic1
- topic2
- topic3

Transactions have the following fields:

- block_hash
- block_number
- from
- gas
- gas_price
- hash
- input
- nonce
- to
- transaction_index
- value

and support the following filters:

- from
- to
- sighash

## Usage

Here are some examples of how to use this tool.

### Create a Datasource Configuration

```rust
use std::sync::Arc;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
    RateLimiter,
};
use tokio::sync::Semaphore;

let config = DatasourceConfig::new("https://v2.archive.subsquid.io/network/ethereum-mainnet".to_string(), 10);
```

### Initialize a Datasource

```rust
let datasource = Datasource::new(config);
```

### Build a Query using QueryBuilder

```rust
use crate::query_builder::{LogFields, LogRequest, QueryBuilder};

// Define log request
let log_request = LogRequest {
    address: Some(vec!["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string()]),
    topic0: Some(vec!["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string()]),
    ..Default::default()
};

// Define log fields to select
let log_fields = LogFields {
    address: true,
    topics: true,
    data: true,
    log_index: true,
    ..Default::default()
};

// Build query
let mut query_builder = QueryBuilder::new();
query_builder
    .select_log_fields(log_fields)
    .add_log(log_request);

let query = query_builder.build();
```

### Fetch Data in a Range of Blocks as JSON

```rust
let start_block = 14000005;
let end_block = 14000006;
let data = datasource.get_data_in_range(query.clone(), start_block, end_block).await.unwrap();

```

### Fetch as DataFrame

```rust
let df = datasource.get_as_df(query, start_block, end_block).await.unwrap();

```

## Complete Example

Here is a complete example combining all the steps:

```rust
use std::sync::Arc;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
    RateLimiter,
};
use tokio::sync::Semaphore;
use reqwest::Client;
use serde_json::json;
use polars::prelude::*;
use crate::datasource::{Datasource, DatasourceConfig};
use crate::query_builder::{LogFields, LogRequest, QueryBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create datasource configuration
    let config = DatasourceConfig::new("https://v2.archive.subsquid.io/network/ethereum-mainnet".to_string(), 10);

    // Initialize datasource
    let datasource = Datasource::new(config);

    // Build query
    let log_request = LogRequest {
        address: Some(vec!["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string()]),
        topic0: Some(vec!["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string()]),
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

    // Fetch data in a range
    let start_block = 14000005;
    let end_block = 14000006;
    let data = datasource.get_data_in_range(query.clone(), start_block, end_block).await.unwrap();



    // Convert data to DataFrame
    let df = datasource.get_as_df(query, start_block, end_block).await.unwrap();



    Ok(())
}


```

### TODO

- [x] tx
- [x] logs
- [x] blocks
- [ ] traces
- [ ] state diffs
- [ ] joint datasets
- [ ] CLI
- [ ] progress bar
