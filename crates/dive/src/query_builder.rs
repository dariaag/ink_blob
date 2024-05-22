use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// QueryBuilder struct to build complex queries for logs, transactions, and blocks
#[derive(Default)]
pub struct QueryBuilder {
    select: Option<Map<String, Value>>,
    logs: Vec<Value>,
    transactions: Vec<Value>,
    blocks: Vec<Value>,
}

/// LogRequest struct to hold parameters for log requests
#[derive(Default)]
pub struct LogRequest {
    pub address: Option<Vec<String>>,
    pub topic0: Option<Vec<String>>,
    pub topic1: Option<Vec<String>>,
    pub topic2: Option<Vec<String>>,
    pub topic3: Option<Vec<String>>,
}

/// TransactionRequest struct to hold parameters for transaction requests
#[derive(Default)]
pub struct TransactionRequest {
    pub from: Option<Vec<String>>,
    pub to: Option<Vec<String>>,
    pub sighash: Option<Vec<String>>,
}

/// BlockRequest struct to hold parameters for block requests
pub struct BlockRequest {
    pub block_number: u64,
}

/// LogFields struct to specify which fields to select in log queries
#[derive(Serialize, Deserialize, Default)]
pub struct LogFields {
    // pub id: bool,
    pub log_index: bool,
    pub transaction_index: bool,
    pub block: bool,
    pub address: bool,
    pub data: bool,
    pub topics: bool,
    pub transaction_hash: bool,
}

impl QueryBuilder {
    /// Creates a new instance of QueryBuilder
    ///
    /// # Examples
    ///
    /// no_run
    /// let query_builder = QueryBuilder::new();
    ///
    pub fn new() -> Self {
        QueryBuilder::default()
    }

    /// Builds the final query as a JSON value
    ///
    /// # Examples
    ///
    /// no_run
    /// let query = query_builder.build();
    ///
    pub fn build(self) -> Value {
        let mut query = Map::new();

        if let Some(select) = self.select {
            query.insert("fields".to_string(), json!(select));
        }

        if !self.logs.is_empty() {
            query.insert("logs".to_string(), json!(self.logs));
        }

        if !self.transactions.is_empty() {
            query.insert("transactions".to_string(), json!(self.transactions));
        }

        if !self.blocks.is_empty() {
            query.insert("blocks".to_string(), json!(self.blocks));
        }

        json!(query)
    }

    /// Adds a log request to the query builder
    ///
    /// # Examples
    ///
    /// no_run
    /// let log_request = LogRequest {
    ///     address: Some(vec!["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string()]),
    ///     topic0: Some(vec!["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string()]),
    ///     ..Default::default()
    /// };
    /// query_builder.add_log(log_request);
    ///
    pub fn add_log(&mut self, log_request: LogRequest) -> &mut Self {
        let mut log = Map::new();
        if let Some(address) = log_request.address {
            log.insert("address".to_string(), json!(address));
        }
        if let Some(topic0) = log_request.topic0 {
            log.insert("topic0".to_string(), json!(topic0));
        }
        if let Some(topic1) = log_request.topic1 {
            log.insert("topic1".to_string(), json!(topic1));
        }
        if let Some(topic2) = log_request.topic2 {
            log.insert("topic2".to_string(), json!(topic2));
        }
        if let Some(topic3) = log_request.topic3 {
            log.insert("topic3".to_string(), json!(topic3));
        }
        self.logs.push(json!(log));
        self
    }

    /// Adds a transaction request to the query builder
    ///
    /// # Examples
    ///
    ///
    /// let tx_request = TransactionRequest {
    ///     from: Some(vec!["0xabc".to_string()]),
    ///     to: Some(vec!["0xdef".to_string()]),
    ///     sighash: Some(vec!["0x123".to_string()]),
    /// };
    /// query_builder.add_transaction(tx_request);
    ///
    pub fn add_transaction(&mut self, transaction_request: TransactionRequest) -> &mut Self {
        let mut transaction = Map::new();
        if let Some(from) = transaction_request.from {
            transaction.insert("from".to_string(), json!(from));
        }
        if let Some(to) = transaction_request.to {
            transaction.insert("to".to_string(), json!(to));
        }
        if let Some(sighash) = transaction_request.sighash {
            transaction.insert("sighash".to_string(), json!(sighash));
        }
        self.transactions.push(json!(transaction));
        self
    }

    /// Adds a block request to the query builder
    ///
    /// # Examples
    ///
    ///
    /// let block_request = BlockRequest {
    ///     block_number: 6082465,
    /// };
    /// query_builder.add_block(block_request);
    ///
    pub fn add_block(&mut self, block_request: BlockRequest) -> &mut Self {
        let mut block = Map::new();
        block.insert(
            "block_number".to_string(),
            json!(block_request.block_number),
        );
        self.blocks.push(json!(block));
        self
    }

    /// Specifies which fields to select in log queries
    ///
    /// # Examples
    ///
    /// no_run
    ///# use my_crate::{QueryBuilder, LogFields};

    /// let log_fields = LogFields {
    ///     address: true,
    ///     topics: true,
    ///     data: true,
    ///     ..Default::default()
    /// };
    /// query_builder.select_log_fields(log_fields);
    ///
    pub fn select_log_fields(&mut self, log_fields: LogFields) -> &mut Self {
        let mut log_select = Map::new();

        // if log_fields.id {
        //     log_select.insert("id".to_string(), json!(true));
        // }
        if log_fields.log_index {
            log_select.insert("logIndex".to_string(), json!(true));
        }
        if log_fields.transaction_index {
            log_select.insert("transaction_index".to_string(), json!(true));
        }
        if log_fields.block {
            log_select.insert("block".to_string(), json!(true));
        }
        if log_fields.address {
            log_select.insert("address".to_string(), json!(true));
        }
        if log_fields.data {
            log_select.insert("data".to_string(), json!(true));
        }
        if log_fields.topics {
            log_select.insert("topics".to_string(), json!(true));
        }
        if log_fields.transaction_hash {
            log_select.insert("transactionHash".to_string(), json!(true));
        }

        if !log_select.is_empty() {
            let mut select = self.select.take().unwrap_or_default();
            select.insert("log".to_string(), json!(log_select));
            self.select = Some(select);
        }

        self
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::{Datasource, DatasourceConfig};

    use super::*;
    use serde_json::json;
    use serde_json_diff;
    #[tokio::test]
    async fn test_query_builder() {
        let mut query_builder = QueryBuilder::new();

        // Add log request
        let log_request = LogRequest {
            address: Some(vec![
                "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string()
            ]),
            topic0: Some(vec![
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string(),
            ]),
            ..Default::default()
        };

        let log_request2 = LogRequest {
            address: Some(vec!["0x".to_string()]),

            ..Default::default()
        };

        // Add transaction request
        let tx_request = TransactionRequest {
            from: Some(vec!["0xabc".to_string()]),
            to: Some(vec!["0xdef".to_string()]),
            sighash: Some(vec!["0x123".to_string()]),
        };
        // Add block request
        let block_request = BlockRequest {
            block_number: 6082465,
        };

        // Select log fields
        let log_fields = LogFields {
            address: true,
            topics: true,
            data: true,
            ..Default::default()
        };
        query_builder
            .select_log_fields(log_fields)
            .add_log(log_request);
        //.add_log(log_request2)
        //.add_transaction(tx_request);
        // .add_transaction(tx_request);
        //.add_block(block_request);

        // Final query
        let query = query_builder.build();

        let good_query = json!({"logs": [
              {
                "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
                "topic0": [
                  "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                ]

              }
            ],
            "fields": {

              "log": {
                "address": true,
                "topics": true,
                "data": true
              }

            },
            //"includeAllBlocks": true,
        });
        println!("{:?}", query);
        assert_eq!(query, good_query);
    }
}
