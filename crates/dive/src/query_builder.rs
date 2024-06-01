use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// QueryBuilder struct to build complex queries for logs, transactions, and blocks
#[derive(Default)]
pub struct QueryBuilder {
    select: Option<Map<String, Value>>,
    logs: Vec<Value>,
    transactions: Vec<Value>,
    blocks: Vec<Value>,
    traces: Vec<Value>,
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
#[derive(Serialize, Deserialize, Default)]
pub struct TraceRequest {
    pub type_: Option<Vec<String>>,
    pub create_from: Option<Vec<String>>,
    pub call_to: Option<Vec<String>>,
    pub call_from: Option<Vec<String>>,
    pub call_sighash: Option<Vec<String>>,
    pub suicide_refund_address: Option<Vec<String>>,
    pub reward_author: Option<Vec<String>>,
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
#[derive(Serialize, Deserialize, Default)]
pub struct TransactionFields {
    pub id: bool,
    pub transaction_index: bool,
    pub from: bool,
    pub to: bool,
    pub hash: bool,
    pub gas: bool,
    pub gas_price: bool,
    pub max_fee_per_gas: bool,
    pub max_priority_fee_per_gas: bool,
    pub input: bool,
    pub nonce: bool,
    pub value: bool,
    pub v: bool,
    pub r: bool,
    pub s: bool,
    pub y_parity: bool,
    pub chain_id: bool,
    pub gas_used: bool,
    pub cumulative_gas_used: bool,
    pub effective_gas_price: bool,
    pub contract_address: bool,
    pub type_: bool,
    pub status: bool,
    pub sighash: bool,
}
#[derive(Serialize, Deserialize, Default)]
pub struct TraceFields {
    pub transaction_index: bool,
    pub trace_address: bool,
    pub subtraces: bool,
    pub error: bool,
    pub revert_reason: bool,
    pub type_: bool,
    pub from: bool,
    pub value: bool,
    pub gas: bool,
    pub init: bool,
    pub gas_used: bool,
    pub result_code: bool,
    pub result_address: bool,
    pub call_type: bool,
    pub input: bool,
    pub sighash: bool,
    pub output: bool,
    pub address: bool,
    pub refund_address: bool,
    pub author: bool,
    pub balance: bool,
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
        if !self.traces.is_empty() {
            query.insert("traces".to_string(), json!(self.traces));
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

    pub fn add_trace(&mut self, trace_request: TraceRequest) -> &mut Self {
        let mut trace = Map::new();
        if let Some(type_) = trace_request.type_ {
            trace.insert("type".to_string(), json!(type_));
        }
        if let Some(create_from) = trace_request.create_from {
            trace.insert("create_from".to_string(), json!(create_from));
        }
        if let Some(call_to) = trace_request.call_to {
            trace.insert("call_to".to_string(), json!(call_to));
        }
        if let Some(call_from) = trace_request.call_from {
            trace.insert("call_from".to_string(), json!(call_from));
        }
        if let Some(call_sighash) = trace_request.call_sighash {
            trace.insert("call_sighash".to_string(), json!(call_sighash));
        }
        if let Some(suicide_refund_address) = trace_request.suicide_refund_address {
            trace.insert(
                "suicide_refund_address".to_string(),
                json!(suicide_refund_address),
            );
        }
        if let Some(author) = trace_request.reward_author {
            trace.insert("author".to_string(), json!(author));
        }
        self.traces.push(json!(trace));
        self
    }

    pub fn select_tx_fields(&mut self, tx_fields: TransactionFields) -> &mut Self {
        let mut tx_select = Map::new();

        if tx_fields.id {
            tx_select.insert("id".to_string(), json!(true));
        }
        if tx_fields.transaction_index {
            tx_select.insert("transaction_index".to_string(), json!(true));
        }
        if tx_fields.from {
            tx_select.insert("from".to_string(), json!(true));
        }
        if tx_fields.to {
            tx_select.insert("to".to_string(), json!(true));
        }
        if tx_fields.hash {
            tx_select.insert("hash".to_string(), json!(true));
        }
        if tx_fields.gas {
            tx_select.insert("gas".to_string(), json!(true));
        }
        if tx_fields.gas_price {
            tx_select.insert("gas_price".to_string(), json!(true));
        }
        if tx_fields.max_fee_per_gas {
            tx_select.insert("max_fee_per_gas".to_string(), json!(true));
        }
        if tx_fields.max_priority_fee_per_gas {
            tx_select.insert("max_priority_fee_per_gas".to_string(), json!(true));
        }
        if tx_fields.input {
            tx_select.insert("input".to_string(), json!(true));
        }
        if tx_fields.nonce {
            tx_select.insert("nonce".to_string(), json!(true));
        }
        if tx_fields.value {
            tx_select.insert("value".to_string(), json!(true));
        }
        if tx_fields.v {
            tx_select.insert("v".to_string(), json!(true));
        }
        if tx_fields.r {
            tx_select.insert("r".to_string(), json!(true));
        }
        if tx_fields.s {
            tx_select.insert("s".to_string(), json!(true));
        }
        if tx_fields.y_parity {
            tx_select.insert("y_parity".to_string(), json!(true));
        }
        if tx_fields.chain_id {
            tx_select.insert("chain_id".to_string(), json!(true));
        }
        if tx_fields.gas_used {
            tx_select.insert("gas_used".to_string(), json!(true));
        }
        if tx_fields.cumulative_gas_used {
            tx_select.insert("cumulative_gas_used".to_string(), json!(true));
        }
        if tx_fields.effective_gas_price {
            tx_select.insert("effective_gas_price".to_string(), json!(true));
        }
        if tx_fields.contract_address {
            tx_select.insert("contract_address".to_string(), json!(true));
        }
        if tx_fields.type_ {
            tx_select.insert("type".to_string(), json!(true));
        }
        if tx_fields.status {
            tx_select.insert("status".to_string(), json!(true));
        }
        if tx_fields.sighash {
            tx_select.insert("sighash".to_string(), json!(true));
        }

        if !tx_select.is_empty() {
            let mut select = self.select.take().unwrap_or_default();
            select.insert("transaction".to_string(), json!(tx_select));
            self.select = Some(select);
        }
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

    pub fn select_trace_fields(&mut self, trace_fields: TraceFields) -> &mut Self {
        let mut trace_select = Map::new();

        if trace_fields.transaction_index {
            trace_select.insert("transactionIndex".to_string(), json!(true));
        }
        if trace_fields.trace_address {
            trace_select.insert("traceAddress".to_string(), json!(true));
        }
        if trace_fields.subtraces {
            trace_select.insert("subtraces".to_string(), json!(true));
        }
        if trace_fields.error {
            trace_select.insert("error".to_string(), json!(true));
        }
        if trace_fields.revert_reason {
            trace_select.insert("revertReason".to_string(), json!(true));
        }
        if trace_fields.type_ {
            trace_select.insert("type".to_string(), json!(true));
        }
        if trace_fields.from {
            trace_select.insert("from".to_string(), json!(true));
        }
        if trace_fields.value {
            trace_select.insert("value".to_string(), json!(true));
        }
        if trace_fields.gas {
            trace_select.insert("gas".to_string(), json!(true));
        }
        if trace_fields.init {
            trace_select.insert("init".to_string(), json!(true));
        }
        if trace_fields.gas_used {
            //add all gas fields for now, only one will not be empty anyway
            trace_select.insert("createResultGasUsed".to_string(), json!(true));
            trace_select.insert("callResultGasUsed".to_string(), json!(true));
            //trace_select.insert("gasUsed".to_string(), json!(true));
        }
        if trace_fields.result_code {
            trace_select.insert("resultCode".to_string(), json!(true));
        }
        if trace_fields.result_address {
            trace_select.insert("resultAddress".to_string(), json!(true));
        }
        if trace_fields.call_type {
            trace_select.insert("callType".to_string(), json!(true));
        }
        if trace_fields.input {
            trace_select.insert("input".to_string(), json!(true));
        }
        if trace_fields.sighash {
            trace_select.insert("sighash".to_string(), json!(true));
        }
        if trace_fields.output {
            trace_select.insert("output".to_string(), json!(true));
        }
        if trace_fields.address {
            trace_select.insert("address".to_string(), json!(true));
        }
        if trace_fields.refund_address {
            trace_select.insert("refundAddress".to_string(), json!(true));
        }
        if trace_fields.author {
            trace_select.insert("rewardAuthor".to_string(), json!(true));
        }
        if trace_fields.balance {
            trace_select.insert("balance".to_string(), json!(true));
        }
        if !trace_select.is_empty() {
            let mut select = self.select.take().unwrap_or_default();
            select.insert("trace".to_string(), json!(trace_select));
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

        let trace_request = TraceRequest {
            type_: Some(vec!["call".to_string()]),
            ..Default::default()
        };

        let trace_fields = TraceFields {
            transaction_index: true,
            trace_address: true,
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
            .add_log(log_request)
            .add_trace(trace_request)
            .select_trace_fields(trace_fields);

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
        println!("trace {:?}", query);
        assert_eq!(query, good_query);
    }
}
