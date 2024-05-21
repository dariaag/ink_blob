pub mod fields;

//use polars::prelude::*;
use fields::Dataset;

use polars::prelude::{DataFrame, ParquetCompression, ParquetWriter, Series};
use serde_json::Value;
use std::collections::HashMap;

use anyhow::Error;
use fields::{create_columns_from_field_data, create_field_data, FieldData};
use std::fs::{self, File};
use std::path::Path;

pub fn to_df(
    dataset: Dataset,
    json_data: Vec<Value>,
    fields: Vec<&str>,
) -> Result<DataFrame, Error> {
    let data_fields: Vec<(&str, FieldData)> = fields
        .iter()
        .filter_map(|&field| {
            match create_field_data(field, dataset) {
                Ok(field_data) => Some((field, field_data)),
                Err(_) => None, //todo change to anyhow
            }
        })
        .collect();

    let mut field_map: HashMap<String, FieldData> = data_fields
        .into_iter()
        .map(|(name, data)| (name.to_string(), data))
        .collect();
    //put loop inside func, return mutable reference to fieldmap
    field_map = process_json_object(json_data, field_map, &fields, &dataset).unwrap(); //todo change to anyhow
                                                                                       //create series from fields
    let columns: Vec<Series> = create_columns_from_field_data(&field_map, &fields);

    let df = DataFrame::new(columns)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    Ok(df)
}

fn process_json_object(
    json_data: Vec<Value>,
    mut field_map: HashMap<String, FieldData>,
    fields: &[&str],
    dataset: &Dataset,
) -> Result<HashMap<String, FieldData>, Error> {
    for json_obj in json_data {
        match dataset {
            Dataset::Blocks => {
                if let Some(header) = json_obj.get("header") {
                    //check types here TODO
                    fields.iter().for_each(|field| {
                        if let Some(data) = field_map.get_mut(*field) {
                            if let Some(value) = header.get(*field) {
                                if let Err(e) = data.add_value(value) {
                                    eprintln!("Error processing value: {}", e);
                                }
                            }
                        }
                    });
                }
            }
            Dataset::Transactions => {
                if let Some(tx_list) = json_obj.get("transactions") {
                    //check types here TODO
                    fields.iter().for_each(|field| {
                        if let Some(data) = field_map.get_mut(*field) {
                            for tx in tx_list.as_array().unwrap() {
                                if let Some(value) = tx.get(*field) {
                                    if let Err(e) = data.add_value(value) {
                                        eprintln!("Error processing value: {}", e);
                                    }
                                }
                            }
                        }
                    });
                }
            }
            Dataset::Logs => {
                if let Some(log_list) = json_obj.get("logs") {
                    //check types here TODO
                    fields.iter().for_each(|field| {
                        if let Some(data) = field_map.get_mut(*field) {
                            for log in log_list.as_array().unwrap() {
                                if let Some(value) = log.get(*field) {
                                    if let Err(e) = data.add_value(value) {
                                        eprintln!("Error processing value: {}", e);
                                    }
                                }
                            }
                        }
                    });
                }
            } // _ => panic!("Dataset not found"),
        }
    }

    Ok(field_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields::{extract_fields, Dataset}; // Add the missing import for the 'dive' crate
    extern crate dive;
    use dive::datasource::{SubsquidApi, SubsquidApiConfig};
    // Add the missing import for the 'dive' crate
    use serde_json::json;
    use tokio::runtime::Runtime;

    #[test]
    fn test_to_df_blocks() {
        let dataset = Dataset::Blocks;
        let json_data = vec![
            json!({"header": {"number": 1, "hash": "0x1", "timestamp": 1000, "miner": "0xabc"}}),
            json!({"header": {"number": 2, "hash": "0x2", "timestamp": 2000, "miner": "0xdef"}}),
        ];
        let fields = vec!["number", "hash", "timestamp", "miner"];

        let df = to_df(dataset, json_data, fields).unwrap();
        assert_eq!(df.shape().0, 2); // 2 rows
        assert_eq!(df.shape().1, 4); // 4 columns
    }

    #[test]
    fn test_to_df_transactions() {
        let dataset = Dataset::Transactions;
        let json_data = vec![
            json!({"transactions": [{"hash": "0x1", "from": "0xabc", "to": "0xdef", "value": 1000}]}),
            json!({"transactions": [{"hash": "0x2", "from": "0xghi", "to": "0xjkl", "value": 2000}]}),
        ];
        let fields = vec!["hash", "from", "to", "value"];

        let df = to_df(dataset, json_data, fields).unwrap();
        assert_eq!(df.shape().0, 2); // 2 rows
        assert_eq!(df.shape().1, 4); // 4 columns
    }

    #[test]
    fn test_to_df_logs() {
        let dataset = Dataset::Logs;
        let json_data = vec![
            json!({"logs": [{"transactionHash": "0x1", "logIndex": 1, "address": "0xabc", "data": "0xdeadbeef"}]}),
            json!({"logs": [{"transactionHash": "0x2", "logIndex": 2, "address": "0xdef", "data": "0xfeedface"}]}),
        ];
        let fields = vec!["transactionHash", "logIndex", "address", "data"];

        let df = to_df(dataset, json_data, fields).unwrap();
        println!("{:?}", df);
        assert_eq!(df.shape().0, 2); // 2 rows
        assert_eq!(df.shape().1, 4); // 4 columns
    }

    #[tokio::test]
    async fn test_with_archive() {
        let dataset = Dataset::Logs;
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
            //"includeAllBlocks": true,
        });
        const BASE_URL: &str = "https://v2.archive.subsquid.io/network/ethereum-mainnet";
        let config = SubsquidApiConfig::new(BASE_URL.to_string(), 10);
        let api = SubsquidApi::new(config);
        let data = api
            .get_data_in_range(query.clone(), 14000000, 14000010)
            .await
            .unwrap();

        let fields = fields::extract_fields(&query);
        let df = to_df(dataset, data, fields).unwrap();
        println!("{:?}", df);
    }
}
