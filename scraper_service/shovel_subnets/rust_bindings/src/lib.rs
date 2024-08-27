use std::sync::Arc;

use pyo3::prelude::*;
use subxt::{
    backend::{legacy::LegacyBackend, rpc::RpcClient},
    utils::H256,
    OnlineClient, PolkadotConfig,
};
use tokio;

#[subxt::subxt(runtime_metadata_path = "./metadata.scale")]
pub mod subtensor {}

/// Formats the sum of two numbers as string.
async fn query_inner(block_hash: String) -> PyResult<Vec<(String, u64)>> {
    let start = std::time::Instant::now();
    let url = std::env::var("SUBSTRATE_ARCHIVE_NODE_URL").unwrap();

    let client = RpcClient::from_insecure_url(url)
        .await
        .expect("Failed to connect to node");
    let backend = LegacyBackend::<PolkadotConfig>::builder()
        .storage_page_size(1000)
        .build(client);
    let api = OnlineClient::from_backend(Arc::new(backend)).await.unwrap();

    let stake_query = subtensor::storage().subtensor_module().stake_iter();
    let block_hash = hex::decode(block_hash.trim_start_matches("0x")).expect("Decoding failed");
    let block_hash = H256::from_slice(&block_hash);

    let mut iter = api
        .storage()
        .at(block_hash)
        .iter(stake_query)
        .await
        .unwrap();

    let mut kvs = vec![];
    while let Some(Ok(kv)) = iter.next().await {
        let key = hex::encode(&kv.key_bytes);
        kvs.push((key, kv.value));
    }
    let end = std::time::Instant::now();
    println!("Query took: {:?}", end - start);
    Ok(kvs)
}

#[pyfunction]
fn query(block_hash: String) -> PyResult<Vec<(String, u64)>> {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(query_inner(block_hash))
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(query, m)?)?;
    Ok(())
}
