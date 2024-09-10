use pyo3::prelude::*;
use std::sync::Arc;
use subxt::{
    backend::{legacy::LegacyBackend, rpc::RpcClient},
    utils::{AccountId32, H256},
    OnlineClient, PolkadotConfig,
};

#[subxt::subxt(runtime_metadata_path = "./metadata.scale")]
pub mod subtensor {}

async fn get_api() -> OnlineClient<PolkadotConfig> {
    let url = std::env::var("SUBSTRATE_ARCHIVE_NODE_URL").unwrap();
    let client = RpcClient::from_insecure_url(url.clone())
        .await
        .expect("Failed to connect to node");
    let backend = LegacyBackend::<PolkadotConfig>::builder()
        .storage_page_size(1000)
        .build(client);
    OnlineClient::from_backend(Arc::new(backend)).await.unwrap()
}

async fn query_block_stakes_inner(
    block_hash: String,
) -> PyResult<Vec<(String, Vec<(String, u64)>)>> {
    let api = get_api().await;
    let block_hash = hex::decode(block_hash.trim_start_matches("0x")).expect("Decoding failed");
    let block_hash = H256::from_slice(&block_hash);
    let query = subtensor::storage().subtensor_module().stake_iter();
    let mut iter = api
        .storage()
        .at(block_hash.clone())
        .iter(query)
        .await
        .unwrap();
    let mut kvs: Vec<(String, Vec<(String, u64)>)> = Vec::new();

    while let Some(Ok(kv)) = iter.next().await {
        let coldkey_bytes: [u8; 32] = kv.key_bytes[kv.key_bytes.len() - 32..].try_into().unwrap();
        let hotkey_bytes: [u8; 32] = kv.key_bytes[kv.key_bytes.len() - 64..kv.key_bytes.len() - 32].try_into().unwrap();
        let coldkey = AccountId32::from(coldkey_bytes).to_string();
        let hotkey = AccountId32::from(hotkey_bytes).to_string();
        kvs.push((coldkey, vec![(hotkey, kv.value)]));
    }

    Ok(kvs)
}

#[pyfunction]
fn query_block_stakes(
    block_hash: String,
) -> PyResult<Vec<(String, Vec<(String, u64)>)>> {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(query_block_stakes_inner(block_hash))
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(query_block_stakes, m)?)?;
    Ok(())
}
