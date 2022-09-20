// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};

pub const MAX_CONNECTION_WINDOW_SIZE: u32 = (1 << 31) - 1;

pub fn load_config<S>(path: &str) -> Result<S>
where
    for<'a> S: Deserialize<'a> + Default,
{
    if path.is_empty() {
        tracing::warn!("risingwave.toml not found, using default config.");
        return Ok(S::default());
    }
    let config_str = fs::read_to_string(PathBuf::from(path.to_owned())).map_err(|e| {
        RwError::from(InternalError(format!(
            "failed to open config file '{}': {}",
            path, e
        )))
    })?;
    let config: S = toml::from_str(config_str.as_str())
        .map_err(|e| RwError::from(InternalError(format!("parse error {}", e))))?;
    Ok(config)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    /// The interval for periodic heartbeat from worker to the meta service.
    #[serde(default = "default::heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u32,

    #[serde(default = "default::connection_pool_size")]
    pub connection_pool_size: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchConfig {
    /// Not used.
    #[cfg(any())]
    #[serde(default = "default::chunk_size")]
    pub chunk_size: u32,

    /// The thread number of the batch task runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub worker_threads_num: Option<usize>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamingConfig {
    /// Not used.
    #[cfg(any())]
    #[serde(default = "default::chunk_size")]
    pub chunk_size: u32,

    /// The interval of periodic checkpointing.
    #[serde(default = "default::checkpoint_interval_ms")]
    pub checkpoint_interval_ms: u32,

    /// The maximum number of barriers in-flight in the compute nodes.
    #[serde(default = "default::in_flight_barrier_nums")]
    pub in_flight_barrier_nums: usize,

    /// Whether to enable the minimal scheduling strategy, that is, only schedule the streaming
    /// fragment on one parallel unit per compute node.
    #[serde(default)]
    pub minimal_scheduling: bool,

    /// The parallelism that the compute node will register to the scheduler of the meta service.
    #[serde(default = "default::worker_node_parallelism")]
    pub worker_node_parallelism: usize,

    /// The thread number of the streaming actor runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub actor_runtime_worker_threads_num: Option<usize>,

    #[serde(default)]
    pub developer: DeveloperConfig,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// Currently all configurations are server before they can be specified with DDL syntaxes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Target size of the Sstable.
    #[serde(default = "default::sst_size_mb")]
    pub sstable_size_mb: u32,

    /// Size of each block in bytes in SST.
    #[serde(default = "default::block_size_kb")]
    pub block_size_kb: u32,

    /// False positive probability of bloom filter.
    #[serde(default = "default::bloom_false_positive")]
    pub bloom_false_positive: f64,

    /// parallelism while syncing share buffers into L0 SST. Should NOT be 0.
    #[serde(default = "default::share_buffers_sync_parallelism")]
    pub share_buffers_sync_parallelism: u32,

    /// Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
    /// tokio's default value (number of CPU core).
    #[serde(default = "default::share_buffer_compaction_worker_threads_number")]
    pub share_buffer_compaction_worker_threads_number: u32,

    /// Maximum shared buffer size, writes attempting to exceed the capacity will stall until there
    /// is enough space.
    #[serde(default = "default::shared_buffer_capacity_mb")]
    pub shared_buffer_capacity_mb: u32,

    /// Remote directory for storing data and metadata objects.
    #[serde(default = "default::data_directory")]
    pub data_directory: String,

    /// Whether to enable write conflict detection
    #[serde(default = "default::write_conflict_detection_enabled")]
    pub write_conflict_detection_enabled: bool,

    /// Capacity of sstable block cache.
    #[serde(default = "default::block_cache_capacity_mb")]
    pub block_cache_capacity_mb: usize,

    /// Capacity of sstable meta cache.
    #[serde(default = "default::meta_cache_capacity_mb")]
    pub meta_cache_capacity_mb: usize,

    #[serde(default = "default::disable_remote_compactor")]
    pub disable_remote_compactor: bool,

    #[serde(default = "default::enable_local_spill")]
    pub enable_local_spill: bool,

    /// Local object store root. We should call `get_local_object_store` to get the object store.
    #[serde(default = "default::local_object_store")]
    pub local_object_store: String,

    /// Number of tasks shared buffer can upload in parallel.
    #[serde(default = "default::share_buffer_upload_concurrency")]
    pub share_buffer_upload_concurrency: usize,

    /// Capacity of sstable meta cache.
    #[serde(default = "default::compactor_memory_limit_mb")]
    pub compactor_memory_limit_mb: usize,

    /// Number of SST ids fetched from meta per RPC
    #[serde(default = "default::sstable_id_remote_fetch_number")]
    pub sstable_id_remote_fetch_number: u32,

    #[serde(default)]
    pub file_cache: FileCacheConfig,

    /// Whether to enable streaming upload for sstable.
    #[serde(default = "default::min_sst_size_for_streaming_upload")]
    pub min_sst_size_for_streaming_upload: u64,

    /// Max sub compaction task numbers
    #[serde(default = "default::max_sub_compaction")]
    pub max_sub_compaction: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCacheConfig {
    #[serde(default = "default::file_cache_capacity_mb")]
    pub capacity_mb: usize,

    #[serde(default = "default::file_cache_total_buffer_capacity_mb")]
    pub total_buffer_capacity_mb: usize,

    #[serde(default = "default::file_cache_cache_file_fallocate_unit_mb")]
    pub cache_file_fallocate_unit_mb: usize,

    #[serde(default = "default::file_cache_cache_meta_fallocate_unit_mb")]
    pub cache_meta_fallocate_unit_mb: usize,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeveloperConfig {
    /// Set to true to enable per-executor row count metrics. This will produce a lot of timeseries
    /// and might affect the prometheus performance. If you only need actor input and output
    /// rows data, see `stream_actor_in_record_cnt` and `stream_actor_out_record_cnt` instead.
    #[serde(default = "default::developer_enable_executor_row_count")]
    pub enable_executor_row_count: bool,
}

impl Default for DeveloperConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

mod default {

    pub fn heartbeat_interval_ms() -> u32 {
        1000
    }

    pub fn connection_pool_size() -> u16 {
        16
    }

    #[expect(dead_code)]
    pub fn chunk_size() -> u32 {
        1024
    }

    pub fn sst_size_mb() -> u32 {
        256
    }

    pub fn block_size_kb() -> u32 {
        1024
    }

    pub fn bloom_false_positive() -> f64 {
        0.01
    }

    pub fn share_buffers_sync_parallelism() -> u32 {
        1
    }

    pub fn share_buffer_compaction_worker_threads_number() -> u32 {
        4
    }

    pub fn shared_buffer_capacity_mb() -> u32 {
        1024
    }

    pub fn data_directory() -> String {
        "hummock_001".to_string()
    }

    pub fn write_conflict_detection_enabled() -> bool {
        cfg!(debug_assertions)
    }

    pub fn block_cache_capacity_mb() -> usize {
        256
    }

    pub fn meta_cache_capacity_mb() -> usize {
        64
    }

    pub fn disable_remote_compactor() -> bool {
        false
    }

    pub fn enable_local_spill() -> bool {
        true
    }

    pub fn local_object_store() -> String {
        "tempdisk".to_string()
    }

    pub fn checkpoint_interval_ms() -> u32 {
        250
    }

    pub fn in_flight_barrier_nums() -> usize {
        40
    }

    pub fn share_buffer_upload_concurrency() -> usize {
        8
    }

    pub fn worker_node_parallelism() -> usize {
        std::thread::available_parallelism().unwrap().get()
    }

    pub fn compactor_memory_limit_mb() -> usize {
        512
    }

    pub fn sstable_id_remote_fetch_number() -> u32 {
        10
    }

    pub fn file_cache_capacity_mb() -> usize {
        1024
    }

    pub fn file_cache_total_buffer_capacity_mb() -> usize {
        128
    }

    pub fn file_cache_cache_file_fallocate_unit_mb() -> usize {
        512
    }

    pub fn file_cache_cache_meta_fallocate_unit_mb() -> usize {
        16
    }

    pub fn min_sst_size_for_streaming_upload() -> u64 {
        // 32MB
        32 * 1024 * 1024
    }

    pub fn max_sub_compaction() -> u32 {
        4
    }

    pub fn developer_enable_executor_row_count() -> bool {
        false
    }
}

pub mod constant {
    pub mod hummock {
        use bitflags::bitflags;
        bitflags! {

            #[derive(Default)]
            pub struct CompactionFilterFlag: u32 {
                const NONE = 0b00000000;
                const STATE_CLEAN = 0b00000010;
                const TTL = 0b00000100;
            }
        }

        impl From<CompactionFilterFlag> for u32 {
            fn from(flag: CompactionFilterFlag) -> Self {
                flag.bits()
            }
        }

        pub const TABLE_OPTION_DUMMY_RETENTION_SECOND: u32 = 0;
        pub const PROPERTIES_RETENTION_SECOND_KEY: &str = "retention_seconds";
    }
}
