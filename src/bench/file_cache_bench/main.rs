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

#[cfg(target_os = "linux")]
mod analyze;
#[cfg(target_os = "linux")]
mod bench;
#[cfg(target_os = "linux")]
mod rate;
#[cfg(target_os = "linux")]
mod utils;

#[tokio::main]
async fn main() {
    if !cfg!(target_os = "linux") {
        panic!("only support linux")
    }

    use isahc::config::Configurable;
    use tracing_subscriber::prelude::*;

    opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

    let filter = tracing_subscriber::filter::Targets::new()
        .with_target(
            "risingwave_storage::hummock::file_cache",
            tracing::Level::TRACE,
        )
        .with_target("risingwave_bench::file_cache_bench", tracing::Level::TRACE)
        .with_default(tracing::Level::INFO);
    let fmt_layer = tracing_subscriber::fmt::layer()
        // .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        // .with_target(true)
        .with_filter(filter);
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("file-cache-bench")
        .with_collector_endpoint("http://127.0.0.1:14268/api/traces")
        .with_http_client(isahc::HttpClient::builder().proxy(None).build().unwrap())
        .install_batch(opentelemetry::runtime::Tokio)
        .unwrap();
    let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(opentelemetry_layer)
        .init();

    #[cfg(target_os = "linux")]
    bench::run().await;

    opentelemetry::global::shutdown_tracer_provider();
}
