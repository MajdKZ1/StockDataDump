use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use async_compression::{tokio::write::ZstdEncoder, Level};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use tokio::{
    fs::File,
    io,
    io::{AsyncWriteExt, BufWriter},
    task,
};
use tokio_util::io::StreamReader;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "dump-core",
    version,
    about = "High-throughput HTTP fetcher + zstd compressor for stock data dumps"
)]
struct Cli {
    /// Maximum concurrent HTTP fetches
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Per-request timeout in seconds
    #[arg(long, default_value_t = 15)]
    timeout_secs: u64,

    /// Retry attempts per URL on non-2xx or IO errors
    #[arg(long, default_value_t = 2)]
    retries: u32,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Fetch a single URL and compress to zstd
    Single {
        /// HTTP endpoint to download (e.g. csv/json)
        #[arg(long)]
        url: String,
        /// Output compressed file (ends with .zst)
        #[arg(long)]
        output: PathBuf,
        /// Compression level (-7..=22). Higher = better ratio, slower.
        #[arg(long, default_value_t = 3)]
        level: i32,
    },
    /// Batch fetch using a JSON Lines manifest
    Batch {
        /// NDJSON file with {"symbol": "...", "url": "..."} per line
        #[arg(long)]
        manifest: PathBuf,
        /// Directory for compressed outputs
        #[arg(long)]
        output_dir: PathBuf,
        /// Compression level (-7..=22). Higher = better ratio, slower.
        #[arg(long, default_value_t = 3)]
        level: i32,
    },
}

#[derive(Debug, Deserialize, Clone)]
struct Job {
    symbol: String,
    url: String,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .compact()
        .init();

    let cli = Cli::parse();

    let client = Client::builder()
        .pool_max_idle_per_host(cli.concurrency * 2)
        .tcp_keepalive(Duration::from_secs(30))
        .gzip(true)
        .timeout(Duration::from_secs(cli.timeout_secs))
        .build()
        .context("building HTTP client")?;

    match cli.command {
        Commands::Single { url, output, level } => {
            let job = Job {
                symbol: output
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("job")
                    .to_string(),
                url,
                headers: None,
            };
            fetch_and_write(&client, job, output, level, cli.retries).await?;
        }
        Commands::Batch {
            manifest,
            output_dir,
            level,
        } => {
            let jobs = read_manifest(&manifest)
                .with_context(|| format!("reading manifest {manifest:?}"))?;
            tokio::fs::create_dir_all(&output_dir)
                .await
                .with_context(|| format!("creating output dir {output_dir:?}"))?;

            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(cli.concurrency));
            let mut ok = 0usize;
            let mut failed = 0usize;

            let mut stream = futures::stream::iter(jobs.into_iter().map(|job| {
                let client = client.clone();
                let output_dir = output_dir.clone();
                let semaphore = semaphore.clone();
                async move {
                    let _permit = semaphore.acquire().await.expect("semaphore closed");
                    let output = output_dir.join(format!("{}.zst", job.symbol));
                    fetch_and_write(&client, job.clone(), output, level, cli.retries).await
                }
            }))
            .buffer_unordered(cli.concurrency);

            while let Some(result) = stream.next().await {
                if let Err(err) = result {
                    warn!("job failed: {err:?}");
                    failed += 1;
                } else {
                    ok += 1;
                }
            }

            if failed > 0 {
                error!("batch done: {ok} ok, {failed} failed");
                anyhow::bail!("some jobs failed");
            } else {
                info!("batch done: {ok} ok");
            }
        }
    }

    Ok(())
}

async fn fetch_and_write(
    client: &Client,
    job: Job,
    output: PathBuf,
    level: i32,
    retries: u32,
) -> Result<()> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match fetch_once(client, &job, &output, level).await {
            Ok(_) => {
                info!("✔ {} -> {}", job.symbol, output.display());
                return Ok(());
            }
            Err(err) if attempts <= retries => {
                warn!(
                    "retry {}/{} for {} due to {err:?}",
                    attempts, retries, job.symbol
                );
                tokio::time::sleep(Duration::from_millis(200 * attempts as u64)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn fetch_once(client: &Client, job: &Job, output: &PathBuf, level: i32) -> Result<()> {
    let mut req = client
        .get(&job.url)
        .header(
            reqwest::header::USER_AGENT,
            "stockdatadump/0.1 (https://github.com/your/repo)",
        );
    if let Some(headers) = &job.headers {
        for (k, v) in headers {
            req = req.header(k, v);
        }
    }
    let resp = req.send().await.with_context(|| format!("requesting {}", job.url))?;

    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("non-2xx {} for {}", status, job.url);
    }

    // Wrap streaming body into AsyncRead
    let byte_stream = resp.bytes_stream().map(|res| {
        res.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("http stream error: {e}")))
    });
    let mut reader = StreamReader::new(byte_stream);

    // Use blocking file creation to avoid partial writes on failure
    let parent = output
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    tokio::fs::create_dir_all(&parent)
        .await
        .with_context(|| format!("creating parent dir {parent:?}"))?;

    let file = File::create(output)
        .await
        .with_context(|| format!("creating {}", output.display()))?;
    let writer = BufWriter::new(file);
    let mut encoder = ZstdEncoder::with_quality(writer, Level::Precise(level));

    // Copy bytes through compressor
    io::copy(&mut reader, &mut encoder)
        .await
        .with_context(|| format!("writing {}", output.display()))?;
    encoder
        .shutdown()
        .await
        .context("finalizing compressed stream")?;

    // Force fsync on completion
    let output_clone = output.clone();
    task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&output_clone)?;
        file.sync_all()?;
        Result::<()>::Ok(())
    })
    .await
    .context("fsync join")??;

    Ok(())
}

fn read_manifest(path: &PathBuf) -> Result<Vec<Job>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading manifest {}", path.display()))?;
    let mut jobs = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let job: Job =
            serde_json::from_str(line).with_context(|| format!("parsing line {} in manifest", idx + 1))?;
        jobs.push(job);
    }
    Ok(jobs)
}
