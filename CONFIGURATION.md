# Configuration

This document outlines the key configuration properties for the GCS Analytics Core library.

## Configuration Properties

All configuration properties can be prefixed with a common string, e.g., `gcs.`. This prefix is not included in the tables below.

### General GCS Client Options

These properties govern the core connections, identity, and access parameters between your compute environment and Google Cloud Storage.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `client-lib-token` | Client library token. | - |
| `service.host` | The GCS service host. | - |
| `user-agent` | The user agent string. | - |
| `project-id` | The Google Cloud project ID for the GCS client. | - |
| `user-project` | Project ID whose Google Cloud Project's billing account should be charged for the operation being executed. | - |
| `decryption-key` | Decryption key for the object. | - |

### Caching and Prefetching

These settings control how aggressively the library prefetches and caches metadata (like Parquet footers) and small objects in memory. Proper configuration here significantly reduces latency and redundant network calls during metadata discovery phases.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `analytics-core.footer.prefetch.enabled` | Controls whether footer prefetching is enabled. | `true` |
| `analytics-core.small-file.footer.prefetch.size-bytes` | Footer prefetch size (in bytes) for files up to 1 GB. | `51200` (50 KB) |
| `analytics-core.large-file.footer.prefetch.size-bytes` | Footer prefetch size (in bytes) for files larger than 1 GB. | `1048576` (1 MB) |
| `analytics-core.footer.cache.enabled` | Controls whether the Parquet footer cache is enabled. | `false` |
| `analytics-core.footer.cache.max-size-bytes`                 | The maximum capacity (in bytes) to hold in the Parquet footer cache.                        | `104857600` (100 MB) |
| `analytics-core.small-file.cache.threshold-bytes` | Threshold (in bytes) below which small files are cached entirely. | `1048576` (1 MB) |
| `analytics-core.small-file.cache.enabled` | Controls whether the small object cache is enabled. | `false` |
| `analytics-core.small-file.cache.max-size-bytes` | The maximum capacity (in bytes) to hold in the small object cache. | `209715200` (200 MB) |

### Predictive Prefetching

These settings control the predictive prefetcher, which learns the column access pattern of a query and speculatively fetches the bytes the engine is about to request. Blocks are held in an in-memory buffer cache that is bounded by total size and expires after a period of inactivity.

| Property | Type | Description | Default Value |
| :--- | :--- | :--- | :--- |
| `analytics-core.prefetch.mode` | Enum | Predictive prefetching strategy. Supported values: `PREDICTIVE_ROW_GROUP`, `DISABLED`. Values are case-insensitive and hyphens are accepted (e.g. `predictive-row-group`). | `DISABLED` |
| `analytics-core.prefetch.buffer.cache.max-size-bytes` | Long | The maximum total capacity (in bytes) of the prefetch buffer cache. | `2147483648` (2 GB) |
| `analytics-core.prefetch.buffer.cache.ttl-seconds` | Long | How long (in seconds) a prefetched block is retained in the buffer cache after it was last read or written. | `60` |
| `analytics-core.prefetch.block.size-bytes` | Integer | The granularity (in bytes) at which the prefetcher requests and caches data. Every speculative request is aligned to this size, and cached bytes are resolved by block index. | `4194304` (4 MB) |
| `analytics-core.prefetch.history.max-columns` | Integer | The maximum number of columns tracked per Parquet schema in the access history. | `15` |
| `analytics-core.prefetch.in-flight-wait-millis` | Long | How long a read waits for a block that a speculative request is already fetching before reading the bytes itself. Beyond roughly one round trip, waiting on a prefetch queued behind a busy thread pool costs more than reading directly. `0` never waits. | `200` |

### Read Performance and I/O Tuning

These parameters fine-tune the low-level data streaming behavior. They allow you to optimize thread concurrency, heuristic file access patterns, and vectored I/O merging to maximize data throughput against GCS.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `channel.read.chunk-size-bytes` | Chunk size for GCS channel reads. | - |
| `analytics-core.read.thread.count` | Number of threads for parallel read operations like vectored IO. | `16` |
| `analytics-core.read.vectored.range.merge-gap.max-bytes` | Maximum gap (in bytes) between ranges to merge in vectored reads. | `4096` (4 KB) |
| `analytics-core.read.vectored.range.merged-size.max-bytes` | Maximum size (in bytes) of a merged range in vectored reads. | `8388608` (8 MB) |
| `analytics-core.read.inplace-seek-limit-bytes` | In-place seek limit (in bytes). | `131072` (128 KB) |
| `analytics-core.read.file-access-pattern` | File access pattern. Supported values: `RANDOM`, `SEQUENTIAL`, `AUTO_SEQUENTIAL`, `AUTO_RANDOM`. | `AUTO_SEQUENTIAL` |
| `analytics-core.adaptive-read.sequential-read-threshold` | Threshold for number of sequential reads to switch to sequential mode. | `3` |
| `analytics-core.random-read.min-request-size` | Minimum request size for random reads. If the requested read size is smaller, it reads up to this size. | `131072` (128 KB) |

### Telemetry and Monitoring

These settings enable the emission of deep internal metrics—such as cache hit rates, operational durations, and throughput—to local logging consoles or distributed OpenTelemetry backends like Google Cloud Monitoring.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `analytics-core.telemetry.logging.enabled` | Controls whether logging telemetry reporter is enabled. | `false` |
| `analytics-core.telemetry.logging.level` | Specifies the log level for logging telemetry events. Supported: `TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`. | `DEBUG` |
| `analytics-core.telemetry.opentelemetry.enabled` | Controls whether OpenTelemetry integration is enabled. | `false` |
| `analytics-core.telemetry.opentelemetry.provider-type` | Specifies the OpenTelemetry provider type. Supported: `GLOBAL`, `LOGGING`, `CLOUD_MONITORING`. | `GLOBAL` |
| `analytics-core.telemetry.opentelemetry.export-interval-seconds` | The export interval in seconds for OpenTelemetry periodic metric readers. | `60` |
| `analytics-core.project-id` | Google Cloud project ID for exporting OpenTelemetry metrics (specifically for `CLOUD_MONITORING`). | - |

## Notes

* **Cloud Monitoring Flush Timeout**: When using `CLOUD_MONITORING` as the OpenTelemetry provider type, closing the last active GCS client instance triggers a telemetry flush. This operation is synchronous and can block the closing thread for up to 10 seconds to ensure all remaining metrics are successfully exported to Google Cloud Monitoring before the application shuts down.
