# Caching Layer

## How it Works
To further reduce latency for metadata-heavy operations, `gcs-analytics-core` provides a configurable multi-tier caching layer supporting both **in-memory executor-level caching (Caffeine)** and **worker-level local storage caching (SSD/File-based)**.

When multiple tasks or queries running across executor JVM processes on the same worker node attempt to access the same metadata objects, the cache ensures that only the first request incurs a network round-trip.

*   **Small Object Cache**: Caches the entirety of very small objects. This is effective for larger datasets with small fact tables or a few hot small files. For example, allocating a 500 MB small object cache on a 30-node cluster yields up to a 5% scan time improvement for the TPCDS 10TB benchmark in Apache Iceberg.
*   **Footer Cache**: Caches the prefetched footers of larger files (like Parquet). If multiple readers need to parse the same file's schema, it can be served instantly from memory or local SSD.
*   **Worker-Level File Cache (SSD)**: When enabled, persists cached objects to a shared local directory (such as a local NVMe SSD). This allows multiple executor processes on the same physical worker VM to share cached data, and preserves cached footers across short-lived `GcsFileSystem` lifecycles.

### Cache Flow Diagram

```mermaid
sequenceDiagram
    participant App as Query Engine
    participant Stream as GoogleCloudStorageInputStream
    participant OptLayer as Optimizer Layer (e.g. Footer Optimizer)
    participant Cache as GcsCacheManager
    participant SSD as Worker SSD File Cache
    participant GCS as Google Cloud Storage

    App->>Stream: read() / readVectored()
    Stream->>OptLayer: Intercept read request
    OptLayer->>Cache: get(itemId)

    alt L1 In-Memory Cache Hit
        Note over Cache, OptLayer: Instantly served from JVM RAM!
        Cache-->>OptLayer: Return Object
        OptLayer-->>Stream: Fulfill read from buffer
        Stream-->>App: Return Data (No I/O)
    else L2 Worker File Cache Hit
        Cache->>SSD: Lock-Free Read & Touch mtime
        SSD-->>Cache: Return File Bytes
        Note over Cache: Stores Object in L1 Memory
        Cache-->>OptLayer: Return Object
        OptLayer-->>Stream: Fulfill read from buffer
        Stream-->>App: Return Data
    else Full Cache Miss
        Cache->>GCS: HTTP GET Object
        GCS-->>Cache: Return Object
        Note over Cache: Atomically Writes to L2 SSD & Stores in L1
        Cache-->>OptLayer: Return Object
        OptLayer-->>Stream: Fulfill read from buffer
        Stream-->>App: Return Data
    end
```

## Internal Implementation Details

The caching layer is implemented using a pluggable, generic cache interface to ensure thread-safety and atomic operations across concurrent reads.

*   **[`AnalyticsCacheManager`](../../client/src/main/java/com/google/cloud/gcs/analyticscore/client/AnalyticsCacheManager.java)**: A thread-safe registry that initializes and holds the specialized caches (footer cache and small object cache).
*   **[`AnalyticsCache`](../../common/src/main/java/com/google/cloud/gcs/analyticscore/common/cache/AnalyticsCache.java)**: The base interface defining generic cache operations.
*   **[`AnalyticsCacheCaffeineImpl`](../../common/src/main/java/com/google/cloud/gcs/analyticscore/common/cache/AnalyticsCacheCaffeineImpl.java)**: The in-memory L1 cache backed by Caffeine.
*   **[`AnalyticsCacheFileImpl`](../../common/src/main/java/com/google/cloud/gcs/analyticscore/common/cache/AnalyticsCacheFileImpl.java)**: The worker-level L2 file cache on local storage (SSD). Features lock-free reads, throttled access timestamp updates, atomic file rename writes, and non-blocking watermark eviction.
*   **[`AnalyticsCacheHybridImpl`](../../common/src/main/java/com/google/cloud/gcs/analyticscore/common/cache/AnalyticsCacheHybridImpl.java)**: A 2-tier hybrid cache combining L1 memory and L2 file storage.
*   **[`AnalyticsCacheNoOpImpl`](../../common/src/main/java/com/google/cloud/gcs/analyticscore/common/cache/AnalyticsCacheNoOpImpl.java)**: A singleton, no-op implementation used when a specific cache is disabled.

## Configuration Knobs

The caching subsystem is configured via [`GcsCacheOptions`](../../client/src/main/java/com/google/cloud/gcs/analyticscore/client/GcsCacheOptions.java):

**Small Object Caching:**
*   `analytics-core.small-file.cache.enabled`: Controls whether small object caching is enabled (Default: `false`).
*   `analytics-core.small-file.cache.max-size-bytes`: The maximum capacity of the small object in-memory cache (Default: `1073741824` i.e., 1 GB).

**Footer Caching:**
*   `analytics-core.footer.cache.enabled`: Controls whether the Parquet footer cache is enabled (Default: `false`).
*   `analytics-core.footer.cache.max-size-bytes`: The maximum capacity of the footer in-memory cache (Default: `1073741824` i.e., 1 GB).

**Worker-Level File Caching (Shared SSD):**
*   `analytics-core.worker.cache.enabled`: Controls whether worker-level local file caching is enabled across executors (Default: `false`).
*   `analytics-core.worker.cache.directory`: The root directory on local disk/SSD to store cached objects (Default: `/tmp/gcs-analytics-cache`).
*   `analytics-core.worker.cache.max-size-bytes`: The maximum disk capacity for the worker-level file cache (Default: `10737418240` i.e., 10 GB).
