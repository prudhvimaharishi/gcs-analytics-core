# PredictivePrefetchOptimizer Performance Review & Implementation Plan

## 1. Executive Summary

Benchmarking `PredictivePrefetchOptimizer` on TPC-DS datasets currently shows no performance improvement over baseline reads. A deep-dive analysis of `gcs-analytics-core` and its integration with Apache Iceberg (`GCSFileIO`, `PrefixedStorage`, and `GcsInputStreamWrapper`) revealed **7 compounding root causes** that neutralize or degrade prefetch performance.

This document provides the complete root cause analysis and a step-by-step implementation specification for fixing each issue while preserving the existing **fixed-size block caching architecture** (`PrefetchBufferCache`).

---

## 2. Build & Test Environment Requirements

- **Java Version**: Must use **Java 21** (`JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`) because the default system JDK 26 fails `-Werror` on `-source 11`.
- **Run Unit Tests**:
  ```bash
  JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./mvnw test
  ```
- **Apply Formatting (Spotless)**:
  ```bash
  JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./mvnw spotless:apply
  ```

---

## 3. Root Cause Analysis

### Root Cause 1: Iceberg Task Serialization Wipes Out `SchemaAccessHistory` (Cold Start on Every Task)
- **Files Involved**:
  - `client/src/main/java/com/google/cloud/gcs/analyticscore/client/SchemaAccessHistory.java`
  - `client/src/main/java/com/google/cloud/gcs/analyticscore/client/AnalyticsCacheManager.java`
  - `client/src/main/java/com/google/cloud/gcs/analyticscore/client/GcsFileSystemImpl.java` (lines 76–79)
  - Sibling Iceberg repo: `org.apache.iceberg.gcp.gcs.GCSFileIO` & `PrefixedStorage`
- **What Happens**:
  In Apache Iceberg, `GCSFileIO.storageByPrefix` and `PrefixedStorage.gcsFileSystem` are marked `transient`. When distributed query engines (Spark, Trino, Flink) serialize `GCSFileIO` from the driver to executors for every task/split, each deserialized task lazily calls `gcsFileSystemSupplier.get()`, creating a brand-new `GcsFileSystemImpl` instance.
  Because `GcsFileSystemImpl` instantiates a new `AnalyticsCacheManager` (which owns `SchemaAccessHistory`) in its constructor, **every task starts with an empty `SchemaAccessHistory`**.
- **Concrete Example**:
  An executor runs 8 tasks scanning 8 Parquet files from the `customer` table. Because each task creates its own `SchemaAccessHistory`, none of the tasks share learned column projections. Every task suffers a cold start on Row Group 0. Furthermore, many ZSTD-compressed TPC-DS files have only 1 row group, so learning during Row Group 0 never benefits that task at all.

---

### Root Cause 2: Cache Hits Silently Flip `AdaptiveReadStrategy` into Permanent Random Mode (Position Desync)
- **Files Involved**:
  - `core/src/main/java/com/google/cloud/gcs/analyticscore/core/channel/SmartReadChannel.java` (lines 100–102)
  - `client/src/main/java/com/google/cloud/gcs/analyticscore/client/GcsReadChannel.java` (lines 197–203)
  - `client/src/main/java/com/google/cloud/gcs/analyticscore/client/AdaptiveReadStrategy.java` (lines 49–73)
- **What Happens**:
  When `PredictivePrefetchOptimizer` serves a read from `PrefetchBufferCache`, `SmartReadChannel.read` advances the underlying channel position by calling `delegate.position(position + bytesRead)`.
  However, `GcsReadChannel.position(long)` only updates `gcsReadChannelPosition` and **never notifies `strategy` (`AdaptiveReadStrategy`)**.
- **Concrete Example**:
  1. `GcsReadChannel` reads bytes `0..1000` via network (`lastReadEndPosition = 1000`).
  2. `PredictivePrefetchOptimizer` serves bytes `1000..5,000,000` from cache and calls `delegate.position(5_000_000)`.
  3. On the next cache miss at offset `5,000,000`, `AdaptiveReadStrategy.prepareDelegateForRead` checks `newPosition - lastReadEndPosition` (`5,000,000 - 1,000 = 4,999,000`).
  4. Because 4.99 MB exceeds `inplaceSeekLimit` (128 KB), `AdaptiveReadStrategy` assumes a random seek occurred and **permanently switches from `SequentialReadStrategy` to `RandomReadStrategy`** (since default `AUTO_SEQUENTIAL` has no transition back to sequential). All subsequent cache misses degrade to small, bounded HTTP requests.

---

### Root Cause 3: Recording Accessed Columns by 4 MB Block Instead of Requested Range Destroys Column Pruning
- **Files Involved**:
  - `core/src/main/java/com/google/cloud/gcs/analyticscore/core/prefetch/PredictivePrefetchOptimizer.java` (`observeAccess` lines 225–245, `recordColumnsInBlock` lines 268–271)
- **Architectural Clarification**:
  Caching fixed-size 4 MB blocks (`DEFAULT_BLOCK_SIZE_BYTES = 4 * 1024 * 1024`) in `PrefetchBufferCache` is intentional and must be preserved to avoid memory fragmentation and simplify range math.
  However, **learning/recording which columns were accessed** must use the exact requested byte range, not the 4 MB block boundaries.
- **What Happens**:
  When Spark requests `[position, position + length)`, `observeAccess` rounds `position` down to `blockOffset` (`position / 4MB * 4MB`) and calls `recordColumnsInBlock(rowGroupOrdinal, blockOffset)`. That method checks every column chunk overlapping `[blockOffset, blockOffset + 4MB)` and records all of them in `SchemaAccessHistory`.
- **Concrete Example**:
  In TPC-DS `customer`, 15 compressed columns fit inside a single 4 MB block in Row Group 0. Spark queries only `c_customer_id` (`0..50 KB`). Because `0..50 KB` falls inside Block 0 (`0..4 MB`), `recordColumnsInBlock` marks all 15 columns as accessed. In Row Group 1 (or a larger file), those 14 unused columns span Blocks 3, 4, and 5 (`12..24 MB`). The optimizer prefetches Blocks 3, 4, and 5 even though Spark never requested those columns.

---

### Root Cause 4: In-Flight Prefetches Treated as Cache Misses & Duplicate Current-RG Downloads
- **Files Involved**:
  - `core/src/main/java/com/google/cloud/gcs/analyticscore/core/prefetch/PredictivePrefetchOptimizer.java` (`tryCompleteFromCache` lines 198–215, `speculateRowGroups` lines 309–316)
  - `core/src/main/java/com/google/cloud/gcs/analyticscore/core/prefetch/PrefetchScheduler.java` (`inFlightByOffset` lines 61, 126–128)
- **What Happens**:
  1. **Vectored & Streaming Cache Miss on In-Flight Blocks**: `tryCompleteFromCache` only checks `bufferCache.isCached(itemId, blockOffset)`. If `PrefetchScheduler` has an active `CompletableFuture<ByteBuffer>` currently downloading that 4 MB block (e.g., 95% complete), `isCached` returns `false`. `tryCompleteFromCache` immediately returns `false` and issues a duplicate network download for the exact same bytes instead of awaiting or chaining onto the in-flight future.
  2. **Duplicate Current Row Group Speculation in Streaming Mode**: In `read(ByteBuffer)`, `speculateRowGroups` schedules background prefetches for the **current** row group starting from `currentBlockOffset`. Immediately after `onRead` returns `-1` (cache miss), `SmartReadChannel.read` calls `delegate.read(dst)` to download the exact same bytes synchronously in parallel.

---

### Root Cause 5: No Prefetch Triggered at `onOpen` When Schema Is Already Known
- **Files Involved**:
  - `core/src/main/java/com/google/cloud/gcs/analyticscore/core/prefetch/PredictivePrefetchOptimizer.java` (`onOpen` lines 105–117)
- **What Happens**:
  `onOpen` records the channel reference and sets `active = true`, but does not load the Parquet layout or check `SchemaAccessHistory`. Even when the schema fingerprint is already known from previous files, Row Group 0 is never prefetched on file open. Prefetching only triggers after the reader is already inside `read` or `readVectored`.

---

### Root Cause 6: Priority Inversion on Shared Thread Pool & Short-Lived Channel Churn in `readFully`
- **Files Involved**:
  - `core/src/main/java/com/google/cloud/gcs/analyticscore/core/prefetch/PredictivePrefetchOptimizer.java` (`readVectored` lines 140–162)
  - `core/src/main/java/com/google/cloud/gcs/analyticscore/core/channel/SmartReadChannel.java` (`readVectored` lines 118–125)
  - `core/src/main/java/com/google/cloud/gcs/analyticscore/core/GoogleCloudStorageInputStream.java` (`readFully` lines 193–216, `readTail` lines 219–242)
- **What Happens**:
  1. **Priority Inversion**: In `PredictivePrefetchOptimizer.readVectored`, `speculateNextRowGroup` submits background prefetch tasks for Row Group `N+1` to the shared 16-thread `readExecutorService` **before** returning unserved demand ranges to `SmartReadChannel.readVectored`. Background tasks for Row Group `N+1` starve foreground demand reads for Row Group `N`.
  2. **Short-Lived Channel Churn**: Iceberg's `GcsInputStreamWrapper.readFully` calls `GoogleCloudStorageInputStream.readFully`, which opens a brand-new `SmartReadChannel` and `PredictivePrefetchOptimizer` inside a `try-with-resources` block and immediately closes it (`onClose()` -> `cancelAll()`), killing any background prefetches triggered during that call.

---

### Root Cause 7: Aggressive 5-Second Cache TTL Expiration (`expireAfterWrite`)
- **Files Involved**:
  - `client/src/main/java/com/google/cloud/gcs/analyticscore/client/GcsPrefetchOptions.java` (`DEFAULT_BUFFER_CACHE_TTL_SECONDS = 5`, line 51)
  - `client/src/main/java/com/google/cloud/gcs/analyticscore/client/PrefetchBufferCache.java` (line 55)
- **What Happens**:
  `PrefetchBufferCache` uses `expireAfterWrite(5 seconds)`. When scanning large TPC-DS row groups with complex joins or aggregations, CPU processing of Row Group `N` often takes >5 seconds. Prefetched blocks for Row Group `N+1` finish downloading in 0.5s and get evicted from cache before the reader finishes Row Group `N`.

---

## 4. Step-by-Step Implementation Plan

### Step 1: Make `SchemaAccessHistory` Static / Process-Wide
- **Goal**: Ensure learned column projections persist across tasks/splits when Iceberg deserializes `GCSFileIO` instances on executors.
- **Changes**:
  1. In `AnalyticsCacheManager` (or `SchemaAccessHistory`), provide a static shared `SchemaAccessHistory` instance (or a static cache keyed by `maxSchemasTracked` / `historyWindowSize`).
  2. Update `AnalyticsCacheManager` to return this shared `SchemaAccessHistory` rather than instantiating an isolated `new SchemaAccessHistory(...)` per `GcsFileSystemImpl` instance.
  3. Ensure thread safety (note: `SchemaAccessHistory` already uses Caffeine cache and `ConcurrentHashMap.newKeySet()`, so it is thread-safe).

### Step 2: Fix Position & Strategy Desync on Cache Hits
- **Goal**: Prevent cache hits from tricking `AdaptiveReadStrategy` into thinking a random seek occurred.
- **Changes**:
  1. In `GcsReadChannel.position(long newPosition)` (lines 197–203), notify the read strategy or update `AdaptiveReadStrategy` so that advancing the channel position sequentially after a cache hit updates `lastReadEndPosition`.
  2. Alternatively, add a method `recordExternalReadAdvance(long newPosition)` on `ReadStrategy` / `AdaptiveReadStrategy` and call it when `SmartReadChannel` advances `delegate.position(position + bytesRead)` after a cache hit.
  3. Add a unit test verifying that interleaved cache hits and cache misses on `SmartReadChannel` keep `AdaptiveReadStrategy` in `SequentialReadStrategy`.

### Step 3: Record Exact Requested Ranges in `SchemaAccessHistory` (Preserve 4 MB Block Caching)
- **Goal**: Restore accurate column pruning without changing fixed-size 4 MB block caching.
- **Changes**:
  1. In `PredictivePrefetchOptimizer`:
     - Replace `recordColumnsInBlock(int rowGroupOrdinal, long blockOffset)` with `recordColumnsInRange(int rowGroupOrdinal, long startOffset, long endOffset)`.
     - Inside `recordColumnsInRange`, check `chunk.overlaps(startOffset, endOffset)` using the exact read range `[position, position + length)` rather than `[blockOffset, blockOffset + blockSize)`.
  2. Keep `collectTargetBlockOffsets` unchanged so that when prefetching a learned column chunk in future row groups, it still maps that column chunk to fixed 4 MB block offsets in `PrefetchBufferCache`.
  3. Update `PredictivePrefetchOptimizerTest` to verify that reading a small column inside a 4 MB block shared with other columns only records the requested column in `SchemaAccessHistory`.

### Step 4: Await In-Flight Prefetches & Fix Streaming Speculation
- **Goal**: Eliminate duplicate network downloads for blocks already being prefetched.
- **Changes**:
  1. In `PrefetchScheduler`:
     - Add a method `Optional<CompletableFuture<ByteBuffer>> getInFlightFuture(long blockOffset)` (or expose a method to retrieve/await in-flight block futures).
  2. In `PredictivePrefetchOptimizer.tryCompleteFromCache(GcsObjectRange range)` (for `readVectored`) and `copyFromCache(long position, ByteBuffer dst)` (for `read`):
     - If a required 4 MB block is not yet in `bufferCache.isCached(itemId, blockOffset)`, check `scheduler.getInFlightFuture(blockOffset)`.
     - In `readVectored`: Chain the range's completion onto `CompletableFuture.allOf(...)` of any in-flight block futures for that range, then copy from cache once settled.
     - In `read(ByteBuffer)`: If the block is in-flight, await the future (with appropriate timeout/error handling) or copy from the completed buffer rather than falling back to `delegate.read()`.
  3. In `PredictivePrefetchOptimizer.speculateRowGroups` (streaming mode):
     - Only schedule blocks starting strictly after the current read range (e.g., `Math.max(currentBlockOffset + blockSize, ...)` or only speculate `rowGroupOrdinal + 1`), preventing parallel duplicate downloads of the block currently being read by `delegate.read()`.

### Step 5: Trigger Prefetch at `onOpen` When Schema Fingerprint Is Known
- **Goal**: Eliminate cold start on File #2 onward (and single-row-group files).
- **Changes**:
  1. In `PredictivePrefetchOptimizer.onOpen(VectoredSeekableByteChannel source)`:
     - If footer caching / layout loading can be performed non-destructively (using `readVectored` for the footer tail so `source.position()` and `AdaptiveReadStrategy` are untouched), call `ensureLayoutLoaded(source)`.
     - Check if `schemaHistory.getAccessedColumns(layout.getSchemaFingerprint())` is non-empty.
     - If non-empty, immediately call `scheduler.schedule(itemId, collectTargetBlockOffsets(0, targetColumns), fileSize)` to prefetch Row Group 0 asynchronously upon stream open.

### Step 6: Fix Thread Pool Priority Inversion & `readFully` Channel Reuse
- **Goal**: Ensure demand reads are never starved by speculative prefetches, and prevent `readFully` from destroying prefetch state.
- **Changes**:
  1. In `SmartReadChannel.readVectored` and `PredictivePrefetchOptimizer.readVectored`:
     - Separate cache-hit filtering from speculative scheduling, or invoke `delegate.readVectored(unservedRanges)` **before** scheduling speculative prefetches for Row Group `N+1`.
  2. In `GoogleCloudStorageInputStream.readFully` (lines 193–216):
     - Reuse the existing open `channel` (via `getChannel()` / `readVectored` or positional read on the active `SmartReadChannel`) rather than opening and immediately closing a brand-new `SmartReadChannel` on every `readFully` call.

### Step 7: Increase Default Cache TTL & Switch to `expireAfterAccess`
- **Goal**: Prevent prefetched blocks from expiring while the CPU processes large row groups.
- **Changes**:
  1. In `GcsPrefetchOptions.java`:
     - Update `DEFAULT_BUFFER_CACHE_TTL_SECONDS` from `5` to `60` (or `120`).
  2. In `PrefetchBufferCache.java`:
     - Change `.expireAfterWrite(Duration.ofSeconds(ttlSeconds))` to `.expireAfterAccess(Duration.ofSeconds(ttlSeconds))`.

---

## 5. Verification Checklist for Implementing Agent

1. Run full unit test suite: `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./mvnw test` (all tests must pass).
2. Add unit tests covering:
   - Static `SchemaAccessHistory` sharing across multiple `GcsFileSystemImpl` instances.
   - `AdaptiveReadStrategy` remaining in `SequentialReadStrategy` across interleaved cache hits and misses.
   - Exact range column recording (`recordColumnsInRange`) vs 4 MB block caching.
   - Awaiting in-flight `PrefetchScheduler` futures during `read` and `readVectored`.
   - `onOpen` prefetching Row Group 0 when schema history is populated.
3. Run Spotless check: `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./mvnw spotless:check`.
