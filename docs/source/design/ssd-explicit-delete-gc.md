# Reliable LOCAL_DISK deletion and bucket GC

This document describes the explicit-delete path for objects offloaded to the
Mooncake Store `BucketStorageBackend`, plus the target-machine verification
plan for the implementation. The feature applies to `Remove`, `BatchRemove`,
and `LOCAL_DISK` replicas. It does not change `RemoveAll`, `RemoveByRegex`,
cross-bucket merging, distributed storage backends, or eviction policy.

## End-to-end behavior

The delete initiator does not modify SSD state directly. The Master reads the
object's replica metadata and creates one holder-specific task for every
completed `LOCAL_DISK` replica. Object metadata is erased only after all tasks
fit in the bounded queue.

The holder pulls a bounded batch from `FileStorage` heartbeat processing:

1. `FetchRemoveTasks(client_id, max_tasks)` returns tasks without deleting
   them from the Master queue.
2. `BucketStorageBackend::MarkRemoved` verifies the tenant-scoped key and
   object incarnation, writes the updated bucket metadata to a temporary file,
   calls `fsync`, atomically renames it, and synchronizes the parent directory.
3. Only after the durable metadata update does the holder remove the key from
   `object_bucket_map_` and call `AckRemoveTasks`.
4. The Master erases acknowledged tasks. A lost response, holder restart, or
   lost ACK therefore produces a safe retry.

The queue is capped by the existing `offloading_queue_limit`. A full queue
returns `TASK_PENDING_LIMIT_EXCEEDED` and leaves object metadata intact.
Disconnected holders retain a lightweight segment placeholder with pending
tasks; the placeholder is reclaimed after the last task is acknowledged.
Pending task state, retry counters, and the fetch cursor are included in Master
snapshots.

## Incarnation and tenant isolation

Every bucket object receives an `object_version` UUID. The version is persisted
in bucket metadata, copied into the Master `LOCAL_DISK` descriptor, and carried
by each remove task. A task matches only the tenant-scoped local key and the
same UUID. If an old task arrives after the key has been recreated, the backend
returns `kStaleVersion`; the new object remains visible and the old task can be
safely acknowledged.

Metadata written before this field was introduced decodes with the zero UUID.
New objects always receive a generated UUID, so a legacy task cannot match a
new incarnation.

## Tombstone persistence and restart reconciliation

`BucketMetadata` stores a sorted list of tombstoned keys. On startup, the
backend restores `deleted_bytes_`, excludes tombstones from
`object_bucket_map_`, and omits them from `ScanMeta` and `BucketScan`.

`FileStorage::ReRegisterOffloadedObjects` no longer blindly recreates Master
replicas. It scans live local entries in batches of at most 1024 and calls
`BatchCheckLocalDiskReplicas`. Master checks holder, tenant, key, object
version, and size. Missing or mismatched entries are durably tombstoned;
matching entries only refresh their transport endpoint. This reconciliation is
the fallback for a task lost across a failure boundary.

## Single-bucket copy-on-write GC

`RunTombstoneGC` advances a bounded number of buckets per heartbeat. For a
partially deleted bucket, compaction:

1. marks the source bucket as mutating and snapshots its live entries;
2. holds a bucket read guard while reading live values outside the global
   metadata lock;
3. writes and synchronizes a new data file and staged metadata file;
4. revalidates every key, source bucket, and object version;
5. atomically publishes the final `.meta` file as the commit record;
6. switches in-memory mappings to the new bucket;
7. waits for source-bucket reads to drain, then deletes the old files.

The new metadata records `compacted_from_bucket_id`. If the process stops after
the new commit record is published but before the old bucket is deleted,
startup treats the source as superseded and retries deletion. A data file or
`.tmp` metadata file without a committed `.meta` file is ignored and cleaned
on startup. If all keys are tombstoned, GC retires and deletes the source
bucket without creating a replacement.

Physical accounting is reduced only after file removal succeeds. The code
tracks live object mappings, tombstoned bytes, and physical bucket bytes
separately. Failed file deletion leaves a retired bucket for a later GC retry.

## Metrics and logs

Master exports counters for queued, delivered (including retries),
acknowledged, and rejected SSD remove tasks:

- `master_ssd_remove_enqueued_total`
- `master_ssd_remove_delivered_total`
- `master_ssd_remove_acked_total`
- `master_ssd_remove_rejected_total`

Store-side SSD metrics count processed remove tasks, tombstone failures,
compacted buckets, and reclaimed physical bytes. Enqueue rejection,
tombstone-persistence failure, compaction, and physical-delete failure also
emit structured log messages with task or bucket identifiers.

## Target-machine test plan

The implementation was prepared without a completed local build or test run.
Run the following on a supported Linux build host and record the exact commit,
compiler, filesystem, pass count, and failures.

Handoff status on the development machine:

| Check | Result |
| --- | --- |
| `clang-format` 20 dry run on all changed C++ files | passed |
| `git diff --check` | passed |
| `pre-commit` | not run; executable is not installed |
| CMake configure and compilation | not completed; run on the target host |
| Automated tests and documentation build | not run by request |

### 1. Formatting and lightweight checks

```bash
./scripts/code_format.sh -b upstream/main --check
pre-commit run --all-files
git diff --check upstream/main...HEAD
```

If the formatting check reports changes, run the same command without
`--check`, review the diff, and rerun pre-commit.

### 2. Configure and compile

Use the repository's normal Linux options for the target host. At minimum,
enable Store tests, then build the affected binaries:

```bash
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build -j "$(nproc)" --target \
  master_service_ssd_test \
  master_service_ssd_test_for_snapshot \
  storage_backend_test \
  file_storage_test
```

### 3. Run focused automated tests

```bash
ctest --test-dir build --output-on-failure \
  -R 'master_service_ssd_test|storage_backend_test|file_storage_test'
```

The new tests cover:

- actual-holder and multi-holder task routing;
- tenant isolation, successful-only `BatchRemove`, offline-holder remount, queue
  admission, bounded fetch, redelivery, and idempotent ACK;
- holder/version/size reconciliation and remove-task snapshot round trips;
- durable tombstones, hidden reads, idempotency, restart recovery, and
  `ScanMeta` filtering;
- stale-task protection after same-key recreation;
- no ACK when atomic tombstone publication fails;
- partial and full-bucket GC, live-data equality, old-file deletion, physical
  accounting, and uncommitted temporary-file cleanup.

Run the broader Store regression set as well:

```bash
ctest --test-dir build --output-on-failure \
  -R 'master_service|batch_remove|client|storage_backend|file_storage|promotion|offload|evict'
```

### 4. Crash and fault-injection matrix

Automate each stop point with `SIGKILL`, restart the Store using the same SSD
directory, and verify both `IsExist`/`ScanMeta` and the physical files:

| Stop or fault point | Required result after restart |
| --- | --- |
| Before tombstone metadata rename | key may remain live; task is redelivered |
| After tombstone rename, before ACK | key stays hidden; repeated task is safe |
| After ACK response is lost | key stays hidden; repeated ACK is safe |
| After compacted data write, before final `.meta` | new orphan is removed; source remains authoritative |
| After final `.meta`, before in-memory switch | new bucket wins; source is superseded |
| After switch, before source deletion | live data comes from new bucket; source deletion is retried |
| Source file deletion returns an error | physical usage is not reported as reclaimed |

### 5. Concurrency and capacity soak

Use a multi-threaded workload that repeatedly loads live keys while another
thread removes keys, compacts their buckets, and recreates some keys. Assert
that no read returns corrupt data, no deleted incarnation reappears after
restart, and no old task deletes a recreated version. Include a holder that
stays offline long enough to fill the configured queue and confirm subsequent
removes are rejected without losing Master metadata.

Before handoff, capture:

```bash
git status
git log --oneline --decorate -10
git diff upstream/main...HEAD --stat
```

Record results as `passed / failed / not run`; do not treat a successful build
as a successful test run.

## Current limitations

- GC compacts one bucket at a time and does not merge buckets.
- Heartbeat advances at most one GC candidate per tick.
- File-per-key, offset-allocator, distributed, NoF, and P2P backends do not
  implement this bucket tombstone protocol. Their holders acknowledge the
  generic Master task without changing their legacy physical-delete behavior.
- `RemoveAll` and `RemoveByRegex` retain their existing behavior.
