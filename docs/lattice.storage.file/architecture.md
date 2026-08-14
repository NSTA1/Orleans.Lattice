# Architecture

How `FileWalStorageProvider` lays out, commits, recovers, and compacts the write-ahead log on disk. The provider implements the core `IWalStorageProvider` contract; see [WAL Storage Providers](../lattice/wal-storage-providers.md) for the seam and its invariants.

## On-disk layout

Every `(tree, shard)` stream is an independent segmented, append-only log under the configured root:

```text
{RootDirectory}/
  {encodedTreeId}/
    shard-0/
      wal.log
    shard-1/
      wal.log
```

A tree id is percent-encoded into a filesystem-safe path segment: every byte outside the unreserved set `[A-Za-z0-9-._]` is written as `%XX` (uppercase hex of its UTF-8 byte), so distinct tree ids always map to distinct directories. The shard directory is `shard-{index}`, and each shard's log lives in a single `wal.log` segment file.

## Append and commit framing

Each append batch is framed as a run of length-prefixed data records sealed by a single commit trailer, and made durable with one write plus - when `FlushToDisk` is enabled - an fsync before the returned task completes. This gives the all-or-nothing batch contract: the trailer is the commit point, so a crash that lands after the data records but before the trailer is durable leaves an uncommitted, torn tail that recovery discards. A batch is only ever visible once its trailer is durable.

The stored payload for each entry is the `WalRecord`-shaped Orleans-serialised bytes - identical to the Azure Table provider's row payload. The hot commit path uses the zero-copy `AppendEncodedBatchAsync` overload: the producer has already encoded each record once via the configured `IWalRecordEncoder`, so the provider stores those segments verbatim with no re-encode, and `ReadEncodedAsync` returns them verbatim with no re-materialisation. The legacy `AppendBatchAsync` seam serialises each mutation to the same on-disk shape as a fallback.

## Offsets

Caller-assigned offsets are stored verbatim. The provider never assumes contiguity with the current tail: it rejects an append that overlaps any persisted offset, and it accepts a gap, so out-of-order concurrent appends (`LatticeOptions.WalMaxPendingBatches` greater than 1) are supported and a failed flush surfaces as an honest gap rather than a silently renumbered tail. `GetHighestOffsetAsync` returns the highest committed offset - which only advances as batches commit - and `GetLowestOffsetAsync` returns the lowest still-retained offset, so a caller computes the live entry count without scanning the log.

## Trimming and compaction

`TrimAsync` marks every entry at or below the supplied offset as dead and advances the retained head; it is idempotent, and trimming through an offset that does not yet exist reserves the trim point for a future append. Dead payload bytes are not reclaimed in place. Instead, once a shard's dead bytes cross both `CompactionThreshold` (a fraction of the on-disk payload) and `CompactionMinimumDeadBytes` (an absolute floor that prevents churn on small trims), the next trim rewrites the segment file to a fresh copy holding only the live entries, and the old file is replaced atomically. Setting `CompactionThreshold` to `1.0` or greater disables trim-triggered compaction; space is then reclaimed only on the next activation-time reconciliation.

## Crash recovery

`ReconcileAsync` runs at grain activation, before the WAL grain reads the highest offset. It scans the segment file, rolls every committed batch forward, discards any torn or uncommitted trailing batch, and reclaims previously-trimmed space. After reconciliation the retained shard entries are sorted by offset and durable, preserving any honest gaps from failed or out-of-order appends, so normal reads and writes rely on a consistent view. This is the local-disk analogue of the Azure Table provider's phase-1/phase-2 orphan repair.

## Durable WAL garbage collection

`AddFileWalStorage` also registers the same durable-WAL GC wiring the Azure Table provider installs - the WAL cursor registry, the leaf reporter, and the WAL GC - so opting into a durable local WAL never silently pairs with a process-local, restart-wiped cursor registry. All three are registered idempotently, so a host that already supplied its own keeps it.
