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

A failed flush is never acknowledged, and recovery never resurrects it. Because a batch is written whole, trailer included, before it is flushed, the bytes can already be in the operating system's cache when the flush throws. If they later reached disk, recovery would roll the batch forward, even though its caller was told it failed. So on a failed write or flush the shard truncates the file back to the last acknowledged end, flushes that truncation, and only then rethrows. A trim marker gets the same treatment. If the truncation also fails, the tail's fate is unknown, and the shard fail-stops: every later operation throws an `IOException` naming the rollback failure, until the shard is reopened. It never acknowledges new records on top of a tail it could not reconcile. Compaction flushes its temporary file before it replaces `wal.log`, so a replaced log is never less durable than the one it superseded.

The stored payload for each entry is the `WalRecord`-shaped Orleans-serialised bytes - the same encoding the Azure Table provider produces, but only *before* that provider applies its per-row payload compression. The Azure Table provider Zstd-compresses each row payload by default, so the file provider's on-disk bytes match that provider's pre-compression encoding, not the compressed bytes it ultimately stores; the file provider applies no compression of its own and keeps the encoded segments verbatim. The hot commit path uses the zero-copy `AppendEncodedBatchAsync` overload: the producer has already encoded each record once via the configured `IWalRecordEncoder`, so the provider stores those segments verbatim with no re-encode, and `ReadEncodedAsync` returns them verbatim with no re-materialisation. The legacy `AppendBatchAsync` seam serialises each mutation to the same on-disk shape as a fallback.

## Offsets

Caller-assigned offsets are stored verbatim. The provider never assumes contiguity with the current tail: it rejects an append that overlaps any persisted offset, and it accepts a gap, so out-of-order concurrent appends (`LatticeOptions.WalMaxPendingBatches` greater than 1) are supported and a failed flush surfaces as an honest gap rather than a silently renumbered tail. `GetHighestOffsetAsync` returns the highest committed offset - which only advances as batches commit - and `GetLowestOffsetAsync` returns the lowest still-retained offset, so a caller computes the live entry count without scanning the log.

## Trimming and compaction

`TrimAsync` marks every entry at or below the supplied offset as dead and advances the retained head; it is idempotent, and trimming through an offset that does not yet exist reserves the trim point for a future append. Dead payload bytes are not reclaimed in place. Instead, once a shard's dead bytes cross both `CompactionThreshold` (a fraction of the on-disk payload) and `CompactionMinimumDeadBytes` (an absolute floor that prevents churn on small trims), the segment file is rewritten to a fresh copy holding only the live entries, and the old file is replaced atomically. Setting `CompactionThreshold` to `1.0` or greater disables threshold-triggered compaction; space is then reclaimed only on the next activation-time reconciliation.

That evaluation is reached from two places, and the distinction matters. `TrimAsync` ends in it unconditionally - a trim that removes no entry still evaluates - and `EvaluateCompactionAsync` performs the same evaluation without trimming, moving no watermark and changing no offset the shard reports. The WAL GC calls the second on a shard whose scan released nothing, because otherwise the gate on every compaction threshold is not "did we trim" but "was `TrimAsync` called at all", and a shard the GC stops scanning is a shard for which it is not. Issue #3207 measured that: a shard stopping on the tree-wide offset floor at its **first** entry reached no threshold comparison of any kind, so its dead bytes were stranded *above* the ratio rather than accumulating below it, and no compaction setting could have reached them.

A ratio is not on its own a bound. It constrains dead bytes only *relative* to live data, so a shard that grows while trimming proportionally can hold an ever-larger absolute quantity of dead space without ever crossing the threshold - and because the activation-time reconciliation is the only other trigger, a grain for an actively written tree never deactivates and so never reaches it. Issue #3107 measured exactly that shape in production: a WAL trimming roughly a thousand entries a minute grew over the same window, holding 949 MB of dead bytes at a dead fraction of 0.08. `CompactionMaximumDeadBytes` closes it with an absolute ceiling, checked before the ratio and reported through `orleans.lattice.wal.compactions` with a `trigger` of `ceiling`. It is disabled by default because compaction rewrites every live byte to reclaim the dead ones, so a ceiling well below a shard's live size buys space back at a standing cost in write amplification.

The two byte figures a shard reports are therefore not interchangeable. `GetRetainedByteSizeAsync` returns the live payload only - it excludes framing and, decisively, excludes dead bytes - while `GetPhysicalByteSizeAsync` returns the file's actual footprint. Disk is bounded by the second; a policy reading the first can be satisfied while the file is up to twice the size it believes.

## Crash recovery

`ReconcileAsync` runs at grain activation, before the WAL grain reads the highest offset. It scans the segment file, rolls every committed batch forward, discards any torn or uncommitted trailing batch, and reclaims previously-trimmed space. After reconciliation the retained shard entries are sorted by offset and durable, preserving any honest gaps from failed or out-of-order appends, so normal reads and writes rely on a consistent view. This is the local-disk analogue of the Azure Table provider's phase-1/phase-2 orphan repair.

## Durable WAL garbage collection

`AddFileWalStorage` also registers the same durable-WAL GC wiring the Azure Table provider installs - the WAL cursor registry, the leaf reporter, and the WAL GC - so opting into a durable local WAL never silently pairs with a process-local, restart-wiped cursor registry. All three are registered idempotently, so a host that already supplied its own keeps it.
