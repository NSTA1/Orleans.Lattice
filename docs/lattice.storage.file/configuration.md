---
agent_spec: "docs/agents/deployment.yaml"
---

# Configuration

Every knob on `FileWalStorageOptions`, its default, and the validation rules the paired validator enforces at first resolve. Options are populated through the `AddFileWalStorage` callback and read once at provider construction.

## Options

| Option | Type | Default | Purpose |
|---|---|---|---|
| `RootDirectory` | `string` | `""` (required) | Absolute or relative path to the root directory under which every tree/WAL-partition stream is stored. The provider creates the directory and per-partition subdirectories on first use. Must not be null or empty. |
| `FlushToDisk` | `bool` | `true` | When `true`, every batch append and trim flushes the file to physical disk (fsync) before the returned task completes, honouring the all-or-nothing durability contract. A failed write or flush is rolled back before it is reported, so recovery never resurrects it. Set `false` only for throwaway test or sample deployments where the WAL need not survive an unclean shutdown. |
| `CompactionThreshold` | `double` | `0.5` | The fraction of a shard's on-disk payload bytes that may be dead (trimmed but not yet reclaimed) before a compaction evaluation rewrites the segment file to reclaim the space. A value greater than `1.0` disables threshold-triggered compaction (space is then reclaimed only by the `CompactionMaximumDeadBytes` ceiling, when enabled, or by the next reconciliation); at exactly `1.0` the ratio still fires, but only on a shard whose payload is entirely dead. |
| `CompactionMinimumDeadBytes` | `int` | `65536` (64 KiB) | The minimum number of dead payload bytes a shard must hold before either the `CompactionThreshold` ratio or the `CompactionMaximumDeadBytes` ceiling can compact it; the compaction a reconciliation runs ignores it. Prevents churn on a shard that trims small prefixes frequently. |
| `CompactionMaximumDeadBytes` | `long` | `0` (disabled) | An **absolute** ceiling on the dead payload bytes a shard may hold. When greater than zero, a compaction evaluation that finds the shard at or above this many dead bytes compacts immediately, whatever `CompactionThreshold` says. It exists because the ratio bounds waste only *relative* to live data, so a large shard can sit indefinitely below the threshold while holding an unbounded amount of dead space in absolute terms - the defect measured in issue #3107, where a tree held 949 MB of dead bytes at a dead fraction of 0.08 and so never compacted. The `CompactionMinimumDeadBytes` floor still applies and is evaluated first, so a ceiling below the floor is **not** inert: every shard reaching the ceiling comparison has already cleared the floor, which makes the comparison unconditionally true and relocates the ceiling to the floor, rewriting the whole live shard on every evaluation that finds that much dead space. Only `0` disables the ceiling, and a non-zero ceiling below the floor is rejected by the registration-time validator. Left at `0` by default: compaction rewrites every *live* byte to reclaim the dead ones, so the write amplification per byte reclaimed is `live/dead`, and a ceiling far below a shard's live size is a real and ongoing I/O cost. Enable it deliberately, sized against the disk you are protecting. |
| `MaxReadBatchBytes` | `long` | `16777216` (16 MiB) | Ceiling on the total payload bytes a single read page may materialise. The write path bounds a batch by entries and by bytes; without this, the read path bounded only entries, so a page of large records was unbounded in memory and a WAL of large entries could exhaust the heap mid-read. A page is truncated to the longest prefix that fits, but always yields at least one entry - even one larger than the whole budget - so the bound can never stall a reader. A truncated page is a resumption, not a skip: callers resume from the last offset actually returned. The value is a ceiling, not a fixed page size: each read narrows it by the memory load the garbage collector last measured, as a fraction of the memory available to it - unchanged at or below 70%, falling linearly to a 1 MiB floor (or the configured value, when that is smaller) at 90% and above - and a page that still fails to allocate is retried at a quarter of its width, down to a single entry, before the read fails as unaffordable. Default is four times the default WAL write-batch byte envelope, so a page always admits a full write batch. |

The compaction and read-page defaults are also published as public constants on `FileWalStorageOptions`: `DefaultCompactionThreshold` (`0.5`), `DefaultCompactionMinimumDeadBytes` (`65536`), `DefaultCompactionMaximumDeadBytes` (`0`), and `DefaultMaxReadBatchBytes` (`16777216`).

## Validation

Construction fails fast when `RootDirectory` is null, empty, or whitespace. The paired options validator additionally rejects an invalid configuration when the options are first resolved, throwing an `OptionsValidationException` that lists every violation. That happens when the provider is first constructed - on the silo's first WAL operation - not while the silo starts: the registration does not request start-up validation. The validator rejects a `CompactionThreshold` that is `NaN` or less than or equal to zero (use a value greater than `1.0` to disable threshold-triggered compaction), rejects a negative `CompactionMinimumDeadBytes`, rejects a negative `CompactionMaximumDeadBytes` (`0` is the valid disabled default), rejects a non-zero `CompactionMaximumDeadBytes` below a non-zero `CompactionMinimumDeadBytes`, and rejects a `MaxReadBatchBytes` below `1`. `MaxReadBatchBytes` is additionally checked at provider construction, because a host that builds the provider directly from `Options.Create` never runs the registration-time validator.

## Full example

```csharp verify
using Orleans.Lattice.Storage.File;

siloBuilder.AddFileWalStorage(options =>
{
    options.RootDirectory = "/data/wal";
    options.FlushToDisk = true;
    options.CompactionThreshold = 0.5;
    options.CompactionMinimumDeadBytes = 64 * 1024;
    options.CompactionMaximumDeadBytes = 0; // disabled; set a byte ceiling to bound absolute waste
    options.MaxReadBatchBytes = 16L * 1024 * 1024;
});
```

## Choosing a data root

Point `RootDirectory` at a path backed by a durable mount - a bind mount or named volume in a container, or a persistent disk on a VM. The directory must be writable by the process identity; a distroless container runs as a non-root user, so the mounted volume must grant that user write access. The provider does not attempt to recover durability if the path is transient (for example a container's writable layer), so state placed there is lost on `docker rm` or an image upgrade.
