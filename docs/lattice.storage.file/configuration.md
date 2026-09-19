# Configuration

Every knob on `FileWalStorageOptions`, its default, and the validation rules the paired validator enforces at first resolve. Options are populated through the `AddFileWalStorage` callback and read once at provider construction.

## Options

| Option | Type | Default | Purpose |
|---|---|---|---|
| `RootDirectory` | `string` | `""` (required) | Absolute or relative path to the root directory under which every tree/shard WAL is stored. The provider creates the directory and per-shard subdirectories on first use. Must not be null or empty. |
| `FlushToDisk` | `bool` | `true` | When `true`, every batch append and trim flushes the file to physical disk (fsync) before the returned task completes, honouring the all-or-nothing durability contract. Set `false` only for throwaway test or sample deployments where the WAL need not survive an unclean shutdown. |
| `CompactionThreshold` | `double` | `0.5` | The fraction of a shard's on-disk payload bytes that may be dead (trimmed but not yet reclaimed) before a `TrimAsync` call rewrites the segment file to reclaim the space. A value of `1.0` or greater disables trim-triggered compaction (space is still reclaimed on the next activation-time reconciliation). |
| `CompactionMinimumDeadBytes` | `int` | `65536` (64 KiB) | The minimum number of dead payload bytes a shard must hold before trim-triggered compaction runs, independent of `CompactionThreshold`. Prevents churn on a shard that trims small prefixes frequently. |
| `CompactionMaximumDeadBytes` | `long` | `0` (disabled) | An **absolute** ceiling on the dead payload bytes a shard may hold. When greater than zero, a `TrimAsync` that leaves the shard at or above this many dead bytes compacts immediately, whatever `CompactionThreshold` says. It exists because the ratio bounds waste only *relative* to live data, so a large shard can sit indefinitely below the threshold while holding an unbounded amount of dead space in absolute terms - the defect measured in issue #3107, where a tree held 949 MB of dead bytes at a dead fraction of 0.08 and so never compacted. The `CompactionMinimumDeadBytes` floor still applies and is evaluated first, so a ceiling below the floor is **not** inert: every shard reaching the ceiling comparison has already cleared the floor, which makes the comparison unconditionally true and relocates the ceiling to the floor, rewriting the whole live shard on every trim that accumulates that much dead space. Only `0` disables the ceiling, and a non-zero ceiling below the floor is rejected at startup. Left at `0` by default: compaction rewrites every *live* byte to reclaim the dead ones, so the write amplification per byte reclaimed is `live/dead`, and a ceiling far below a shard's live size is a real and ongoing I/O cost. Enable it deliberately, sized against the disk you are protecting. |
| `MaxReadBatchBytes` | `long` | `16777216` (16 MiB) | Ceiling on the total payload bytes a single read page may materialise. The write path bounds a batch by entries and by bytes; without this, the read path bounded only entries, so a page of large records was unbounded in memory and a WAL of large entries could exhaust the heap mid-read. A page is truncated to the longest prefix that fits, but always yields at least one entry - even one larger than the whole budget - so the bound can never stall a reader. A truncated page is a resumption, not a skip: callers resume from the last offset actually returned. Default is four times the default WAL write-batch byte envelope, so a page always admits a full write batch. |

## Validation

Construction fails fast when `RootDirectory` is null, empty, or whitespace. The paired options validator additionally rejects an invalid configuration at first resolve, so a misconfigured host surfaces the error at startup rather than on the first write. The validator rejects a `CompactionThreshold` that is `NaN` or less than or equal to zero (use `1.0` or greater to disable trim-triggered compaction), rejects a negative `CompactionMinimumDeadBytes`, rejects a negative `CompactionMaximumDeadBytes` (`0` is the valid disabled default), and rejects a `MaxReadBatchBytes` below `1`. `MaxReadBatchBytes` is additionally checked at provider construction, because a host that builds the provider directly from `Options.Create` never runs the registration-time validator.

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
