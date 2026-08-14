# Configuration

Every knob on `FileWalStorageOptions`, its default, and the validation rules the paired validator enforces at first resolve. Options are populated through the `AddFileWalStorage` callback and read once at provider construction.

## Options

| Option | Type | Default | Purpose |
|---|---|---|---|
| `RootDirectory` | `string` | `""` (required) | Absolute or relative path to the root directory under which every tree/shard WAL is stored. The provider creates the directory and per-shard subdirectories on first use. Must not be null or empty. |
| `FlushToDisk` | `bool` | `true` | When `true`, every batch append and trim flushes the file to physical disk (fsync) before the returned task completes, honouring the all-or-nothing durability contract. Set `false` only for throwaway test or sample deployments where the WAL need not survive an unclean shutdown. |
| `CompactionThreshold` | `double` | `0.5` | The fraction of a shard's on-disk payload bytes that may be dead (trimmed but not yet reclaimed) before a `TrimAsync` call rewrites the segment file to reclaim the space. A value of `1.0` or greater disables trim-triggered compaction (space is still reclaimed on the next activation-time reconciliation). |
| `CompactionMinimumDeadBytes` | `int` | `65536` (64 KiB) | The minimum number of dead payload bytes a shard must hold before trim-triggered compaction runs, independent of `CompactionThreshold`. Prevents churn on a shard that trims small prefixes frequently. |

## Validation

Construction fails fast when `RootDirectory` is null, empty, or whitespace. The paired options validator additionally rejects an invalid configuration at first resolve, so a misconfigured host surfaces the error at startup rather than on the first write. The validator rejects a `CompactionThreshold` that is `NaN` or less than or equal to zero (use `1.0` or greater to disable trim-triggered compaction), and rejects a negative `CompactionMinimumDeadBytes`.

## Full example

```csharp verify
using Orleans.Lattice.Storage.File;

siloBuilder.AddFileWalStorage(options =>
{
    options.RootDirectory = "/data/wal";
    options.FlushToDisk = true;
    options.CompactionThreshold = 0.5;
    options.CompactionMinimumDeadBytes = 64 * 1024;
});
```

## Choosing a data root

Point `RootDirectory` at a path backed by a durable mount - a bind mount or named volume in a container, or a persistent disk on a VM. The directory must be writable by the process identity; a distroless container runs as a non-root user, so the mounted volume must grant that user write access. The provider does not attempt to recover durability if the path is transient (for example a container's writable layer), so state placed there is lost on `docker rm` or an image upgrade.
