namespace Orleans.Lattice.Storage.File;

/// <summary>
/// Configuration for the local disk-backed
/// <see cref="FileWalStorageProvider"/>. A host populates these options
/// through
/// <see cref="LatticeFileServiceCollectionExtensions.AddFileWalStorage"/>;
/// the provider reads them once at construction.
/// </summary>
public sealed class FileWalStorageOptions
{
    /// <summary>
    /// The default fraction of physically-retained payload bytes that
    /// may be trimmed-but-not-yet-reclaimed before
    /// <see cref="IWalStorageProvider.TrimAsync"/>-triggered compaction rewrites a shard's
    /// segment file (<c>0.5</c>: compact once at least half of the
    /// on-disk payload is dead).
    /// </summary>
    public const double DefaultCompactionThreshold = 0.5;

    /// <summary>
    /// The default minimum number of dead (trimmed) payload bytes a
    /// shard must accumulate before opportunistic compaction runs, so a
    /// lightly-trimmed shard is never rewritten for a trivial saving
    /// (<c>65536</c>).
    /// </summary>
    public const int DefaultCompactionMinimumDeadBytes = 64 * 1024;

    /// <summary>
    /// The default per-read payload byte ceiling (<c>16 MiB</c>). Four
    /// times <see cref="LatticeOptions.DefaultWalMaxBatchBytes"/>, so a
    /// page always has room for a full default-sized write batch and the
    /// bound never costs a round trip on an ordinary workload.
    /// </summary>
    public const long DefaultMaxReadBatchBytes = 16L * 1024 * 1024;

    /// <summary>
    /// Absolute or relative filesystem path to the root directory under
    /// which every tree/shard write-ahead log is stored. The provider
    /// creates the directory (and per-shard subdirectories) on first
    /// use. Must not be <see langword="null"/> or empty.
    /// </summary>
    public string RootDirectory { get; set; } = string.Empty;

    /// <summary>
    /// When <see langword="true"/> (the default) every batch append and
    /// trim flushes the underlying file to physical disk (fsync) before
    /// the returned task completes, honouring the
    /// <see cref="IWalStorageProvider"/> all-or-nothing durability
    /// contract. Setting this to <see langword="false"/> trades
    /// crash-durability for throughput and is intended only for
    /// throwaway test or sample deployments where the WAL need not
    /// survive an unclean shutdown.
    /// </summary>
    public bool FlushToDisk { get; set; } = true;

    /// <summary>
    /// The fraction of a shard's on-disk payload bytes that may be dead
    /// (trimmed but not yet physically reclaimed) before a
    /// <see cref="IWalStorageProvider.TrimAsync"/> call rewrites the segment file to reclaim
    /// the space. A value of <c>1.0</c> or greater disables
    /// trim-triggered compaction (space is still reclaimed on the next
    /// activation-time <see cref="IWalStorageProvider.ReconcileAsync"/>). Defaults to
    /// <see cref="DefaultCompactionThreshold"/>.
    /// </summary>
    public double CompactionThreshold { get; set; } = DefaultCompactionThreshold;

    /// <summary>
    /// The minimum number of dead (trimmed) payload bytes a shard must
    /// hold before trim-triggered compaction runs, independent of
    /// <see cref="CompactionThreshold"/>. Prevents churn on a shard that
    /// trims small prefixes frequently. Defaults to
    /// <see cref="DefaultCompactionMinimumDeadBytes"/>.
    /// </summary>
    public int CompactionMinimumDeadBytes { get; set; } = DefaultCompactionMinimumDeadBytes;

    /// <summary>
    /// Maximum total payload bytes a single read page may materialise,
    /// bounding <see cref="IWalStorageProvider.ReadAsync"/> and
    /// <see cref="IWalStorageProvider.ReadEncodedAsync"/> by size as well
    /// as by the caller's entry count. Defaults to
    /// <see cref="DefaultMaxReadBatchBytes"/>. Must be at least <c>1</c>.
    /// <para>
    /// The write path bounds a batch by both entries
    /// (<see cref="LatticeOptions.WalMaxBatchEntries"/>) and bytes
    /// (<see cref="LatticeOptions.WalMaxBatchBytes"/>). Without this
    /// option the read path bounded only entries, so a page of large
    /// records was unbounded in memory: an operator who set
    /// <c>WalMaxBatchBytes</c> would reasonably believe WAL memory was
    /// bounded in both directions, and it was not (issue #2689).
    /// </para>
    /// <para>
    /// A page always yields at least one entry, even when that entry
    /// alone exceeds the ceiling. Returning nothing would be
    /// indistinguishable from end-of-stream to every reader and would
    /// stall replay permanently at that offset, so this option bounds a
    /// page's size without ever being able to block progress.
    /// </para>
    /// <para>
    /// That floor is why this option cannot, on its own, keep a read
    /// affordable, and why it must not be tuned downwards in the hope that
    /// it will (issue #2742). It bounds how many bytes a page totals; it
    /// bounds neither the largest single block the read must find nor the
    /// page's cost relative to the memory that actually remains. Both gaps
    /// are closed below this option rather than by changing it:
    /// </para>
    /// <list type="bullet">
    /// <item><description>the oversized-single-entry case is decoded from
    /// pooled non-contiguous chunks, so it no longer needs a contiguous
    /// buffer its own size - the floor stays, and stops being expensive;
    /// </description></item>
    /// <item><description>this value is treated as a ceiling and narrowed
    /// per read by the process's current heap occupancy, because a ceiling
    /// chosen for healthy operation is the wrong one for a process whose
    /// allocations are already failing; and</description></item>
    /// <item><description>a page that still cannot be allocated is retried
    /// at a quarter of its width, down to one entry, before the read is
    /// refused as unaffordable.</description></item>
    /// </list>
    /// <para>
    /// The configured value therefore continues to mean what it says on a
    /// healthy host, and stops being the binding constraint on a host that
    /// is out of memory.
    /// </para>
    /// </summary>
    public long MaxReadBatchBytes { get; set; } = DefaultMaxReadBatchBytes;
}
