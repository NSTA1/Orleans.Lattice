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
}
