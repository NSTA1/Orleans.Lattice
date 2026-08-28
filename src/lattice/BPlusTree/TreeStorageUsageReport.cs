namespace Orleans.Lattice;

/// <summary>
/// Byte-accurate retained-storage snapshot for a single Lattice tree,
/// returned by <see cref="ILattice.GetStorageUsageAsync"/>. Aggregates the
/// three physical surfaces a tree occupies - write-ahead-log (WAL) rows,
/// snapshot blobs, and leaf/shard-root grain state - as exact retained
/// on-wire byte counts rather than entry-count estimates. Values are a
/// point-in-time sample; the aggregator may serve repeat calls from a short
/// in-memory cache (configured via <see cref="LatticeOptions.StorageUsageCacheTtl"/>).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeStorageUsageReport)]
[Immutable]
public readonly record struct TreeStorageUsageReport
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>
    /// Retained WAL bytes for the tree, summed across every partition.
    /// The on-wire byte total between the lowest and highest live offset.
    /// </summary>
    [Id(1)] public long WalRetainedBytes { get; init; }

    /// <summary>
    /// Snapshot blob bytes, read from snapshot-store metadata (content
    /// length) without a full blob read. <c>0</c> when no checkpoint exists.
    /// </summary>
    [Id(2)] public long SnapshotBytes { get; init; }

    /// <summary>
    /// Summed serialized leaf and shard-root grain-state bytes, read from
    /// each grain's cached last-persisted length (no extra I/O).
    /// </summary>
    [Id(3)] public long LeafStateBytes { get; init; }

    /// <summary>
    /// Sum of the three surfaces (<see cref="WalRetainedBytes"/> +
    /// <see cref="SnapshotBytes"/> + <see cref="LeafStateBytes"/>). A
    /// surface that reports "unsupported" contributes <c>0</c> to the sum
    /// and sets <see cref="Partial"/>; the total is therefore a lower bound
    /// when <see cref="Partial"/> is <c>true</c>.
    /// </summary>
    [Id(4)] public long TotalBytes { get; init; }

    /// <summary>
    /// <c>true</c> when the report is a lower bound rather than an exact
    /// figure, for either of two reasons:
    /// <list type="bullet">
    /// <item><description>a storage surface reported that it does not support
    /// byte accounting (a provider that does not override
    /// <see cref="IWalStorageProvider.GetRetainedByteSizeAsync"/>, or a
    /// snapshot store without content-length metadata); or</description></item>
    /// <item><description>a shard root or WAL partition did not answer the
    /// fan-out at all (it failed or timed out). Such a surface contributes
    /// nothing rather than a zero, because summing its zeroes would understate
    /// the tree while still presenting the total as complete.</description></item>
    /// </list>
    /// Either way, consumers should render the affected gauge as "no data"
    /// rather than a wrong zero. A partial report is transient: the next
    /// sample after the surface recovers is exact again.
    /// </summary>
    [Id(5)] public bool Partial { get; init; }

    /// <summary>UTC time at which this report was sampled.</summary>
    [Id(6)] public DateTimeOffset SampledAt { get; init; }

    /// <summary>
    /// Summed live (non-tombstone) key count across every shard in the tree,
    /// the figure the <c>orleans.lattice.admission.live_keys</c> gauge reports
    /// and that per-tree admission control compares against
    /// <see cref="LatticeOptions.MaxLiveKeys"/>. Best-effort and
    /// eventually-consistent: it is assembled from each shard root's O(1)
    /// incrementally-maintained live-key total (or re-anchored exactly on a
    /// deep refresh), so a concurrent cross-shard write may make it lag or lead
    /// the true count slightly. A time-expired entry that compaction has not yet
    /// reaped is counted as live until the next deep re-anchor. A shard that did
    /// not answer the fan-out contributes no keys and sets
    /// <see cref="Partial"/>, so this is a lower bound whenever
    /// <see cref="Partial"/> is <c>true</c>; a WAL-only cause of
    /// <see cref="Partial"/> (an unsupported byte surface) leaves it exact.
    /// </summary>
    [Id(7)] public long LiveKeys { get; init; }
}
