using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Wal;

/// <summary>
/// Outcome of a single <see cref="IWalSubscriber.DrainAsync"/> pass. The
/// consumer persists <see cref="AdvancedOffsets"/> and
/// <see cref="HighestTimestamp"/> into its own durable checkpoint and acts on
/// <see cref="FellOffLog"/> by taking its rebuild / bootstrap path.
/// </summary>
internal sealed class WalDrainResult
{
    /// <summary>
    /// <see langword="true"/> when at least one partition's oldest still-readable
    /// offset has advanced past the consumer's checkpoint - the entries needed to
    /// move the consumer forward by tail-replay were trimmed off the WAL. When set,
    /// the subscriber surfaces no entries and advances no offsets: the consumer must
    /// rebuild / bootstrap from current source state. Mutually exclusive with any
    /// surfaced entries.
    /// </summary>
    public bool FellOffLog { get; init; }

    /// <summary>The total number of entries read off the WAL this pass (surfaced or skipped).</summary>
    public long EntriesRead { get; init; }

    /// <summary>The number of entries surfaced to the handler this pass.</summary>
    public long EntriesSurfaced { get; init; }

    /// <summary>
    /// The highest source <see cref="HybridLogicalClock"/> observed across every
    /// entry read this pass (applicable or not), seeded from
    /// <see cref="WalSubscriptionContext.HighestApplied"/>. Monotonically
    /// non-decreasing; the consumer persists it and reports it as its cursor.
    /// </summary>
    public HybridLogicalClock HighestTimestamp { get; init; } = HybridLogicalClock.Zero;

    /// <summary>
    /// The new per-partition resume offset after this pass: the highest offset
    /// read on each partition that was drained. Partitions that read nothing are
    /// absent. The consumer merges these into its durable checkpoint.
    /// </summary>
    public IReadOnlyDictionary<int, long> AdvancedOffsets { get; init; } =
        new Dictionary<int, long>();
}
