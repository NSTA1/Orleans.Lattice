using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Wal;

/// <summary>
/// Per-drain configuration handed to <see cref="IWalSubscriber.DrainAsync"/>.
/// Describes which per-shard WAL to tail, the durable per-partition
/// checkpoint to resume from, and the filtering / back-pressure policy the
/// generic tailing loop applies.
/// <para>
/// The context is consumer-owned: the durable checkpoint
/// (<see cref="Checkpoints"/>) and the highest consumed timestamp
/// (<see cref="HighestApplied"/>) live in the consumer's persistent state.
/// The subscriber reads them, advances them across a drain pass, and returns
/// the new positions in <see cref="WalDrainResult"/> for the consumer to
/// persist - the subscriber never writes consumer state itself.
/// </para>
/// </summary>
internal sealed class WalSubscriptionContext
{
    /// <summary>
    /// Creates a drain context.
    /// </summary>
    /// <param name="sourceTreeId">Logical tree id whose WAL is tailed. Must not be null or empty.</param>
    /// <param name="consumerId">Stable cursor-registry consumer id (for example <c>"view:{name}"</c> or <c>"replication:{peer}"</c>). Must not be null or empty.</param>
    /// <param name="partitions">The current number of WAL shards (partitions) for the source tree. A growing value across drains onboards new shards automatically.</param>
    /// <param name="checkpoints">The consumer's durable per-partition resume offset (the last applied offset; <c>-1</c> for a never-read partition).</param>
    public WalSubscriptionContext(
        string sourceTreeId,
        string consumerId,
        int partitions,
        IReadOnlyDictionary<int, long> checkpoints)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        ArgumentException.ThrowIfNullOrEmpty(consumerId);
        ArgumentNullException.ThrowIfNull(checkpoints);
        ArgumentOutOfRangeException.ThrowIfNegative(partitions);
        SourceTreeId = sourceTreeId;
        ConsumerId = consumerId;
        Partitions = partitions;
        Checkpoints = checkpoints;
    }

    /// <summary>Logical tree id whose WAL is tailed.</summary>
    public string SourceTreeId { get; }

    /// <summary>Stable cursor-registry consumer id used for WAL pinning.</summary>
    public string ConsumerId { get; }

    /// <summary>
    /// The current number of WAL shards (partitions). Re-read by the consumer
    /// each drain so a split / reshard that grows the partition count onboards
    /// the new shards on the next pass.
    /// </summary>
    public int Partitions { get; }

    /// <summary>
    /// The consumer's durable per-partition resume offset. A partition absent
    /// from the map (or mapped to <c>-1</c>) is read from the start of the WAL.
    /// </summary>
    public IReadOnlyDictionary<int, long> Checkpoints { get; }

    /// <summary>
    /// The highest source <see cref="HybridLogicalClock"/> the consumer has
    /// already consumed. Seeds the running high-water mark so a drain pass that
    /// reads nothing still reports a non-regressing cursor. Defaults to
    /// <see cref="HybridLogicalClock.Zero"/>.
    /// </summary>
    public HybridLogicalClock HighestApplied { get; init; } = HybridLogicalClock.Zero;

    /// <summary>
    /// Maximum number of entries read per partition per drain pass. Caps the
    /// work a single drain does so a deep backlog is paged off the WAL across
    /// several passes rather than in one unbounded read. Must be positive;
    /// defaults to <see cref="DefaultBatchSize"/>.
    /// </summary>
    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>The default per-partition batch size when none is supplied.</summary>
    public const int DefaultBatchSize = 256;

    /// <summary>
    /// When set, only entries whose <see cref="LatticeMutation.ShardIndex"/>
    /// equals this value are surfaced; sibling chain-shard entries that share
    /// the same physical WAL partition are skipped (the cursor still advances
    /// past them). <see langword="null"/> disables ShardIndex filtering and
    /// surfaces every entry on the partition.
    /// </summary>
    public int? ShardIndexFilter { get; init; }

    /// <summary>How the subscriber treats maintenance-category entries.</summary>
    public WalMaintenancePolicy MaintenancePolicy { get; init; } = WalMaintenancePolicy.Skip;

    /// <summary>
    /// When <see langword="true"/> (the default), the subscriber reports the
    /// drained HLC cursor (and the handler's <see cref="IWalSubscriptionHandler.BlockedAtHlc"/>
    /// pin) to the <see cref="IWalCursorRegistry"/> under
    /// <see cref="ConsumerId"/> after a successful pass, so the WAL garbage
    /// collector pins retention to this consumer's lag. Consumers that report a
    /// richer cursor shape themselves (for example a causal-plus per-origin
    /// vector) can set this to <see langword="false"/> and own the report.
    /// </summary>
    public bool PinWal { get; init; } = true;
}
