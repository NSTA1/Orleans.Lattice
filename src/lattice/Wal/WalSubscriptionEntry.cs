using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Wal;

/// <summary>
/// A single entry surfaced by <see cref="IWalSubscriber"/> to an
/// <see cref="IWalSubscriptionHandler"/> while tailing a per-shard
/// write-ahead log. Carries the durable WAL <see cref="Offset"/> and the
/// originating <see cref="Partition"/> alongside the decoded
/// <see cref="LatticeMutation"/>, plus convenience accessors that surface
/// the saga / atomic-batch and cross-tree coordination metadata a consumer
/// needs to buffer and reassemble multi-key writes without reaching into the
/// mutation itself.
/// <para>
/// Entries are surfaced strictly in ascending <see cref="Offset"/> order
/// within a partition. Ordering across partitions is not defined - a
/// consumer that needs a cross-partition order must impose one from the
/// per-entry <see cref="Timestamp"/>.
/// </para>
/// </summary>
internal readonly record struct WalSubscriptionEntry
{
    /// <summary>
    /// Creates a surfaced entry over a decoded WAL record.
    /// </summary>
    /// <param name="partition">The WAL shard (partition) index the entry was read from. Non-negative.</param>
    /// <param name="offset">The durable WAL offset of the entry within its partition.</param>
    /// <param name="mutation">The decoded mutation.</param>
    public WalSubscriptionEntry(int partition, long offset, LatticeMutation mutation)
    {
        Partition = partition;
        Offset = offset;
        Mutation = mutation;
    }

    /// <summary>The WAL shard (partition) index the entry was read from.</summary>
    public int Partition { get; }

    /// <summary>The durable WAL offset of the entry within its partition.</summary>
    public long Offset { get; }

    /// <summary>The decoded mutation.</summary>
    public LatticeMutation Mutation { get; }

    /// <summary>The mutation's hybrid logical clock timestamp.</summary>
    public HybridLogicalClock Timestamp => Mutation.Timestamp;

    /// <summary>Whether the entry is a user-driven write or a library-internal maintenance write.</summary>
    public MutationCategory Category => Mutation.Category;

    /// <summary>
    /// Whether the entry is a non-terminal prepared write that belongs to an
    /// in-flight atomic batch and must be buffered (keyed by
    /// <see cref="TransactionId"/>) until its terminal commit / abort is seen.
    /// </summary>
    public bool IsPrepared => Mutation.IsPrepared;

    /// <summary>
    /// The total number of writes in the atomic batch this entry belongs to,
    /// or <c>0</c> when the entry is not part of a batch. Used together with
    /// <see cref="AtomicShardCount"/> to detect batch completeness across the
    /// consumer's per-partition cursors.
    /// </summary>
    public int AtomicBatchSize => Mutation.AtomicBatchSize;

    /// <summary>
    /// The number of distinct WAL shards an atomic batch touched, stamped on
    /// terminal mutations by the saga coordinator. <c>0</c> on non-terminal
    /// mutations.
    /// </summary>
    public int AtomicShardCount => Mutation.AtomicShardCount;

    /// <summary>
    /// The saga identity that keys an atomic batch's sibling membership, or
    /// <see cref="System.Guid.Empty"/> for writes outside a saga.
    /// </summary>
    public Guid TransactionId => Mutation.TransactionId;

    /// <summary>
    /// The cross-tree operation id stamped on a cross-tree sub-saga terminal,
    /// or <see langword="null"/> for entries that do not participate in a
    /// joint cross-tree apply.
    /// </summary>
    public string? CrossTreeOperationId => Mutation.CrossTreeOperationId;

    /// <summary>
    /// The participating tree ids of a cross-tree operation, surfaced on the
    /// terminal so a consumer can drive joint all-or-nothing apply across the
    /// derived trees, or <see langword="null"/> when not a cross-tree terminal.
    /// </summary>
    public IReadOnlyList<string>? CrossTreeParticipants => Mutation.CrossTreeParticipants;
}
