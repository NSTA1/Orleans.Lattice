using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TreeSnapshotGrain"/>.
/// Tracks the progress of an in-flight snapshot operation so that it can be
/// resumed after a silo restart.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeSnapshotState)]
internal sealed class TreeSnapshotState
{
    /// <summary>Whether a snapshot operation is currently in progress.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>The current phase of the active shard being processed.</summary>
    [Id(1)] public SnapshotPhase Phase { get; set; }

    /// <summary>The next source shard index to process (0-based).</summary>
    [Id(2)] public int NextShardIndex { get; set; }

    /// <summary>
    /// Number of consecutive failures for the current shard/phase.
    /// Reset to 0 when the shard advances or the phase changes.
    /// </summary>
    [Id(3)] public int ShardRetries { get; set; }

    /// <summary>The destination tree ID to snapshot into.</summary>
    [Id(4)] public string? DestinationTreeId { get; set; }

    /// <summary>The snapshot mode (online or offline).</summary>
    [Id(5)] public SnapshotMode Mode { get; set; }

    /// <summary>
    /// Unique operation ID for the overall snapshot. Used to generate per-shard
    /// bulk-load operation IDs for idempotency.
    /// </summary>
    [Id(6)] public string? OperationId { get; set; }

    /// <summary>
    /// The total number of shards. Captured at the start of the snapshot so that
    /// it is consistent even if options change mid-operation.
    /// </summary>
    [Id(7)] public int ShardCount { get; set; }

    /// <summary>Whether the snapshot has fully completed.</summary>
    [Id(8)] public bool Complete { get; set; }

    /// <summary>Optional leaf sizing override for the destination tree. Stored for idempotency checks.</summary>
    [Id(9)] public int? MaxLeafKeys { get; set; }

    /// <summary>Optional internal node sizing override for the destination tree. Stored for idempotency checks.</summary>
    [Id(10)] public int? MaxInternalChildren { get; set; }
}

/// <summary>
/// The phase of snapshot processing for the current shard.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.SnapshotPhase)]
internal enum SnapshotPhase
{
    /// <summary>
    /// All source shards need to be marked as deleted (offline mode only).
    /// This is the initial phase for offline snapshots — persisted before any
    /// external side effects so that a crash before shard marking can be recovered.
    /// </summary>
    Lock = 0,

    /// <summary>
    /// The shard needs to be copied: drain live entries from the source shard's
    /// leaf chain into memory, then bulk-load into the destination shard.
    /// </summary>
    Copy = 1,

    /// <summary>
    /// The source shard needs to be unmarked as deleted (offline mode only).
    /// In online mode this phase is skipped.
    /// </summary>
    Unmark = 2
}
