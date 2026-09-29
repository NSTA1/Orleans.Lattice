namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The read-only status of a tree's snapshot machinery, returned by the snapshot
/// capture verb and the standalone status read. Reports whether a snapshot is
/// currently in flight for the source tree, echoing the destination tree id and
/// mode requested by the trigger that produced it. A pure projection with no side
/// effects.
/// <para>
/// A snapshot is self-completing and reminder-durable (it drains the source tree
/// shard-by-shard into the destination and, in <see cref="TreeSnapshotMode.Online"/>
/// mode, shadow-forwards live writes until the drain converges, then clears itself),
/// so this status surfaces the observable idle/in-flight signal and a coarse,
/// durable progress measure (<see cref="Phase"/>, <see cref="CopiedShardCount"/> and
/// <see cref="ShardCount"/>) rather than the coordinator's internal phase machine.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeSnapshotStatus)]
[Immutable]
public sealed record TreeSnapshotStatus
{
    /// <summary>The source tree id whose snapshot status this reports.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when a snapshot is currently in flight for the source
    /// tree; <see langword="false"/> when the coordinator is idle (either no snapshot
    /// has ever been initiated, or the last one has run to completion).
    /// </summary>
    [Id(1)] public bool InProgress { get; init; }

    /// <summary>
    /// The destination tree id requested by the capture trigger that produced this
    /// status, or <see langword="null"/> for a standalone status read (the
    /// coordinator's in-flight destination is not publicly surfaced).
    /// </summary>
    [Id(2)] public string? RequestedDestinationTreeId { get; init; }

    /// <summary>
    /// The snapshot mode requested by the capture trigger that produced this status,
    /// or <see langword="null"/> for a standalone status read.
    /// </summary>
    [Id(3)] public TreeSnapshotMode? RequestedMode { get; init; }

    /// <summary>
    /// The step the snapshot has durably reached while it runs, or
    /// <see langword="null"/> when nothing is in flight, or when the status comes
    /// from a build that does not report it.
    /// </summary>
    [Id(4)] public TreeSnapshotPhase? Phase { get; init; }

    /// <summary>
    /// The source shards whose copy has durably finished, read against
    /// <see cref="ShardCount"/>. It never runs ahead of what a resumed snapshot
    /// would start from. 0 when nothing is in flight.
    /// </summary>
    [Id(5)] public int CopiedShardCount { get; init; }

    /// <summary>
    /// The source shards the running snapshot copies, or <see langword="null"/>
    /// when it is not known: nothing is in flight, or the status comes from a
    /// build that does not report progress. A caller should then show the
    /// <see cref="Phase"/> without a percentage.
    /// </summary>
    [Id(6)] public int? ShardCount { get; init; }
}
