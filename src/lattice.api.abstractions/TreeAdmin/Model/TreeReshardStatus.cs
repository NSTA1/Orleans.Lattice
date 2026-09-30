namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The read-only status of a tree's online reshard, returned by the reshard
/// trigger verb and the standalone status read. Reports whether a reshard is
/// currently in flight and the tree's current physical shard fan-out as
/// observed from its <c>ShardMap</c>. A tree that has never been resharded
/// reports <see cref="InProgress"/> <see langword="false"/> with the current
/// map's shard counts. A pure projection with no side effects.
/// <para>
/// Reshard is online and self-completing (it grows the tree to the requested
/// physical shard count via reminder-anchored splits and then clears itself),
/// so this status surfaces the observable idle/in-flight signal, the map
/// fan-out, and the target and starting counts that fan-out is measured
/// against, rather than the coordinator's internal phase machine.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeReshardStatus)]
[Immutable]
public sealed record TreeReshardStatus
{
    /// <summary>The tree id whose reshard status this reports.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when an online reshard is currently in flight for
    /// the tree; <see langword="false"/> when the coordinator is idle (either no
    /// reshard has ever been initiated, or the last one has run to completion).
    /// </summary>
    [Id(1)] public bool InProgress { get; init; }

    /// <summary>
    /// The number of distinct physical shards the tree's current
    /// <c>ShardMap</c> routes to, or <c>0</c> when the tree has no custom map
    /// yet (it routes to the cluster default).
    /// </summary>
    [Id(2)] public int CurrentPhysicalShardCount { get; init; }

    /// <summary>
    /// The size of the virtual-slot routing space of the tree's current
    /// <c>ShardMap</c>, or <c>0</c> when the tree has no custom map yet.
    /// </summary>
    [Id(3)] public int VirtualShardCount { get; init; }

    /// <summary>
    /// The monotonically increasing version of the tree's current
    /// <c>ShardMap</c>, or <c>0</c> when the tree has no custom map yet.
    /// </summary>
    [Id(4)] public long MapVersion { get; init; }

    /// <summary>
    /// The target physical shard count requested by the reshard trigger that
    /// produced this status, or <see langword="null"/> for a standalone status
    /// read. The coordinator's in-flight target is reported on every read as
    /// <see cref="TargetShardCount"/>.
    /// </summary>
    [Id(5)] public int? RequestedShardCount { get; init; }

    /// <summary>
    /// The physical shard count the running reshard grows the tree to, read from
    /// the reshard coordinator's persisted intent - reported on every status read,
    /// unlike <see cref="RequestedShardCount"/>, which only echoes a trigger's
    /// argument. <see langword="null"/> when nothing is in flight, or when the
    /// status comes from a build that does not report it.
    /// </summary>
    [Id(6)] public int? TargetShardCount { get; init; }

    /// <summary>
    /// The physical shard count the tree had when the running reshard started,
    /// or <see langword="null"/> when nothing is in flight or it was not recorded
    /// (a reshard started by an earlier build). The reshard's progress is
    /// <see cref="CurrentPhysicalShardCount"/> minus this, out of
    /// <see cref="TargetShardCount"/> minus this: each split adds one physical
    /// shard to the map once it has durably committed.
    /// </summary>
    [Id(7)] public int? StartPhysicalShardCount { get; init; }
}
