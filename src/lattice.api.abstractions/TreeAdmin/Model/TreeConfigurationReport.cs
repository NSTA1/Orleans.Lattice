namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A read-only snapshot of a tree's registry-backed configuration: its structural
/// sizing pins, its alias target, and its per-tree runtime overrides (publish-events,
/// projection-digest maintenance, durable-history retention). Returned by reading a
/// tree's config and by setting it (the resulting state after the mutation).
/// </summary>
/// <remarks>
/// Every override field is nullable: <see langword="null"/> means the tree pins no
/// override for that knob and the silo-wide option value applies. A tree that is not
/// registered reports <see cref="Exists"/> <see langword="false"/> with every other
/// field left at its unset default.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeConfigurationReport)]
[Immutable]
public sealed record TreeConfigurationReport
{
    /// <summary>The logical tree id the configuration was read for.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree is registered; <see langword="false"/>
    /// when it has no registry entry (every other field is then at its unset default).
    /// </summary>
    [Id(1)] public bool Exists { get; init; }

    /// <summary>
    /// The physical tree id this logical tree is aliased to, or <see langword="null"/>
    /// when the tree is not aliased (it resolves to itself).
    /// </summary>
    [Id(2)] public string? PhysicalTreeId { get; init; }

    /// <summary>The tree's pinned physical shard count, or <see langword="null"/> when unregistered.</summary>
    [Id(3)] public int? ShardCount { get; init; }

    /// <summary>The tree's pinned maximum number of keys per leaf node, or <see langword="null"/> when unregistered.</summary>
    [Id(4)] public int? MaxLeafKeys { get; init; }

    /// <summary>The tree's pinned maximum number of children per internal node, or <see langword="null"/> when unregistered.</summary>
    [Id(5)] public int? MaxInternalChildren { get; init; }

    /// <summary>
    /// The per-tree publish-events override, or <see langword="null"/> when the tree
    /// pins no override and the silo-wide option applies.
    /// </summary>
    [Id(6)] public bool? PublishEvents { get; init; }

    /// <summary>
    /// The per-tree projection-digest-maintenance override, or <see langword="null"/>
    /// when the tree pins no override and the silo-wide option applies.
    /// </summary>
    [Id(7)] public bool? MaintainProjectionDigest { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree has permanently latched projection-digest
    /// maintenance off (a write landed while maintenance was disabled), which
    /// supersedes any <see cref="MaintainProjectionDigest"/> override. The latch is
    /// one-way and never cleared.
    /// </summary>
    [Id(8)] public bool ProjectionDigestPermanentlyDisabled { get; init; }

    /// <summary>
    /// The per-tree durable-history retention mode override, or <see langword="null"/>
    /// when the tree pins no override (retention falls back to metadata-only).
    /// </summary>
    [Id(9)] public HistoryRetentionMode? HistoryRetentionMode { get; init; }

    /// <summary>
    /// The per-tree durable-history age bound in ticks, or <see langword="null"/> when
    /// the tree pins no age bound (the timeline is retained until an explicit rebuild).
    /// </summary>
    [Id(10)] public long? HistoryRetentionWindowTicks { get; init; }
}
