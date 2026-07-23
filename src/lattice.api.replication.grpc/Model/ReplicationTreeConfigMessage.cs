using Orleans.Lattice;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// One tree's runtime replication configuration on the wire: the target tree id,
/// whether it is enabled, its single unambiguous declared merge mode (absent
/// when unassigned or ambiguous), and whether the merge mode is currently
/// ambiguous (so shipping is paused fail-closed).
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationTreeConfigMessage)]
[Immutable]
public sealed record ReplicationTreeConfigMessage
{
    /// <summary>The target tree id this entry describes.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree's enablement flag is currently set.
    /// </summary>
    [Id(1)] public bool Enabled { get; init; }

    /// <summary>
    /// Whether a single unambiguous merge mode is present. When
    /// <see langword="false"/>, <see cref="Mode"/> is not meaningful (no mode was
    /// assigned, or the mode is ambiguous).
    /// </summary>
    [Id(2)] public bool HasMode { get; init; }

    /// <summary>
    /// The single unambiguous declared merge mode. Only meaningful when
    /// <see cref="HasMode"/> is <see langword="true"/>.
    /// </summary>
    [Id(3)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree's merge-mode register carries more
    /// than one live value, so shipping is paused fail-closed until an operator
    /// resolves it.
    /// </summary>
    [Id(4)] public bool Ambiguous { get; init; }
}
