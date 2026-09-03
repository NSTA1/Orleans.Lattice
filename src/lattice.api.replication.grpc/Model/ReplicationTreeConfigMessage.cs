using Orleans.Lattice;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// One tree's effective replication configuration on the wire: the target tree
/// id, whether it is enrolled, the single unambiguous merge mode in force
/// (absent when unassigned or ambiguous), whether the merge mode is currently
/// ambiguous (so shipping is paused fail-closed), and which enrollment source
/// put the tree in force.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationTreeConfigMessage)]
[Immutable]
public sealed record ReplicationTreeConfigMessage
{
    /// <summary>The target tree id this entry describes.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree is effectively enrolled for
    /// replication, i.e. the host admits its mutations for shipping.
    /// </summary>
    [Id(1)] public bool Enabled { get; init; }

    /// <summary>
    /// Whether a single unambiguous merge mode is present. When
    /// <see langword="false"/>, <see cref="Mode"/> is not meaningful (no mode was
    /// assigned, or the mode is ambiguous).
    /// </summary>
    [Id(2)] public bool HasMode { get; init; }

    /// <summary>
    /// The single unambiguous merge mode in force. Only meaningful when
    /// <see cref="HasMode"/> is <see langword="true"/>.
    /// </summary>
    [Id(3)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree's merge-mode register carries more
    /// than one live value, so shipping is paused fail-closed until an operator
    /// resolves it.
    /// </summary>
    [Id(4)] public bool Ambiguous { get; init; }

    /// <summary>
    /// Which enrollment source put this tree's configuration in force - the
    /// runtime config tree, the static deployment map, or both with the runtime
    /// entry winning. A peer predating this field sends nothing, which
    /// deserializes to <see cref="ReplicationEnrollmentSource.Runtime"/> - the
    /// only source the older report described.
    /// </summary>
    [Id(5)] public ReplicationEnrollmentSource Source { get; init; }
}
