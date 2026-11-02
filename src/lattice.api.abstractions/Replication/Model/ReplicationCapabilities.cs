namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The read-only result of a replication capability probe
/// (<see cref="ILatticeReplicationControl.ProbeCapabilitiesAsync"/>): whether the
/// current caller may configure replication for one tree, evaluated through the same
/// fail-closed <see cref="LatticeOperation.Replication"/> gate the real enable and
/// disable use, but with <b>no side effects</b>. Default-deny: the flag is
/// <see langword="true"/> only when the gate would authorize the operation.
/// </summary>
/// <remarks>
/// The probe is a UX affordance, so a management surface can withhold a control the
/// caller cannot use; it is <b>not</b> a security boundary. The facade still
/// authorizes every real enable and disable fail-closed on attempt, so a client that
/// acts on a stale or wrong flag is still refused.
/// </remarks>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationCapabilities)]
[Immutable]
public sealed record ReplicationCapabilities
{
    /// <summary>Initializes a new <see cref="ReplicationCapabilities"/>.</summary>
    /// <param name="treeId">The tree id the probe was evaluated over. Must not be <c>null</c>.</param>
    /// <param name="canConfigure">Whether the caller may enable and disable replication for the tree.</param>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    public ReplicationCapabilities(string treeId, bool canConfigure)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        TreeId = treeId;
        CanConfigure = canConfigure;
    }

    /// <summary>
    /// The tree id the probe was evaluated over: the effective id, tenant-qualified
    /// for a tree of an asserted, non-default tenant.
    /// </summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller holds the whole-tree
    /// <see cref="LatticeOperation.Replication"/> capability over the tree, so an
    /// enable or disable for it would be authorized.
    /// </summary>
    [Id(1)] public bool CanConfigure { get; init; }
}
