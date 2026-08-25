namespace Orleans.Lattice.Replication;

/// <summary>
/// The default no-op <see cref="IReplicationTenantIsolationGate"/>: the null seam
/// core replication ships until the tenancy add-on supplies a real gate.
/// <see cref="IsActive"/> is <c>false</c> and <see cref="EvaluateAsync"/> always
/// admits, so the inbound apply path skips tenant isolation entirely and behaves
/// byte-for-byte as it did before tenancy existed. Registered via
/// <c>TryAddSingleton</c> so the tenancy add-on can displace it with
/// <c>Replace</c>.
/// </summary>
internal sealed class NullReplicationTenantIsolationGate : IReplicationTenantIsolationGate
{
    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public ValueTask<ReplicationTenantIsolationDecision> EvaluateAsync(
        string treeId,
        CancellationToken cancellationToken = default) =>
        new(ReplicationTenantIsolationDecision.Admit);
}
