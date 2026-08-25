namespace Orleans.Lattice.Replication;

/// <summary>
/// The tenant-isolation seam the inbound replication apply path consults to keep a
/// replicated write inside its correct tenant namespace. It is a nested
/// null-default seam local to the replication package: core replication ships only
/// the no-op <see cref="NullReplicationTenantIsolationGate"/> (whose
/// <see cref="IsActive"/> is <c>false</c>), so replication is byte-for-byte
/// unchanged until the tenancy add-on replaces the default with a real gate. This
/// keeps the dependency direction correct - replication never references the
/// tenancy package - while still letting tenancy enforce isolation at the single
/// narrowest receiver seam.
/// </summary>
/// <remarks>
/// <para>
/// The apply path reads <see cref="IsActive"/> first and calls
/// <see cref="EvaluateAsync"/> only when it is <c>true</c>, so the tenancy-off path
/// is a single bool read that adds no allocation and no grain call to the apply hot
/// path. When active, the gate is consulted once per inbound run (a run shares one
/// tree id, hence one owning tenant), after the enrollment / merge-mode gate and
/// before the write is applied.
/// </para>
/// <para>
/// The gate enforces the isolation boundary only - namespace correctness, tenant
/// existence, and residency. It never gates on quota: a replicated apply is
/// receiver-side convergence of a write that already happened on the origin, so it
/// must not be rejected on quota grounds. Quota admission stays on the authoring
/// path.
/// </para>
/// </remarks>
public interface IReplicationTenantIsolationGate
{
    /// <summary>
    /// <c>true</c> when a real tenancy gate is wired in (the tenancy add-on
    /// replaced the null default); <c>false</c> for the null default. The apply
    /// path reads this first and consults <see cref="EvaluateAsync"/> only when it
    /// is <c>true</c>, so replication behaves exactly as it did before tenancy when
    /// this is <c>false</c>.
    /// </summary>
    bool IsActive { get; }

    /// <summary>
    /// Classifies whether an inbound replicated write for <paramref name="treeId"/>
    /// may land in the tenant namespace the tree id names. The owning tenant is
    /// derived from the tree id alone - never from a wire-supplied tenant field -
    /// so a peer cannot influence the decision. Platform-owned and bare legacy
    /// trees always admit; a well-formed tenant tree admits only when its tenant
    /// exists and is resident in this serving region.
    /// </summary>
    /// <param name="treeId">The inbound entry's tree id. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the evaluation.</param>
    /// <returns>
    /// The <see cref="ReplicationTenantIsolationDecision"/> for the write:
    /// <see cref="ReplicationTenantIsolationDecision.Admit"/> to apply it, or a
    /// reject decision to refuse it.
    /// </returns>
    ValueTask<ReplicationTenantIsolationDecision> EvaluateAsync(
        string treeId,
        CancellationToken cancellationToken = default);
}
