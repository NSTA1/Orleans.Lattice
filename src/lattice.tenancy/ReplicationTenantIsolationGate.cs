using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The real <see cref="IReplicationTenantIsolationGate"/> the tenancy add-on wires
/// into the inbound replication apply path so a replicated write lands only in its
/// correct tenant namespace. It derives the owning tenant from the entry's tree id
/// alone through <see cref="LatticeTenantTrees.GetOwner"/> - never from a
/// wire-supplied field, so a peer cannot redirect a write into a foreign tenant -
/// and refuses a write whose tenant does not exist in the <see cref="ITenantRegistry"/>
/// or is not resident in this serving region per the
/// <see cref="ITenantResidencyResolver"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is the isolation boundary only. It enforces namespace correctness, tenant
/// existence, and residency; it never gates on quota, because a replicated apply is
/// receiver-side convergence of a write that already happened on the origin and must
/// not be rejected on quota grounds. Quota admission stays on the authoring path.
/// </para>
/// <para>
/// Fail-closed and allocation-conscious: platform-owned system / definition trees
/// (so definitions converge everywhere) and bare legacy (default-tenant adoption)
/// trees admit on the allocation-free ownership-derivation fast path with no registry
/// or residency round-trip, preserving pre-tenancy replication behaviour; only a
/// well-formed <c>t/{tenantId}/{name}</c> tree naming a real tenant pays the existence
/// and residency checks, and only that path allocates.
/// </para>
/// </remarks>
internal sealed class ReplicationTenantIsolationGate(
    ITenantRegistry registry,
    ITenantResidencyResolver residency) : IReplicationTenantIsolationGate
{
    /// <inheritdoc />
    /// <remarks>
    /// Always <see langword="true"/>: the tenancy add-on registers this gate only
    /// when tenancy is enabled, so once wired the isolation boundary is enforced on
    /// every inbound run for a real tenant tree.
    /// </remarks>
    public bool IsActive => true;

    /// <inheritdoc />
    public async ValueTask<ReplicationTenantIsolationDecision> EvaluateAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var ownership = LatticeTenantTrees.GetOwner(treeId);

        // Platform-owned system / definition trees sit outside every tenant
        // namespace and converge everywhere (spec pt 4): admit on the allocation-free
        // ownership fast path with no registry / residency call.
        if (ownership.IsPlatformOwned)
        {
            return ReplicationTenantIsolationDecision.Admit;
        }

        var tenant = ownership.Tenant;

        // A bare legacy id adopted by the reserved default tenant is pre-tenancy
        // global state, not a real tenant namespace: admit unconditionally so
        // existing (unsegmented) trees keep replicating exactly as before tenancy.
        if (tenant.IsDefault)
        {
            return ReplicationTenantIsolationDecision.Admit;
        }

        // A well-formed t/{tenantId}/{name} tree naming a real tenant. The tenant
        // must exist here - never auto-create a tenant from an inbound write - and
        // must be resident in this serving region.
        if (!await registry.ExistsAsync(tenant, cancellationToken).ConfigureAwait(false))
        {
            return ReplicationTenantIsolationDecision.RejectUnknownTenant;
        }

        if (residency.IsActive && !residency.IsOnlineInServingRegion(tenant))
        {
            return ReplicationTenantIsolationDecision.RejectOutOfRegion;
        }

        return ReplicationTenantIsolationDecision.Admit;
    }
}
