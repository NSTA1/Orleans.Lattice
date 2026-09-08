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
/// <para>
/// Tenant existence is answered from the same compiled in-memory snapshot the
/// authoring-side policy engine already decides on - an O(1) frozen-dictionary
/// lookup - and falls back to an authoritative <see cref="ITenantRegistry"/> grain
/// call only when the tenant is absent from that snapshot. Previously every inbound
/// apply for a tenant tree made that grain call unconditionally, which made the
/// isolation gate itself the throughput ceiling of the replication apply path: the
/// apply path is deliberately not rate-limited (a replicated write is receiver-side
/// convergence and must not be refused), so a single busy tenant's replication
/// stream could saturate the registry grain and slow inbound convergence for every
/// other tenant in the estate. The asymmetry was visible within this one method,
/// whose residency check three lines later was already an in-memory lookup.
/// </para>
/// </remarks>
internal sealed class ReplicationTenantIsolationGate(
    ITenantRegistry registry,
    ITenantResidencyResolver residency,
    CompiledTenantPolicySnapshotMaintainer policy) : IReplicationTenantIsolationGate
{
    private readonly ITenantRegistry _registry = registry ?? throw new ArgumentNullException(nameof(registry));
    private readonly ITenantResidencyResolver _residency = residency ?? throw new ArgumentNullException(nameof(residency));
    private readonly CompiledTenantPolicySnapshotMaintainer _policy = policy ?? throw new ArgumentNullException(nameof(policy));
    /// <inheritdoc />
    /// <remarks>
    /// Always <see langword="true"/>: the tenancy add-on registers this gate only
    /// when tenancy is enabled, so once wired the isolation boundary is enforced on
    /// every inbound run for a real tenant tree.
    /// </remarks>
    public bool IsActive => true;

    /// <inheritdoc />
    public ValueTask<ReplicationTenantIsolationDecision> EvaluateAsync(
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
            return new ValueTask<ReplicationTenantIsolationDecision>(
                ReplicationTenantIsolationDecision.Admit);
        }

        var tenant = ownership.Tenant;

        // A bare legacy id adopted by the reserved default tenant is pre-tenancy
        // global state, not a real tenant namespace: admit unconditionally so
        // existing (unsegmented) trees keep replicating exactly as before tenancy.
        if (tenant.IsDefault)
        {
            return new ValueTask<ReplicationTenantIsolationDecision>(
                ReplicationTenantIsolationDecision.Admit);
        }

        // A well-formed t/{tenantId}/{name} tree naming a real tenant. The tenant
        // must exist here - never auto-create a tenant from an inbound write - and
        // must be resident in this serving region.
        //
        // The compiled snapshot is rebuilt on every mutation of the tenant registry
        // tree, so a tenant present in it demonstrably existed as of the last
        // successful rebuild; answering from it keeps the steady-state apply path
        // free of a per-entry grain call. A miss is not treated as absence - a
        // tenant created moments ago may not be compiled yet - so it falls through
        // to the authoritative registry, which is what keeps this fail-closed.
        //
        // A hit is trusted only while the snapshot is authoritative. A tenant
        // DELETED from the registry stays present in the snapshot until the
        // rebuild that deletion scheduled actually lands, and indefinitely if that
        // rebuild keeps failing (the maintainer logs and retains the previous
        // snapshot). Trusting a hit unconditionally would therefore keep admitting
        // a peer region's writes for a revoked tenant - a deny silently becoming an
        // allow, which is the one regression this optimisation must not introduce.
        // IsSnapshotAuthoritative is false exactly while a rebuild is outstanding
        // or failing, so those windows fall back to the registry and the fast path
        // is kept for the steady state it was added for.
        if (_policy.IsSnapshotAuthoritative
            && _policy.Current.TryGetTenant(tenant.Value ?? string.Empty, out var compiled)
            && compiled is not null)
        {
            // A tenant that exists but has been SUSPENDED is not admissible. The
            // authoring path already refuses it (LatticeTenantPolicyEngine
            // .ValidateActiveTenant denies any non-Active status), so admitting its
            // inbound shipping here would make suspension a one-sided control: an
            // operator suspends a tenant, every local write is refused, and the
            // tenant's data goes on changing anyway from any peer region still
            // shipping for it. Existence is not the same question as admissibility,
            // and this gate previously only asked the first.
            return new ValueTask<ReplicationTenantIsolationDecision>(
                compiled.Status == TenantStatus.Active
                    ? EvaluateResidency(tenant)
                    : ReplicationTenantIsolationDecision.RejectSuspendedTenant);
        }

        return EvaluateAgainstRegistryAsync(tenant, cancellationToken);
    }

    /// <summary>
    /// Residency half of the decision, shared by the snapshot-hit fast path and the
    /// registry fallback so both apply identical rules. An in-memory lookup against
    /// the residency snapshot; inert (admits every region) when residency is not
    /// wired.
    /// </summary>
    private ReplicationTenantIsolationDecision EvaluateResidency(TenantId tenant)
        => _residency.IsActive && !_residency.IsOnlineInServingRegion(tenant)
            ? ReplicationTenantIsolationDecision.RejectOutOfRegion
            : ReplicationTenantIsolationDecision.Admit;

    /// <summary>
    /// Slow path for a tenant absent from the compiled snapshot: consults the
    /// authoritative registry before admitting, so a not-yet-compiled tenant is
    /// evaluated correctly and an unknown one is still refused. The record is read
    /// rather than merely probed for existence, because the decision turns on the
    /// tenant's lifecycle status as well as its existence and a bare existence
    /// probe cannot answer the second.
    /// </summary>
    private async ValueTask<ReplicationTenantIsolationDecision> EvaluateAgainstRegistryAsync(
        TenantId tenant,
        CancellationToken cancellationToken)
    {
        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        if (record is null)
        {
            return ReplicationTenantIsolationDecision.RejectUnknownTenant;
        }

        if (!record.IsActive)
        {
            return ReplicationTenantIsolationDecision.RejectSuspendedTenant;
        }

        return EvaluateResidency(tenant);
    }
}
