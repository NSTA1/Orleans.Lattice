using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of <see cref="ILatticeTenantScopedTreeAdmin"/>:
/// the tenant-confined composition of the existing <see cref="ILatticeTreeAdmin"/>
/// and <see cref="ILatticeSchemaAdmin"/> surfaces. It is the single narrowest seam
/// at which a tenant-local tree name is bound to the active tenant's namespace and
/// quota; the wrapped facades then apply their own fail-closed authorization on the
/// composed, fully-qualified tree id.
/// </summary>
/// <remarks>
/// <para>
/// <b>Single confinement seam.</b> Every verb funnels through <c>ResolveScope</c>,
/// which (1) rejects a null/empty local name, (2) derives the operating tenant
/// solely from the ambient <see cref="LatticeActiveTenantContext"/> - never from a
/// parameter or the wire - refusing fail-closed with
/// <see cref="TenantScopeRequiredException"/> when none is in scope, and
/// (3) composes the target id through <see cref="LatticeTenantTrees.Compose"/> under
/// that tenant's prefix. Because the composed id's structural owner is always the
/// active tenant (<see cref="LatticeTenantTrees.GetOwner"/>), no supplied local name
/// - however adversarial - can name another tenant's tree, so tenant-namespace
/// confinement is structural rather than filter-based.
/// </para>
/// <para>
/// <b>Quota.</b> Tree creation is subject to the tenant's quota - <c>MaxTreeCount</c>
/// in particular - but that ceiling is <em>not</em> applied at this layer. It is
/// enforced by the delegated <see cref="ILatticeTreeAdmin.CreateTreeAsync"/>, which
/// is the single seam every create funnels through. Applying it here as well would
/// double-charge a single create, because admission is stateful: it consumes a
/// request-rate token. See the comment on <see cref="CreateTreeAsync"/>.
/// </para>
/// <para>
/// <b>Authorize before accounting.</b> The whole-tree
/// <see cref="LatticeOperation.Admin"/> authorization of the composed id performed
/// here is deliberately sequenced <em>before</em> the delegated create, whose own
/// quota consultation follows its own authorization check. The tenant quota is
/// accounted against is derived from the ambient
/// <see cref="LatticeActiveTenantContext"/>, which is a client-supplied assertion
/// that only the access gate validates. Accounting first would let an unauthorized
/// caller nominate any tenant and have a stateful, quota-consuming, rate-limiting
/// evaluation charged to that victim - and read the victim's current usage and
/// ceiling back out of the resulting quota exception. The pre-check mirrors the
/// authorization the delegated <see cref="ILatticeTreeAdmin.CreateTreeAsync"/>
/// performs on the same id, so an authorized caller sees no behavioural change,
/// while an unauthorized one is refused before any tenant state is read or mutated.
/// The core no-op gate short-circuits to allow at zero cost, so an auth-off host is
/// unaffected.
/// </para>
/// </remarks>
internal sealed class LatticeTenantScopedTreeAdmin : ILatticeTenantScopedTreeAdmin
{
    private readonly ILatticeTreeAdmin _treeAdmin;
    private readonly ILatticeSchemaAdmin _schemaAdmin;
    private readonly ILatticeAccessGate _gate;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>
    /// Initialises a new <see cref="LatticeTenantScopedTreeAdmin"/>.
    /// </summary>
    /// <param name="treeAdmin">The whole-tree lifecycle facade to delegate to.</param>
    /// <param name="schemaAdmin">The per-tree schema-policy facade to delegate to.</param>
    /// <param name="gate">
    /// The registered core access gate, consulted before delegating so accounting
    /// performed downstream can never precede authorization. In a host with no
    /// authorization add-on this is the no-op gate, so the check short-circuits to
    /// allow at zero cost.
    /// </param>
    /// <param name="membership">
    /// The membership context used to resolve the caller subject, or <c>null</c>
    /// when none is registered (every caller then resolves to the anonymous subject).
    /// </param>
    /// <exception cref="ArgumentNullException">Any required dependency is <c>null</c>.</exception>
    public LatticeTenantScopedTreeAdmin(
        ILatticeTreeAdmin treeAdmin,
        ILatticeSchemaAdmin schemaAdmin,
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        ArgumentNullException.ThrowIfNull(schemaAdmin);
        ArgumentNullException.ThrowIfNull(gate);

        _treeAdmin = treeAdmin;
        _schemaAdmin = schemaAdmin;
        _gate = gate;
        _membership = membership;
    }

    /// <inheritdoc />
    public async Task<TreeCreationResult> CreateTreeAsync(
        string name,
        int? shardCount = null,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default)
    {
        var (_, treeId) = ResolveScope(name);

        // Authorization strictly precedes quota accounting. The tenant resolved by
        // ResolveScope comes from the ambient active-tenant assertion, which is
        // client-supplied and validated only by the gate, so consulting the
        // admission controller first would let an unauthorized caller charge a
        // named victim tenant's quota and rate budget and read its usage and
        // ceiling back out of the resulting quota exception. This mirrors the
        // whole-tree Admin check the delegated CreateTreeAsync performs on the
        // same composed id; the no-op core gate short-circuits at zero cost.
        await LatticeAccessGateEnforcement
            .EnforceWholeTreeAsync(_gate, _membership, treeId, LatticeOperation.Admin, cancellationToken)
            .ConfigureAwait(false);

        // Quota admission - MaxTreeCount in particular - is enforced by the
        // delegated CreateTreeAsync below, on the same composed id and strictly
        // after its own authorization check. It deliberately is not repeated here:
        // admission is stateful (it consumes a request-rate token), so evaluating
        // it at both layers would charge a single create twice, and enforcing it
        // only at the outer layer was the defect - the inner facade is itself
        // tenant-aware and reachable directly, so a ceiling applied only here did
        // not bind on every path that creates a tenant tree.
        return await _treeAdmin
            .CreateTreeAsync(treeId, shardCount, maxLeafKeys, maxInternalChildren, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeExistenceResult> CheckTreeExistsAsync(
        string name, CancellationToken cancellationToken = default)
    {
        var (_, treeId) = ResolveScope(name);
        return await _treeAdmin.CheckTreeExistsAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeDeletionStatus> DeleteTreeAsync(
        string name, CancellationToken cancellationToken = default)
    {
        var (_, treeId) = ResolveScope(name);
        return await _treeAdmin.DeleteTreeAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeDeletionStatus> RecoverTreeAsync(
        string name, CancellationToken cancellationToken = default)
    {
        var (_, treeId) = ResolveScope(name);
        return await _treeAdmin.RecoverTreeAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeDeletionStatus> PurgeTreeAsync(
        string name, bool confirm, CancellationToken cancellationToken = default)
    {
        var (_, treeId) = ResolveScope(name);
        return await _treeAdmin.PurgeTreeAsync(treeId, confirm, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(
        string name, CancellationToken cancellationToken = default)
    {
        var (_, treeId) = ResolveScope(name);
        return await _treeAdmin.GetTreeDeletionStatusAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SetSchemaPolicyAsync(
        string name, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);
        var (_, treeId) = ResolveScope(name);
        await _schemaAdmin.SetPolicyAsync(treeId, policy, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> ClearSchemaPolicyAsync(
        string name, CancellationToken cancellationToken = default)
    {
        var (_, treeId) = ResolveScope(name);
        return await _schemaAdmin.ClearPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaPolicy?> GetSchemaPolicyAsync(
        string name, CancellationToken cancellationToken = default)
    {
        var (_, treeId) = ResolveScope(name);
        return await _schemaAdmin.GetPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The single narrowest confinement seam. Validates the tenant-local
    /// <paramref name="name"/>, derives the operating tenant fail-closed from the
    /// ambient active-tenant scope (never a parameter), and composes the
    /// fully-qualified, tenant-scoped tree id. Returns a value tuple (a struct, so
    /// no heap allocation); the only allocation is the composed id string, which is
    /// unavoidable.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name.</param>
    /// <returns>The active tenant and the composed tenant-scoped tree id.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    private static (TenantId Tenant, string TreeId) ResolveScope(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        if (LatticeActiveTenantContext.Current is not { Value: not null } tenant)
        {
            throw new TenantScopeRequiredException();
        }

        return (tenant, LatticeTenantTrees.Compose(tenant, name));
    }
}
