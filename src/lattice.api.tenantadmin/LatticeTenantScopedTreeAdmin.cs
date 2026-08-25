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
/// <b>Quota.</b> Tree creation consults the <see cref="ITenantAdmissionController"/>
/// (the tenancy quota seam) against the active tenant and composed id before the
/// tree is registered; the controller throws <see cref="LatticeQuotaExceededException"/>
/// on a breach, and its refusal is additionally treated as fail-closed here.
/// </para>
/// </remarks>
internal sealed class LatticeTenantScopedTreeAdmin : ILatticeTenantScopedTreeAdmin
{
    private readonly ILatticeTreeAdmin _treeAdmin;
    private readonly ILatticeSchemaAdmin _schemaAdmin;
    private readonly ITenantAdmissionController _admission;

    /// <summary>
    /// Initialises a new <see cref="LatticeTenantScopedTreeAdmin"/>.
    /// </summary>
    /// <param name="treeAdmin">The whole-tree lifecycle facade to delegate to.</param>
    /// <param name="schemaAdmin">The per-tree schema-policy facade to delegate to.</param>
    /// <param name="admission">The tenant admission / quota controller consulted on create.</param>
    /// <exception cref="ArgumentNullException">Any dependency is <c>null</c>.</exception>
    public LatticeTenantScopedTreeAdmin(
        ILatticeTreeAdmin treeAdmin,
        ILatticeSchemaAdmin schemaAdmin,
        ITenantAdmissionController admission)
    {
        ArgumentNullException.ThrowIfNull(treeAdmin);
        ArgumentNullException.ThrowIfNull(schemaAdmin);
        ArgumentNullException.ThrowIfNull(admission);

        _treeAdmin = treeAdmin;
        _schemaAdmin = schemaAdmin;
        _admission = admission;
    }

    /// <inheritdoc />
    public async Task<TreeCreationResult> CreateTreeAsync(
        string name,
        int? shardCount = null,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default)
    {
        var (tenant, treeId) = ResolveScope(name);

        // Count the create against the tenant's quota before it is applied. The
        // real controller throws LatticeQuotaExceededException on a breach; a
        // plain refusal is treated as fail-closed here. The IsActive short-circuit
        // keeps a tenancy-off cluster allocation-free on this path.
        if (_admission.IsActive
            && !await _admission.IsAdmittedAsync(tenant, treeId, cancellationToken).ConfigureAwait(false))
        {
            throw new LatticeTenantAccessDeniedException(
                $"Tenant '{tenant.Value}' is not admitted to create tree '{treeId}': the tenant's quota would be exceeded.");
        }

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
