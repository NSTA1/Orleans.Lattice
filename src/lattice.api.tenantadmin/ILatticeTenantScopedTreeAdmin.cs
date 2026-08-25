using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>tenant-scoped</b> whole-tree lifecycle and schema control
/// facade: a tenant-confined composition of the existing
/// <see cref="ILatticeTreeAdmin"/> (whole-tree lifecycle) and
/// <see cref="ILatticeSchemaAdmin"/> (per-tree schema policy) surfaces. Every verb
/// takes a tenant-<b>local</b>, unqualified tree name and runs it within the active
/// tenant's structural namespace (<c>t/{tenantId}/{name}</c>) and quota.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed tenant derivation.</b> The operating tenant is never a method
/// parameter and is never taken from the wire. It is derived solely from the
/// ambient <see cref="LatticeActiveTenantContext"/>, which the tenancy layer sets
/// only after validating the asserted tenant against the caller's membership. A
/// call made with no active tenant in scope is refused with a
/// <see cref="TenantScopeRequiredException"/> before any tree is named or touched.
/// </para>
/// <para>
/// <b>Structural namespace confinement.</b> Every verb composes its target tree id
/// through <see cref="LatticeTenantTrees.Compose"/> under the active tenant's
/// prefix, so a caller can only ever address a tree it owns: the composed id's
/// structural owner is always the active tenant, regardless of what the supplied
/// local name contains. A tenant is therefore structurally unable to lifecycle or
/// schema-modify a tree outside its own namespace - there is no parameter through
/// which another tenant's tree could be named. This is the single narrowest seam
/// at which confinement is enforced; the underlying facades then apply their own
/// fail-closed authorization on the composed id.
/// </para>
/// <para>
/// <b>Quota.</b> Tree creation is admitted against the active tenant's quota
/// (including the owned-tree-count dimension) before the tree is registered, so a
/// tenant at its ceiling is refused rather than growing its footprint past its
/// allocation.
/// </para>
/// </remarks>
public interface ILatticeTenantScopedTreeAdmin
{
    /// <summary>
    /// Creates (registers) the active tenant's tree named <paramref name="name"/>
    /// within the tenant's namespace and quota, with optional initial structural
    /// sizing. Registration is idempotent. The create is admitted against the
    /// tenant's quota (owned-tree-count and the other bounded dimensions) before it
    /// is applied.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="shardCount">The initial physical shard count, or <c>null</c> for the library default.</param>
    /// <param name="maxLeafKeys">The initial maximum keys per leaf node, or <c>null</c> for the library default.</param>
    /// <param name="maxInternalChildren">The initial maximum children per internal node, or <c>null</c> for the library default.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The creation result for the tenant-scoped tree.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    /// <exception cref="LatticeQuotaExceededException">The active tenant's quota would be exceeded by the create.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to administer the tree.</exception>
    Task<TreeCreationResult> CreateTreeAsync(
        string name,
        int? shardCount = null,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports whether the active tenant's tree named <paramref name="name"/> is
    /// registered. A pure read with no side effects.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The existence result, whose reported tree id is the composed tenant-scoped id.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeExistenceResult> CheckTreeExistsAsync(
        string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Soft-deletes the active tenant's tree named <paramref name="name"/>,
    /// reversible with <see cref="RecoverTreeAsync"/> until the recovery window
    /// elapses. Idempotent.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after the soft delete.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeDeletionStatus> DeleteTreeAsync(
        string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Recovers a soft-deleted tenant-local tree named <paramref name="name"/>
    /// within its recovery window.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after recovery.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeDeletionStatus> RecoverTreeAsync(
        string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Immediately hard-purges a soft-deleted tenant-local tree named
    /// <paramref name="name"/>, bypassing the soft-delete window. This is
    /// irreversible; the caller must pass <paramref name="confirm"/>
    /// <see langword="true"/>.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="confirm">Must be <see langword="true"/> to acknowledge the irreversible purge.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after the purge.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty, or <paramref name="confirm"/> is <see langword="false"/>.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeDeletionStatus> PurgeTreeAsync(
        string name, bool confirm, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the soft-deletion lifecycle status of the active tenant's tree named
    /// <paramref name="name"/>. A pure read with no side effects.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(
        string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets or replaces the schema-enforcement policy on the active tenant's tree
    /// named <paramref name="name"/>, enforced immediately on subsequent writes.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="policy">The policy to apply. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty, or a rule is invalid.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <c>null</c>.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    Task SetSchemaPolicyAsync(
        string name, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clears the schema-enforcement policy on the active tenant's tree named
    /// <paramref name="name"/>. Returns <c>true</c> when a policy was removed.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns><c>true</c> when a policy was removed; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    Task<bool> ClearSchemaPolicyAsync(
        string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the schema-enforcement policy on the active tenant's tree named
    /// <paramref name="name"/>, or <c>null</c> when none exists.
    /// </summary>
    /// <param name="name">The tenant-local, unqualified tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The enforcement policy, or <c>null</c> when none is set.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantScopeRequiredException">No active tenant is in scope.</exception>
    Task<LatticeSchemaPolicy?> GetSchemaPolicyAsync(
        string name, CancellationToken cancellationToken = default);
}
