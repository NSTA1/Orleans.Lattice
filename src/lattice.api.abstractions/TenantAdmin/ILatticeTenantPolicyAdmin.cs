namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>tenant policy</b> facade for delegated tenant access
/// administration: the tenant-tier rules a tenant's administrators author on the
/// tenant's own trees, a layer-aware explanation of any decision on those trees,
/// a subject's effective permissions there, and the tenant's access posture. Every
/// transport binding (gRPC, MCP) is a thin adapter over this one surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Opt-in.</b> The surface is inert unless the cluster enables delegated tenant
/// access administration (<c>LatticeTenancyOptions.DelegatedAccessAdministrationEnabled</c>).
/// While it is off every operation except <see cref="GetPostureAsync"/> is refused
/// with <see cref="TenantAccessAdministrationDisabledException"/> before anything is
/// read or written. <see cref="GetPostureAsync"/> answers either way, so a caller can
/// tell "off" from "denied".
/// </para>
/// <para>
/// <b>Tenant-tier, fail-closed authorization.</b> Every operation names its tenant
/// explicitly and is authorized against the caller: a platform operator, or an admin
/// of that tenant directly or through a group. Any other caller is refused
/// <see cref="Orleans.Lattice.LatticeAuthorizationDeniedException"/>, whether or not
/// the tenant exists. The reserved default tenant
/// (<see cref="Orleans.Lattice.TenantId.DefaultId"/>) has no tenant-tier rules and is
/// refused with <see cref="ReservedTenantOperationException"/>.
/// </para>
/// <para>
/// <b>Layering: platform rules are final.</b> Operator rules form the
/// <see cref="TenantRuleLayer.Platform"/> layer and are evaluated first; when one
/// matches, its verdict is final. Only when none matches is the
/// <see cref="TenantRuleLayer.Tenant"/> layer consulted. A tenant-tier allow can
/// therefore never carve a hole in a platform deny, and a tenant-tier deny can never
/// revoke a platform allow.
/// </para>
/// <para>
/// <b>Confinement.</b> A tenant-tier rule may only govern the tenant's own trees
/// (never its app-owned trees, a reserved or system tree, or another tenant's tree),
/// only data-plane operations, and only users, the tenant's own groups, and cluster
/// groups. Its id is local; the facade composes the reserved <c>tenant:{tenant}:</c>
/// id. A violation is refused with <see cref="TenantAccessConfinementException"/>.
/// The tenant's <c>MaxTenantRules</c> cap bounds its rule count; a write that would
/// exceed it is refused with <see cref="Orleans.Lattice.LatticeQuotaExceededException"/>.
/// </para>
/// <para>
/// <b>What a tenant administrator can see.</b> Listings show the tenant's
/// tenant-tier rules (editable) and the operator rules scoped to its own trees
/// (read-only). Cluster-wide <c>Tree:*</c> rules and app role rules are never
/// listed; when one decides an explanation or applies to a subject, it is reported
/// by rule id and effect only.
/// </para>
/// </remarks>
public interface ILatticeTenantPolicyAdmin
{
    // ----- Tenant-tier rules -----

    /// <summary>Creates or replaces a tenant-tier rule on the tenant's own keyspace.</summary>
    /// <param name="tenantId">The tenant whose rule to write. Must be a valid, non-empty tenant id.</param>
    /// <param name="rule">The rule to write, with a tenant-local id. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The rule as stored, in the <see cref="TenantRuleLayer.Tenant"/> layer and editable.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="rule"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is not a valid tenant id, or the rule is malformed (an empty id or subject, or a scope shape its <see cref="TenantRuleDraft.ScopeKind"/> does not allow).</exception>
    /// <exception cref="TenantAccessConfinementException">The rule would govern a tree or operation the tenant may not, name another tenant's group, or carry a reserved id.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeQuotaExceededException">Creating the rule would exceed the tenant's <c>MaxTenantRules</c> cap.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantRuleView> PutRuleAsync(
        string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default);

    /// <summary>Reads one of the tenant's tenant-tier rules by its local id, or <c>null</c> when none exists.</summary>
    /// <param name="tenantId">The tenant whose rule to read. Must be a valid, non-empty tenant id.</param>
    /// <param name="ruleId">The rule's tenant-local id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The rule, or <c>null</c> when it does not exist.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is not a valid tenant id, or <paramref name="ruleId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantRuleView?> GetRuleAsync(
        string tenantId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes one of the tenant's tenant-tier rules by its local id. Returns
    /// <see langword="true"/> when a rule was removed; removing a rule that does not
    /// exist is a no-op that returns <see langword="false"/>. A platform rule can
    /// never be removed through this surface.
    /// </summary>
    /// <param name="tenantId">The tenant whose rule to remove. Must be a valid, non-empty tenant id.</param>
    /// <param name="ruleId">The rule's tenant-local id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns><see langword="true"/> when a rule was removed.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is not a valid tenant id, or <paramref name="ruleId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<bool> RemoveRuleAsync(
        string tenantId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads one page of the rules governing the tenant: its tenant-tier rules
    /// (editable) and the operator rules scoped to its own trees (read-only, in the
    /// <see cref="TenantRuleLayer.Platform"/> layer). Cluster-wide <c>Tree:*</c> rules
    /// and app role rules are not listed.
    /// </summary>
    /// <param name="tenantId">The tenant whose rules to list. Must be a valid, non-empty tenant id.</param>
    /// <param name="page">Paging request (page size and continuation cursor). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>One page of the tenant's rules.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="page"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantRulePage> ListRulesAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default);

    // ----- Introspection -----

    /// <summary>
    /// Explains whether <paramref name="subjectId"/> may perform
    /// <paramref name="operation"/> on one of the tenant's trees, or on one key of
    /// it, reporting the deciding layer and rule. The subject's groups are resolved
    /// from the membership directory.
    /// </summary>
    /// <param name="tenantId">The tenant that owns the tree. Must be a valid, non-empty tenant id.</param>
    /// <param name="subjectId">The subject to explain the decision for, read per <paramref name="subjectKind"/>. Must not be <c>null</c> or empty.</param>
    /// <param name="treeName">The tenant-local tree name. Must not be <c>null</c> or empty.</param>
    /// <param name="key">The key to evaluate, or <c>null</c> to evaluate the whole tree.</param>
    /// <param name="operation">The operation to evaluate.</param>
    /// <param name="subjectKind">The kind of principal <paramref name="subjectId"/> names. When a group, the decision is evaluated for a member of that group.</param>
    /// <param name="cancellationToken">Cancels the evaluation.</param>
    /// <returns>The layer-aware explanation.</returns>
    /// <exception cref="ArgumentException">An argument is <c>null</c>, empty, or malformed.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantExplanation> ExplainAsync(
        string tenantId,
        string subjectId,
        string treeName,
        string? key,
        LatticeOperation operation,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the rules of both layers that name <paramref name="subjectId"/>
    /// directly or through one of its groups and govern the tenant's trees, grants
    /// and denies alike, optionally narrowed to one tree.
    /// </summary>
    /// <param name="tenantId">The tenant whose trees to resolve permissions on. Must be a valid, non-empty tenant id.</param>
    /// <param name="subjectId">The subject to resolve permissions for, read per <paramref name="subjectKind"/>. Must not be <c>null</c> or empty.</param>
    /// <param name="treeName">The tenant-local tree name to narrow to, or <c>null</c> for every tree of the tenant.</param>
    /// <param name="subjectKind">The kind of principal <paramref name="subjectId"/> names.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The layer-aware effective permissions.</returns>
    /// <exception cref="ArgumentException">An argument is <c>null</c>, empty, or malformed.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantEffectivePermissions> EffectivePermissionsAsync(
        string tenantId,
        string subjectId,
        string? treeName = null,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default);

    // ----- Posture -----

    /// <summary>
    /// Reports the tenant's access posture: whether delegated tenant access
    /// administration is enabled, whether the caller is an admin of the tenant or a
    /// platform operator, and the tenant's access caps with current usage. Answers
    /// while the feature is disabled, reporting <see cref="TenantAccessPosture.Enabled"/>
    /// <see langword="false"/>.
    /// </summary>
    /// <param name="tenantId">The tenant whose posture to read. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's access posture.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant, which has no delegated tenant access administration.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantAccessPosture> GetPostureAsync(
        string tenantId, CancellationToken cancellationToken = default);
}
