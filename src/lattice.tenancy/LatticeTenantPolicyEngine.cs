namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default <see cref="ITenantPolicyEngine"/>. Reads the current compiled
/// snapshot from the <see cref="CompiledTenantPolicySnapshotMaintainer"/> and
/// answers each tenant-policy query against it. Holds no mutable state of its
/// own; the snapshot lifecycle (build, swap, epoch) lives on the maintainer.
/// </summary>
internal sealed class LatticeTenantPolicyEngine(CompiledTenantPolicySnapshotMaintainer maintainer)
    : ITenantPolicyEngine
{
    /// <inheritdoc />
    public long CurrentEpoch => maintainer.CurrentEpoch;

    /// <inheritdoc />
    public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        return maintainer.Current.ResolveAllowedTenants(subjectId);
    }

    /// <inheritdoc />
    public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId, IReadOnlyCollection<string> groupIds) =>
        maintainer.Current.ResolveAllowedTenants(subjectId, groupIds);

    /// <inheritdoc />
    public TenantAccessDecision ValidateActiveTenant(string subjectId, TenantId activeTenant) =>
        ValidateActiveTenant(maintainer.Current, subjectId, activeTenant);

    /// <inheritdoc />
    public TenantAccessDecision ValidateActiveTenant(
        string subjectId,
        IReadOnlyCollection<string> groupIds,
        TenantId activeTenant) =>
        ValidateActiveTenant(maintainer.Current, subjectId, groupIds, activeTenant);

    /// <summary>
    /// Validates that <paramref name="subjectId"/> may act as
    /// <paramref name="activeTenant"/> against an explicit compiled
    /// <paramref name="policy"/>, without groups: exactly
    /// <see cref="ValidateActiveTenant(CompiledTenantPolicy, string, IReadOnlyCollection{string}, TenantId)"/>
    /// with an empty group set.
    /// </summary>
    /// <param name="policy">The compiled policy to validate against. Must not be <c>null</c>.</param>
    /// <param name="subjectId">The caller subject id. Must not be <c>null</c>.</param>
    /// <param name="activeTenant">The asserted active tenant.</param>
    /// <returns>An allow decision when the subject may act as the tenant, or a denial carrying the reason.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> or <paramref name="subjectId"/> is <c>null</c>.</exception>
    internal static TenantAccessDecision ValidateActiveTenant(
        CompiledTenantPolicy policy,
        string subjectId,
        TenantId activeTenant) =>
        ValidateActiveTenant(policy, subjectId, Array.Empty<string>(), activeTenant);

    /// <summary>
    /// Validates that a subject may act as <paramref name="activeTenant"/> against
    /// an explicit compiled <paramref name="policy"/>: the tenant is registered,
    /// <see cref="TenantStatus.Active"/>, and admits the subject. A tenant compiled
    /// with delegated access administration disabled admits exactly its admin
    /// subjects by id; a group-aware one also admits its member entries and any
    /// subject one of whose <paramref name="groupIds"/> is an admin or member entry
    /// (<see cref="CompiledTenant.IsMember"/>). The single home of the active-tenant
    /// rule: the engine applies it to the maintainer's current snapshot, and the
    /// data-plane gate applies it to a policy compiled from the authoritative
    /// registry record while that snapshot is not authoritative, so both answer by
    /// the same rule (issue #4053).
    /// </summary>
    /// <param name="policy">The compiled policy to validate against. Must not be <c>null</c>.</param>
    /// <param name="subjectId">The caller subject id. Must not be <c>null</c>.</param>
    /// <param name="groupIds">The subject's resolved transitive group ids. Must not be <c>null</c>.</param>
    /// <param name="activeTenant">The asserted active tenant.</param>
    /// <returns>An allow decision when the subject may act as the tenant, or a denial carrying the reason.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/>, <paramref name="subjectId"/> or <paramref name="groupIds"/> is <c>null</c>.</exception>
    internal static TenantAccessDecision ValidateActiveTenant(
        CompiledTenantPolicy policy,
        string subjectId,
        IReadOnlyCollection<string> groupIds,
        TenantId activeTenant)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(subjectId);
        ArgumentNullException.ThrowIfNull(groupIds);

        if (activeTenant.Value is null)
        {
            return TenantAccessDecision.Deny("The uninitialised 'no tenant' value cannot be an active tenant.");
        }

        if (!policy.TryGetTenant(activeTenant.Value, out var tenant) || tenant is null)
        {
            return TenantAccessDecision.Deny($"Tenant '{activeTenant}' is not registered.");
        }

        if (tenant.Status != TenantStatus.Active)
        {
            return TenantAccessDecision.Deny($"Tenant '{activeTenant}' is not active (status '{tenant.Status}').");
        }

        if (!tenant.IsGroupAware)
        {
            if (!tenant.IsAdmin(subjectId))
            {
                return TenantAccessDecision.Deny($"Subject '{subjectId}' is not an admin of tenant '{activeTenant}'.");
            }
        }
        else if (!tenant.IsMember(subjectId, groupIds))
        {
            return TenantAccessDecision.Deny(
                $"Subject '{subjectId}' is not an admin or member of tenant '{activeTenant}', directly or through a group.");
        }

        return TenantAccessDecision.Allow();
    }

    /// <inheritdoc />
    public TenantAccessDecision ResolveCrossTenantGrant(
        TenantId sourceTenant,
        TenantId targetTenant,
        string scope,
        TenantGrantOperations operation) =>
        ResolveCrossTenantGrant(maintainer.Current, sourceTenant, targetTenant, scope, operation);

    /// <summary>
    /// Resolves a cross-tenant grant against an explicit compiled
    /// <paramref name="policy"/>. The single home of the grant decision: the
    /// engine applies it to the maintainer's current snapshot, and the data-plane
    /// gate applies it to a policy compiled from the authoritative registry record
    /// while that snapshot is being rebuilt, so both answer by the same rule.
    /// </summary>
    /// <param name="policy">The compiled policy to resolve against. Must not be <c>null</c>.</param>
    /// <param name="sourceTenant">The tenant requesting access.</param>
    /// <param name="targetTenant">The tenant whose data is being accessed (the granting tenant).</param>
    /// <param name="scope">The scope being accessed. Must not be <c>null</c>.</param>
    /// <param name="operation">The operation being requested.</param>
    /// <returns>An allow decision when a matching active grant exists, or a denial carrying the reason.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> or <paramref name="scope"/> is <c>null</c>.</exception>
    internal static TenantAccessDecision ResolveCrossTenantGrant(
        CompiledTenantPolicy policy,
        TenantId sourceTenant,
        TenantId targetTenant,
        string scope,
        TenantGrantOperations operation)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(scope);

        if (sourceTenant.Value is null)
        {
            return TenantAccessDecision.Deny("The source tenant is the uninitialised 'no tenant' value.");
        }

        if (targetTenant.Value is null)
        {
            return TenantAccessDecision.Deny("The target tenant is the uninitialised 'no tenant' value.");
        }

        if (!policy.TryGetTenant(targetTenant.Value, out var target) || target is null)
        {
            return TenantAccessDecision.Deny($"Target tenant '{targetTenant}' is not registered.");
        }

        if (target.TryGetTenantGrants(sourceTenant.Value, out var grants) && grants is not null)
        {
            for (var i = 0; i < grants.Length; i++)
            {
                var grant = grants[i];

                // The single seam at which a cross-tenant grant becomes an allow,
                // and therefore the single place the lifecycle gate belongs. Only
                // an Active grant authorizes: an offered-but-unapproved (Pending),
                // declined (Rejected), or withdrawn (Revoked) grant resolves to a
                // denial. Without this the approval step would be decorative and a
                // granting tenant could widen a grantee's access unilaterally by
                // offering alone. The compiled snapshot deliberately indexes every
                // live grant rather than pre-filtering, so this decision has
                // exactly one home.
                if (TenantGrantLifecycle.Authorizes(grant.State)
                    && (grant.Operations & operation) == operation
                    && ScopeCovers(grant.Scope, scope))
                {
                    return TenantAccessDecision.Allow();
                }
            }
        }

        return TenantAccessDecision.Deny(
            $"Tenant '{targetTenant}' holds no active grant for tenant '{sourceTenant}' covering scope '{scope}' with operation '{operation}'.");
    }

    /// <summary>
    /// <c>true</c> when a grant issued for <paramref name="grantScope"/> covers the
    /// requested <paramref name="requestedScope"/>: an exact match, or the grant
    /// scope is a <b>segment-boundary</b> prefix of the requested scope (a grant's
    /// scope is a tree name or tree-name prefix). The prefix must end at a tree-id
    /// segment separator (<c>/</c>) - either the grant scope already ends in it, or
    /// the requested scope has it immediately after the prefix - so a grant for
    /// <c>t/acme/orders</c> covers the child <c>t/acme/orders/2024</c> but never a
    /// distinct sibling tree such as <c>t/acme/orders-archive</c> that merely shares
    /// a leading substring. The enforcement layer may refine this interpretation.
    /// </summary>
    private static bool ScopeCovers(string grantScope, string requestedScope)
    {
        if (string.Equals(grantScope, requestedScope, StringComparison.Ordinal))
        {
            return true;
        }

        if (grantScope.Length == 0 || !requestedScope.StartsWith(grantScope, StringComparison.Ordinal))
        {
            return false;
        }

        // requestedScope is strictly longer than grantScope here (equal was handled
        // above), so indexing at grantScope.Length is in range. Require the prefix
        // to land on a segment boundary to avoid a sibling substring over-match.
        return grantScope[^1] == '/' || requestedScope[grantScope.Length] == '/';
    }
}
