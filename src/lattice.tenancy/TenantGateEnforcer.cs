using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ITenantGateEnforcer"/>: tenant-aware enforcement at the
/// auth gate. Registered by <c>AddLatticeTenancy</c> in place of the auth
/// package's <c>NullTenantGateEnforcer</c>, so once the tenancy add-on is
/// installed the auth gate composes tenant isolation on top of its policy
/// decision. The posture is <b>default-deny</b>: a request for a tenant-owned
/// tree is allowed only when one of the isolation rules below admits it.
/// </summary>
/// <remarks>
/// <para>
/// Enforcement is a warm, synchronous, in-memory decision. It consults the
/// compiled <see cref="ITenantPolicyEngine"/> (T6) for active-tenant validation
/// and cross-tenant grant resolution, derives tree ownership from the tree id
/// via <see cref="LatticeTenantTrees.GetOwner"/> (T0/T1), reads the ambient
/// active tenant from <see cref="LatticeActiveTenantContext"/> (T2), and gates
/// on the nested <see cref="ITenantResidencyResolver"/> residency seam. None of
/// these touch storage, so the enforcer is safe on the per-request hot path; the
/// allow path allocates only the single tenant-id string
/// <see cref="LatticeTenantTrees.GetOwner"/> materialises, and a deny allocates
/// its reason.
/// </para>
/// <para>
/// The four composed checks map to the tenancy spec:
/// </para>
/// <list type="number">
/// <item>Active-tenant-owns-tree: the active tenant may touch a tree it owns,
/// once its selection is validated by the engine.</item>
/// <item>Multi-membership / active-tenant switch: the engine's
/// <see cref="ITenantPolicyEngine.ValidateActiveTenant"/> decides whether the
/// subject may act as the selected active tenant (and fails closed when none is
/// selected).</item>
/// <item>Cross-tenant crossing: a cross-tenant grant from the owning tenant to
/// the active tenant, resolved via
/// <see cref="ITenantPolicyEngine.ResolveCrossTenantGrant"/>, admits a crossing
/// of the ownership boundary. The platform-operator crossing is realised earlier
/// by the auth gate's bootstrap-administrator bypass, so a platform operator
/// never reaches this enforcer.</item>
/// <item>Residency / online: the active tenant must be online in this serving
/// region, per the nested residency seam (allow when the seam is absent).</item>
/// </list>
/// </remarks>
internal sealed class TenantGateEnforcer(
    ITenantPolicyEngine engine,
    ITenantResidencyResolver residency) : ITenantGateEnforcer
{
    /// <summary>
    /// The read-only operation capabilities. A request composed exclusively of
    /// these maps to a <see cref="TenantGrantOperations.Read"/> cross-tenant
    /// grant requirement; anything else (a write, an admin verb, a lifecycle
    /// verb, or an empty mask) maps to the stricter
    /// <see cref="TenantGrantOperations.Write"/>.
    /// </summary>
    private const LatticeOperation ReadOnlyMask =
        LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Backup;

    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    public LatticeAccessDecision Enforce(in LatticeAccessRequest request)
    {
        var owner = LatticeTenantTrees.GetOwner(request.TreeId);

        // A platform-owned system tree (the _lattice_ / sys- namespaces) is not
        // tenant data; the auth gate already governs it and tenant isolation does
        // not apply. Allow so tenant enforcement never fences platform state.
        if (owner.IsPlatformOwned)
        {
            return LatticeAccessDecision.Allow();
        }

        var subjectId = request.Subject.SubjectId;
        var active = LatticeActiveTenantContext.Current;
        var tenantScoped = LatticeTenantTrees.IsTenantScoped(request.TreeId);

        // Compatibility carve-out: a bare (unsegmented) legacy tree addressed
        // with no active tenant is pre-tenancy traffic. Tenant adoption is
        // non-destructive, so an opted-in cluster's existing tenant-unaware
        // clients keep working. A tenant-scoped t/ tree never matches (it is
        // tenantScoped), and a request that carries an explicit active tenant
        // never matches (it is validated below).
        if (!tenantScoped && active is not { Value: not null })
        {
            return LatticeAccessDecision.Allow();
        }

        // From here the tree is tenant-owned data, so the posture is default-deny
        // unless a rule admits the request.
        if (active is { Value: not null } activeTenant)
        {
            if (activeTenant.Equals(owner.Tenant))
            {
                // (1)+(2) The active tenant owns the tree. Validate that the
                // subject may act as this active tenant, then gate on residency.
                var validation = engine.ValidateActiveTenant(subjectId, activeTenant);
                return validation.Allowed
                    ? EnforceResidency(activeTenant)
                    : Deny(validation.Reason);
            }

            // (3) Cross-tenant: the active tenant does not own the tree. A grant
            // the owning tenant issued to the active tenant, covering this scope
            // and operation, admits the crossing; otherwise deny.
            var grant = engine.ResolveCrossTenantGrant(
                activeTenant,
                owner.Tenant,
                request.TreeId,
                ToGrantOperations(request.Operation));
            return grant.Allowed
                ? EnforceResidency(activeTenant)
                : Deny(grant.Reason);
        }

        // (2) No active tenant selected on a tenant-owned tree. Fail closed
        // through the engine's active-tenant contract for the uninitialised
        // tenant, which denies ("no tenant" can never be an active tenant).
        var noSelection = engine.ValidateActiveTenant(subjectId, default);
        return noSelection.Allowed
            ? LatticeAccessDecision.Allow()
            : Deny(noSelection.Reason);
    }

    /// <summary>
    /// (4) Applies the residency / online gate: when the residency seam is active
    /// the active tenant must be online in this serving region. When the seam is
    /// absent (<see cref="ITenantResidencyResolver.IsActive"/> is <c>false</c>)
    /// this is a single bool read that allows.
    /// </summary>
    private LatticeAccessDecision EnforceResidency(TenantId tenant)
    {
        if (residency.IsActive && !residency.IsOnlineInServingRegion(tenant))
        {
            return LatticeAccessDecision.Deny(
                $"Tenant '{tenant}' is not online in this serving region.");
        }

        return LatticeAccessDecision.Allow();
    }

    /// <summary>
    /// Builds a deny decision, falling back to a generic default-deny reason when
    /// the engine returned a denial with no reason (it never does, but
    /// <see cref="LatticeAccessDecision.Deny"/> rejects an empty reason, so this
    /// keeps the enforcer fail-closed rather than throwing).
    /// </summary>
    private static LatticeAccessDecision Deny(string? reason) =>
        LatticeAccessDecision.Deny(
            string.IsNullOrEmpty(reason)
                ? "Tenant isolation denied the request."
                : reason);

    /// <summary>
    /// Maps a data-plane <see cref="LatticeOperation"/> mask to the coarse
    /// read/write capability a cross-tenant grant is expressed in. Fail-closed:
    /// only a request composed exclusively of read capabilities maps to
    /// <see cref="TenantGrantOperations.Read"/>; every other mask - including the
    /// empty <see cref="LatticeOperation.None"/> - maps to the stricter
    /// <see cref="TenantGrantOperations.Write"/>, so an unexpected mask can never
    /// be admitted by a read-only grant.
    /// </summary>
    private static TenantGrantOperations ToGrantOperations(LatticeOperation operation) =>
        operation != LatticeOperation.None && (operation & ~ReadOnlyMask) == 0
            ? TenantGrantOperations.Read
            : TenantGrantOperations.Write;
}
