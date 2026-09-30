using Microsoft.Extensions.Logging;
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
/// these touch storage on the steady state, so the enforcer is safe on the
/// per-request hot path; the allow path allocates only the single tenant-id string
/// <see cref="LatticeTenantTrees.GetOwner"/> materialises, and a deny allocates
/// its reason.
/// </para>
/// <para>
/// The one exception is a decision that consumes the asserted active tenant
/// (a tenant-owned tree addressed with an active tenant selected) made while the
/// compiled snapshot is not authoritative
/// (<see cref="CompiledTenantPolicySnapshotMaintainer.IsSnapshotAuthoritative"/>
/// is <c>false</c> while a registry-driven rebuild is outstanding or failing, and
/// while this silo cannot confirm its snapshot reflects a registry write committed
/// on another silo - see issue #4030).
/// The snapshot then still holds the pre-write membership, status and grant state,
/// so <see cref="EnforceAsync"/> confirms both the active-tenant validation and,
/// for a crossing, the grant against the authoritative <see cref="ITenantRegistry"/>
/// records, and the synchronous <see cref="Enforce"/> denies: a removed admin, a
/// suspended or deleted tenant, and a revoked grant never outlive the write that
/// ended them, and an added admin or an approved grant is read-your-writes
/// (issues #4001, #4053).
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
/// selected). The active tenant is a caller-supplied assertion, so this check
/// gates <b>every</b> branch that consumes it - the owned-tree branch and the
/// cross-tenant crossing alike - not just the owned-tree one.</item>
/// <item>Cross-tenant crossing: a cross-tenant grant from the owning tenant to
/// the active tenant, resolved via
/// <see cref="ITenantPolicyEngine.ResolveCrossTenantGrant"/>, admits a crossing
/// of the ownership boundary, <em>after</em> the subject's right to act as the
/// active tenant has been validated. The platform-operator crossing is realised
/// earlier by the auth gate's bootstrap-administrator bypass, so a platform
/// operator never reaches this enforcer.</item>
/// <item>Residency / online: the active tenant must be online in this serving
/// region, per the nested residency seam (allow when the seam is absent).</item>
/// </list>
/// </remarks>
internal sealed class TenantGateEnforcer(
    ITenantPolicyEngine engine,
    ITenantResidencyResolver residency,
    CompiledTenantPolicySnapshotMaintainer policy,
    ITenantRegistry registry,
    ILogger<TenantGateEnforcer> logger) : ITenantGateEnforcer
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

    /// <summary>
    /// The reason instance that marks <see cref="ConfirmationRequired"/>. Compared
    /// by reference, so it is a private string instance no other denial shares.
    /// </summary>
    private static readonly string ConfirmationRequiredReason = new('?', 1);

    /// <summary>
    /// The internal marker <see cref="Decide"/> returns for a request it cannot
    /// answer authoritatively. Never returned to a caller: <see cref="Enforce"/>
    /// denies it and <see cref="EnforceAsync"/> confirms it.
    /// </summary>
    private static readonly LatticeAccessDecision ConfirmationRequired =
        LatticeAccessDecision.Deny(ConfirmationRequiredReason);

    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    /// <remarks>
    /// The synchronous form cannot consult the registry, so a request that consumes
    /// the asserted active tenant and arrives while the compiled snapshot is not
    /// authoritative is <b>denied</b> here: the subject's membership, the tenant's
    /// status, and any grant cannot be confirmed, and a subject removed or a grant
    /// revoked moments ago must not keep admitting access. The auth gate calls
    /// <see cref="EnforceAsync"/>, which confirms such a request against the
    /// registry instead.
    /// </remarks>
    public LatticeAccessDecision Enforce(in LatticeAccessRequest request)
    {
        var decision = Decide(in request);
        return NeedsConfirmation(in decision)
            ? DenyUnconfirmed(PendingConfirmation.From(in request))
            : decision;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Completes synchronously, with no allocation beyond <see cref="Enforce"/>'s,
    /// on every path except two: the first call on a silo whose snapshot has never
    /// been built, which builds it first; and a request that consumes the asserted
    /// active tenant while the compiled snapshot is not authoritative (a
    /// tenant-registry write has scheduled a rebuild that has not landed, rebuilds
    /// are failing, or this silo cannot confirm it has seen every write committed on
    /// the other silos). That request is confirmed against the authoritative
    /// <see cref="ITenantRegistry"/>: the active tenant's record decides whether the
    /// subject may act as it (the tenant exists, is active, and lists the subject),
    /// and for a cross-tenant crossing the owning tenant's record decides the grant,
    /// exactly as <see cref="ReplicationTenantIsolationGate"/> falls back. A removed
    /// admin, a suspended or deleted tenant, and a revoked or rejected grant are
    /// refused at once; an added admin and an approved grant are admitted at once.
    /// A registry failure denies (fail closed).
    /// </remarks>
    public ValueTask<LatticeAccessDecision> EnforceAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        // A silo that has never built its snapshot would report every tenant as
        // unregistered; build it before the first decision (issue #4030). One field
        // read on the steady state.
        if (policy.CurrentEpoch == 0)
        {
            return WarmThenEnforceAsync(request, cancellationToken);
        }

        var decision = Decide(in request);
        return NeedsConfirmation(in decision)
            ? ConfirmAsync(PendingConfirmation.From(in request), cancellationToken)
            : new ValueTask<LatticeAccessDecision>(decision);
    }

    /// <summary>
    /// Builds a cold snapshot, then decides as <see cref="EnforceAsync"/> does. A
    /// warm-up failure other than the caller's own cancellation is logged and the
    /// request is decided against the still-empty snapshot, which admits no
    /// tenant-owned access (fail closed).
    /// </summary>
    private async ValueTask<LatticeAccessDecision> WarmThenEnforceAsync(
        LatticeAccessRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            await policy.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Could not build the compiled tenant-policy snapshot before its first decision; deciding against the empty snapshot.");
        }

        var decision = Decide(in request);
        return NeedsConfirmation(in decision)
            ? await ConfirmAsync(PendingConfirmation.From(in request), cancellationToken).ConfigureAwait(false)
            : decision;
    }

    /// <summary>
    /// The shared decision. Returns the final decision, or - when the request
    /// consumes the asserted active tenant while the compiled snapshot cannot answer
    /// authoritatively - the <see cref="ConfirmationRequired"/> marker, for the
    /// caller to deny (sync) or confirm (async). A marker rather than an
    /// <c>out</c> descriptor keeps the steady-state frame free of a zero-initialised
    /// struct; the pending request is re-derived only on the rare path.
    /// </summary>
    private LatticeAccessDecision Decide(in LatticeAccessRequest request)
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
            // The compiled snapshot is only as current as its last rebuild, and a
            // tenant-registry write (an admin removed or added, a tenant suspended or
            // deleted, a grant approved, rejected or revoked) only SCHEDULES one.
            // Until it lands the snapshot still holds the pre-write state, so
            // trusting it would keep a removed admin acting as the tenant, a
            // suspended or deleted tenant serving traffic, or a revoked grant
            // admitting access, for a whole rebuild - indefinitely if rebuilds keep
            // failing (issues #4001, #4053). IsSnapshotAuthoritative is false exactly
            // in that window, so every decision that consumes the active tenant is
            // handed back for confirmation against the registry (or denied on the
            // synchronous path) before the snapshot is read. The steady state pays
            // the authority check's field reads and allocates nothing.
            if (!policy.IsSnapshotAuthoritative)
            {
                return ConfirmationRequired;
            }

            // (2) Validate that the subject may act as the asserted active tenant
            // BEFORE branching on ownership. The active tenant is a
            // caller-supplied assertion (the `lattice-active-tenant` header),
            // never a fact, so it must be re-validated against the subject's own
            // membership on *every* branch that consumes it. Validating it only
            // on the owned-tree branch left the cross-tenant branch strictly
            // weaker than the owned one: any authenticated subject could assert a
            // tenant it has no membership of and consume that tenant's inbound
            // cross-tenant grants, reading (or, with a write grant, writing) the
            // granting tenant's data. Residency is still gated per branch below.
            var validation = engine.ValidateActiveTenant(subjectId, activeTenant);
            if (!validation.Allowed)
            {
                return Deny(validation.Reason);
            }

            if (activeTenant.Equals(owner.Tenant))
            {
                // (1) The active tenant owns the tree and its selection is
                // validated; gate on residency.
                return EnforceResidency(activeTenant);
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
    /// Confirms a pending decision against the authoritative registry records it
    /// depends on, compiled and decided by the same rules the snapshot path uses:
    /// the subject's right to act as the active tenant
    /// (<see cref="LatticeTenantPolicyEngine.ValidateActiveTenant(CompiledTenantPolicy, string, TenantId)"/>)
    /// against the active tenant's record, and - for a cross-tenant crossing - the
    /// grant (<see cref="LatticeTenantPolicyEngine.ResolveCrossTenantGrant(CompiledTenantPolicy, TenantId, TenantId, string, TenantGrantOperations)"/>)
    /// against the owning tenant's record. Both checks are evaluated independently
    /// and both must pass; neither's success stands in for the other. An owned-tree
    /// request reads one record; a crossing reads its two records concurrently.
    /// Fail-closed: an unregistered tenant denies, and a registry failure other than
    /// the caller's own cancellation denies rather than admitting. Runs only in the
    /// non-authoritative window, so its allocations are off the steady state.
    /// </summary>
    private async ValueTask<LatticeAccessDecision> ConfirmAsync(
        PendingConfirmation pending,
        CancellationToken cancellationToken)
    {
        TenantRecord? activeRecord;
        TenantRecord? ownerRecord;
        try
        {
            if (pending.IsCrossing)
            {
                var activeRead = registry.GetAsync(pending.ActiveTenant, cancellationToken);
                var ownerRead = registry.GetAsync(pending.Owner, cancellationToken);
                await Task.WhenAll(activeRead, ownerRead).ConfigureAwait(false);
                activeRecord = activeRead.Result;
                ownerRecord = ownerRead.Result;
            }
            else
            {
                activeRecord = await registry.GetAsync(pending.ActiveTenant, cancellationToken).ConfigureAwait(false);
                ownerRecord = null;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Could not confirm subject '{SubjectId}' acting as tenant '{ActiveTenant}' on tree '{TreeId}' against the tenant registry while the compiled tenant-policy snapshot was not authoritative; the request was denied.",
                pending.SubjectId,
                pending.ActiveTenant.Value,
                pending.TreeId);
            return DenyUnconfirmed(in pending);
        }

        var confirmed = CompileConfirmed(in pending, activeRecord, ownerRecord);

        var validation = LatticeTenantPolicyEngine.ValidateActiveTenant(
            confirmed,
            pending.SubjectId,
            pending.ActiveTenant);
        if (!validation.Allowed)
        {
            return Deny(validation.Reason);
        }

        if (pending.IsCrossing)
        {
            var grant = LatticeTenantPolicyEngine.ResolveCrossTenantGrant(
                confirmed,
                pending.ActiveTenant,
                pending.Owner,
                pending.TreeId,
                pending.Operation);
            if (!grant.Allowed)
            {
                return Deny(grant.Reason);
            }
        }

        return EnforceResidency(pending.ActiveTenant);
    }

    /// <summary>
    /// Compiles the registry records a confirmation read into a policy. A record is
    /// admitted only under the tenant id it was read for, so a record the registry
    /// returned for a different tenant can neither stand in for the active tenant
    /// nor for the owner (fail closed: the missing tenant then reads unregistered).
    /// </summary>
    private static CompiledTenantPolicy CompileConfirmed(
        in PendingConfirmation pending,
        TenantRecord? activeRecord,
        TenantRecord? ownerRecord)
    {
        var active = activeRecord is not null && activeRecord.Id.Equals(pending.ActiveTenant) ? activeRecord : null;
        var owner = ownerRecord is not null && ownerRecord.Id.Equals(pending.Owner) ? ownerRecord : null;

        return (active, owner) switch
        {
            (null, null) => CompiledTenantPolicy.Empty,
            (not null, null) => CompiledTenantPolicy.Compile([active]),
            (null, not null) => CompiledTenantPolicy.Compile([owner]),
            _ => CompiledTenantPolicy.Compile([active, owner]),
        };
    }

    /// <summary>
    /// The fail-closed denial for a decision that could not be confirmed while the
    /// snapshot is not authoritative.
    /// </summary>
    private static LatticeAccessDecision DenyUnconfirmed(in PendingConfirmation pending) =>
        LatticeAccessDecision.Deny(
            pending.IsCrossing
                ? $"The cross-tenant grant from tenant '{pending.Owner}' to tenant '{pending.ActiveTenant}' "
                    + "could not be confirmed while the tenant-policy snapshot is being rebuilt."
                : $"Subject '{pending.SubjectId}' acting as tenant '{pending.ActiveTenant}' "
                    + "could not be confirmed while the tenant-policy snapshot is being rebuilt.");

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

    /// <summary>
    /// <c>true</c> when <paramref name="decision"/> is the
    /// <see cref="ConfirmationRequired"/> marker rather than a final decision. The
    /// marker's reason is a private instance compared by reference, so no engine
    /// denial can match it.
    /// </summary>
    private static bool NeedsConfirmation(in LatticeAccessDecision decision) =>
        !decision.Allowed && ReferenceEquals(decision.Reason, ConfirmationRequiredReason);

    /// <summary>
    /// A request <see cref="Decide"/> could not answer authoritatively: the subject
    /// acting as its asserted active tenant on a tenant-owned tree - the active
    /// tenant's own tree, or (<see cref="IsCrossing"/>) another tenant's tree across
    /// the ownership boundary - while the compiled snapshot is not authoritative.
    /// </summary>
    private readonly record struct PendingConfirmation(
        TenantId ActiveTenant,
        TenantId Owner,
        string SubjectId,
        string TreeId,
        TenantGrantOperations Operation)
    {
        /// <summary><c>true</c> when the active tenant does not own the tree, so a grant is needed too.</summary>
        public bool IsCrossing => !ActiveTenant.Equals(Owner);

        /// <summary>
        /// Re-derives the request <see cref="Decide"/> marked, from the same
        /// request and the same ambient active tenant it read.
        /// </summary>
        public static PendingConfirmation From(in LatticeAccessRequest request) =>
            new(
                LatticeActiveTenantContext.Current.GetValueOrDefault(),
                LatticeTenantTrees.GetOwner(request.TreeId).Tenant,
                request.Subject.SubjectId,
                request.TreeId,
                ToGrantOperations(request.Operation));
    }
}
