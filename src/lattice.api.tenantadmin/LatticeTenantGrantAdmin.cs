using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of the transport-agnostic
/// <see cref="ILatticeTenantGrantAdmin"/> cross-tenant grant control facade. It
/// is the single narrowest seam at which every step of the two-step grant
/// agreement - offer, approve, reject, revoke - is authorized (fail-closed,
/// tenant-tier, <b>on the side the step belongs to</b>) and applied to the
/// tenancy engine's <see cref="ITenantRegistry"/>; every transport binding is a
/// thin adapter over this one surface. It is a sibling of
/// <see cref="LatticeTenantAccessAdmin"/> and
/// <see cref="LatticeTenantRegionAdmin"/>, added append-only so the tenant
/// lifecycle facade is unchanged.
/// </summary>
/// <remarks>
/// <para>
/// <b>The authorized tenant differs per operation.</b> Every operation goes
/// through
/// <see cref="TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync(TenantId, string, CancellationToken)"/>
/// - the platform-operator <b>or</b> live-admin-subject-of-that-tenant check -
/// and deliberately <em>not</em> through <see cref="TenantAdminAccessAuthorizer"/>,
/// whose identically-named <c>AuthorizeTenantAdminAsync</c> is
/// platform-operator-only and gates the lifecycle mutations. Which tenant it is
/// applied to is the substance of the surface: <b>offer</b> requires the granting
/// tenant's authority (it is offering its own data), <b>approve</b> and
/// <b>reject</b> require the grantee's (it decides what lands in its view), and
/// <b>revoke</b> accepts either party's, so neither is trapped. An admin of one
/// tenant therefore cannot approve a grant offered to another, nor offer one from
/// another.
/// </para>
/// <para>
/// <b>The grant lives on the granting tenant's record.</b> That is where the
/// tenancy engine's cross-tenant resolution reads it from, so there is exactly
/// one copy and no second replica of the agreement to diverge. Where the
/// authorized tenant <em>is</em> the tenant whose record is written (offer, and
/// revoke driven by the granting side), the authorizer's returned record is the
/// record mutated, so the read that proved authority is the only read taken. The
/// grantee-side steps necessarily touch two tenants, and take exactly one extra
/// registry read.
/// </para>
/// <para>
/// <b>Convergent and idempotent.</b> Each transition is stamped with a strictly
/// increasing <see cref="ITenantAdminClock"/> clock and the cluster's writer id,
/// and the record's per-grant CRDT merge joins concurrent transitions from the
/// two parties on the more restrictive outcome, so convergence can never widen
/// access. Asking for the state a grant already holds writes nothing and reports
/// <see cref="TenantGrantChangeResult.Changed"/> <see langword="false"/>.
/// </para>
/// <para>
/// <b>The pre-write guard is not trusted alone.</b> Both parties act
/// independently, so a transition written concurrently by the other side is the
/// normal operating condition rather than an edge case, and the local read a
/// legality check ran against can be stale by the time the write lands. Every
/// mutation therefore re-reads its own outcome from the registry's committed
/// CRDT join and refuses when the converged state is not the one it asked for,
/// so a caller is never handed a success carrying a state it did not request -
/// and the response is always built from that merged record, never from the
/// caller's pre-merge view.
/// </para>
/// </remarks>
internal sealed class LatticeTenantGrantAdmin : ILatticeTenantGrantAdmin
{
    /// <summary>
    /// The surface name interpolated into the authorizer's denial message, so a
    /// refused caller is told which tenant-scoped authority it lacked.
    /// </summary>
    private const string GrantsAction = "cross-tenant grants";

    private readonly ITenantRegistry _registry;
    private readonly TenantRegionResidencyAuthorizer _authorizer;
    private readonly ITenantAdminClock _clock;
    private readonly string? _writerId;

    /// <summary>
    /// Initializes a new <see cref="LatticeTenantGrantAdmin"/>.
    /// </summary>
    /// <param name="registry">The tenancy engine's lifecycle store. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The tenant-tier fail-closed authorization seam. Must not be <c>null</c>.</param>
    /// <param name="clock">The monotonic clock supplying last-writer-wins stamps. Must not be <c>null</c>.</param>
    /// <param name="clusterOptions">The cluster options supplying the writer id stamped on registry writes. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public LatticeTenantGrantAdmin(
        ITenantRegistry registry,
        TenantRegionResidencyAuthorizer authorizer,
        ITenantAdminClock clock,
        IOptions<ClusterOptions> clusterOptions)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(clusterOptions);

        _registry = registry;
        _authorizer = authorizer;
        _clock = clock;
        _writerId = clusterOptions.Value.ClusterId;
    }

    /// <inheritdoc />
    public async Task<TenantGrantReport> ListGrantsAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId, nameof(tenantId));

        // The authorizer returns the record, so the read that proved authority is
        // the same read the issued projection is built from - no second hit.
        var record = await _authorizer
            .AuthorizeTenantAdminAsync(tenant, GrantsAction, cancellationToken)
            .ConfigureAwait(false);

        return new TenantGrantReport
        {
            TenantId = tenant.Value,
            Issued = ProjectIssued(tenant.Value, record),
            Received = await CollectReceivedAsync(tenant, cancellationToken).ConfigureAwait(false),
        };
    }

    /// <inheritdoc />
    public async Task<TenantGrantChangeResult> OfferGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantAccess operations,
        CancellationToken cancellationToken = default)
    {
        var (granter, grantee) = ParsePair(granterTenantId, granteeTenantId);
        ValidateScope(scope);

        var engineOperations = TenantGrantMapping.ToEngine(operations);
        if (engineOperations == TenantGrantOperations.None)
        {
            throw new ArgumentException(
                "A cross-tenant grant must authorize at least one operation this cluster recognises.",
                nameof(operations));
        }

        // Offering exposes the granting tenant's own data, so it is that tenant's
        // admins (or an operator) who may do it - and their record is the one the
        // offer is written to, so this single read serves both.
        var record = await _authorizer
            .AuthorizeTenantAdminAsync(granter, GrantsAction, cancellationToken)
            .ConfigureAwait(false);

        ThrowIfReservedTenant(granter, "offer-cross-tenant-grant");
        ThrowIfReservedTenant(grantee, "offer-cross-tenant-grant");

        var offered = CrossTenantGrant.Create(
            grantee.Value, TenantGranteeKind.Tenant, scope, engineOperations, TenantGrantState.Pending);

        // CrossTenantGrant.GrantId is a computed property that builds its string on
        // every access, so it is read once and reused across the lookup and the
        // commit rather than rebuilt per use.
        var grantId = offered.GrantId;

        if (record.TryGetGrant(grantId, out var existing))
        {
            if (!TenantGrantLifecycle.IsLegalOffer(existing.State))
            {
                throw new TenantGrantTransitionException(
                    granter.Value,
                    grantee.Value,
                    scope,
                    TenantGrantMapping.ToContract(existing.State),
                    TenantGrantLifecycleState.Pending);
            }

            // Idempotent no-op: the identical offer already stands unanswered, so
            // re-sending it must not disturb the grantee's inbox or the stamp.
            if (existing.State == TenantGrantState.Pending && existing.Operations == engineOperations)
            {
                return Unchanged(granter.Value, existing);
            }
        }

        record.OfferGrant(offered, _clock.Next(), _writerId);
        return await CommitAsync(
                record, granter, grantee, scope, grantId, TenantGrantState.Pending, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> ApproveGrantAsync(
        string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)
        => TransitionFromGranteeSideAsync(
            granterTenantId, granteeTenantId, scope, TenantGrantState.Active, cancellationToken);

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> RejectGrantAsync(
        string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)
        => TransitionFromGranteeSideAsync(
            granterTenantId, granteeTenantId, scope, TenantGrantState.Rejected, cancellationToken);

    /// <inheritdoc />
    public async Task<TenantGrantChangeResult> RevokeGrantAsync(
        string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)
    {
        var (granter, grantee) = ParsePair(granterTenantId, granteeTenantId);
        ValidateScope(scope);

        // Either party may walk away, so the granting side is probed first with
        // the non-throwing overload: a null result is a denial, not an
        // authorization, and the grantee side is then authorized through the
        // throwing overload so a caller party to neither tenant is refused.
        var record = await _authorizer
            .TryAuthorizeTenantAdminAsync(granter, cancellationToken)
            .ConfigureAwait(false);

        if (record is null)
        {
            await _authorizer
                .AuthorizeTenantAdminAsync(grantee, GrantsAction, cancellationToken)
                .ConfigureAwait(false);

            record = await _registry.GetAsync(granter, cancellationToken).ConfigureAwait(false)
                ?? throw new TenantGrantNotFoundException(granter.Value, grantee.Value, scope);
        }

        return await TransitionAsync(
            record, granter, grantee, scope, TenantGrantState.Revoked, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Drives a transition only the <b>grantee</b> tenant may make (approve and
    /// reject). The grantee's own record is what proves authority, while the grant
    /// itself lives on the granting tenant's record, so this path necessarily
    /// takes one further registry read. A granting tenant that is not registered
    /// is reported exactly as an unoffered grant, so the grantee's admins cannot
    /// use the surface to probe for tenants.
    /// </summary>
    private async Task<TenantGrantChangeResult> TransitionFromGranteeSideAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantState target,
        CancellationToken cancellationToken)
    {
        var (granter, grantee) = ParsePair(granterTenantId, granteeTenantId);
        ValidateScope(scope);

        await _authorizer
            .AuthorizeTenantAdminAsync(grantee, GrantsAction, cancellationToken)
            .ConfigureAwait(false);

        var record = await _registry.GetAsync(granter, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantGrantNotFoundException(granter.Value, grantee.Value, scope);

        return await TransitionAsync(record, granter, grantee, scope, target, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Applies an already-authorized lifecycle transition to the grant held on
    /// <paramref name="record"/>: an absent grant is a not-found, the state the
    /// grant already holds is an idempotent no-op, and anything the lifecycle's
    /// legal-transition set refuses raises
    /// <see cref="TenantGrantTransitionException"/> before any write.
    /// </summary>
    private async Task<TenantGrantChangeResult> TransitionAsync(
        TenantRecord record,
        TenantId granter,
        TenantId grantee,
        string scope,
        TenantGrantState target,
        CancellationToken cancellationToken)
    {
        var grantId = GrantIdFor(grantee, scope);

        if (!record.TryGetGrant(grantId, out var existing))
        {
            throw new TenantGrantNotFoundException(granter.Value, grantee.Value, scope);
        }

        if (existing.State == target)
        {
            return Unchanged(granter.Value, existing);
        }

        if (!TenantGrantLifecycle.IsLegalTransition(existing.State, target))
        {
            throw new TenantGrantTransitionException(
                granter.Value,
                grantee.Value,
                scope,
                TenantGrantMapping.ToContract(existing.State),
                TenantGrantMapping.ToContract(target));
        }

        record.TransitionGrant(grantId, target, _clock.Next(), _writerId);
        return await CommitAsync(record, granter, grantee, scope, grantId, target, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Commits a mutated record and projects the grant <em>as merged</em>. The
    /// registry's put is a CRDT read-merge-write returning the committed join, so
    /// the response reports the converged state - which may be the other party's
    /// concurrent, more restrictive transition rather than this call's own - and
    /// never this caller's optimistic pre-merge view.
    /// </summary>
    private async Task<TenantGrantChangeResult> CommitAsync(
        TenantRecord record,
        TenantId granter,
        TenantId grantee,
        string scope,
        string grantId,
        TenantGrantState target,
        CancellationToken cancellationToken)
    {
        var merged = await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        if (!merged.TryGetGrant(grantId, out var committed))
        {
            // The grant was hard-removed from the record concurrently, so there is
            // nothing left to report a state for.
            throw new TenantGrantNotFoundException(granter.Value, grantee.Value, scope);
        }

        // The pre-write legality check ran against this caller's own read, so a
        // transition written concurrently by the other party - the normal case in a
        // two-party agreement, not an edge case - is only observable here, on the
        // registry's committed join. The join always keeps the more restrictive
        // outcome, so the merged state is never wider than either intent and there
        // is nothing to repair; but this call's intent did not take effect, so it
        // is refused rather than reported as a success carrying a state the caller
        // never asked for. The party whose intent won still succeeds, and a retry
        // of the refused call is stopped by the pre-write guard, so this
        // terminates.
        if (committed.State != target)
        {
            throw new TenantGrantTransitionException(
                granter.Value,
                grantee.Value,
                scope,
                TenantGrantMapping.ToContract(committed.State),
                TenantGrantMapping.ToContract(target));
        }

        return new TenantGrantChangeResult
        {
            Grant = TenantGrantMapping.Describe(granter.Value, committed),
            Changed = true,
        };
    }

    /// <summary>
    /// Sweeps the registry for the grants other tenants have offered to
    /// <paramref name="tenant"/> - its inbox. The grant lives only on the granting
    /// tenant's record, so there is no index to read it from and no second copy
    /// that could diverge from it. A tenant holding no grants is skipped on a
    /// counted probe that materialises nothing, so a registry of grant-free
    /// tenants costs no allocation at all.
    /// </summary>
    private async Task<IReadOnlyList<TenantGrantDescriptor>> CollectReceivedAsync(
        TenantId tenant, CancellationToken cancellationToken)
    {
        List<TenantGrantDescriptor>? received = null;

        await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (record.GrantCount == 0)
            {
                continue;
            }

            var granterTenantId = record.Id.Value;
            foreach (var grant in record.Grants)
            {
                if (grant.GranteeKind != TenantGranteeKind.Tenant
                    || !string.Equals(grant.Grantee, tenant.Value, StringComparison.Ordinal))
                {
                    continue;
                }

                received ??= [];
                received.Add(TenantGrantMapping.Describe(granterTenantId, grant));
            }
        }

        if (received is null)
        {
            return [];
        }

        // The registry enumerates in no defined order, so the inbox is sorted by
        // granting tenant and then grant id. That makes the projection a pure
        // function of the registry's contents rather than of its iteration order.
        received.Sort(static (a, b) =>
        {
            var byTenant = string.CompareOrdinal(a.GranterTenantId, b.GranterTenantId);
            return byTenant != 0 ? byTenant : string.CompareOrdinal(a.GrantId, b.GrantId);
        });

        return received;
    }

    /// <summary>
    /// Projects the tenant-to-tenant grants <paramref name="record"/> issued.
    /// <see cref="TenantRecord.Grants"/> is already ordered by grant id, so the
    /// projection preserves that order without a second sort. Subject-grantee
    /// grants are excluded: they have no counterparty tenant able to approve them
    /// and are not administered through this surface.
    /// </summary>
    private static IReadOnlyList<TenantGrantDescriptor> ProjectIssued(string tenantId, TenantRecord record)
    {
        if (record.GrantCount == 0)
        {
            return [];
        }

        List<TenantGrantDescriptor>? issued = null;
        foreach (var grant in record.Grants)
        {
            if (grant.GranteeKind != TenantGranteeKind.Tenant)
            {
                continue;
            }

            issued ??= [];
            issued.Add(TenantGrantMapping.Describe(tenantId, grant));
        }

        return issued ?? (IReadOnlyList<TenantGrantDescriptor>)[];
    }

    private static TenantGrantChangeResult Unchanged(string granterTenantId, CrossTenantGrant grant) =>
        new()
        {
            Grant = TenantGrantMapping.Describe(granterTenantId, grant),
            Changed = false,
        };

    /// <summary>
    /// The registry key of the grant <paramref name="grantee"/> holds on
    /// <paramref name="scope"/>. Derived through the grant type's own identity
    /// rule so the facade can never drift from how the record keys its slots; the
    /// carrier is a struct, so only the id string itself is allocated.
    /// </summary>
    private static string GrantIdFor(TenantId grantee, string scope) =>
        CrossTenantGrant.Create(
            grantee.Value, TenantGranteeKind.Tenant, scope, TenantGrantOperations.None).GrantId;

    /// <summary>
    /// Rejects a grant operation naming the reserved default tenant on either
    /// side. It names the cluster's own legacy state, so a cross-tenant grant
    /// to or from it would expose the whole legacy keyspace across a tenant
    /// boundary. The reserved id is a constant, so the refusal leaks nothing about
    /// registry contents.
    /// </summary>
    private static void ThrowIfReservedTenant(TenantId tenant, string operation)
    {
        if (tenant.IsDefault)
        {
            throw new ReservedTenantOperationException(tenant.Value, operation);
        }
    }

    private static void ValidateScope(string scope)
    {
        if (string.IsNullOrWhiteSpace(scope))
        {
            throw new ArgumentException(
                "A cross-tenant grant scope must not be null, empty, or whitespace.", nameof(scope));
        }
    }

    /// <summary>
    /// Parses and validates the two tenant ids every grant operation names. The
    /// two must differ: a grant from a tenant to itself is meaningless, and would
    /// collapse the two-sided authorization rule into a single side.
    /// </summary>
    private static (TenantId Granter, TenantId Grantee) ParsePair(
        string granterTenantId, string granteeTenantId)
    {
        var granter = ParseTenant(granterTenantId, nameof(granterTenantId));
        var grantee = ParseTenant(granteeTenantId, nameof(granteeTenantId));

        if (granter.Equals(grantee))
        {
            throw new ArgumentException(
                "A cross-tenant grant must name two different tenants.", nameof(granteeTenantId));
        }

        return (granter, grantee);
    }

    private static TenantId ParseTenant(string tenantId, string parameterName)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId, parameterName);
        if (!TenantId.TryParse(tenantId, out var tenant))
        {
            throw new ArgumentException($"'{tenantId}' is not a valid tenant id.", parameterName);
        }

        return tenant;
    }
}
