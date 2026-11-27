using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The fail-closed authorization seam the telemetry facade consults: the coarse
/// <see cref="LatticeOperation.Telemetry"/> capability that gates the surface at
/// all - cluster-wide, or confined to one tenant - and the platform-operator
/// validation that decides whether a caller's request to see beyond its own tenant
/// may be honoured.
/// </summary>
/// <remarks>
/// <para>
/// <b>The capability check authorizes over the cluster-wide sentinel; the operator
/// check over the reserved auth policy tree.</b> Telemetry addresses no single tree,
/// so its capability is authorized over <see cref="LatticeScope.ClusterWideTreeId"/> -
/// exactly the scope <see cref="LatticeScope.ClusterWide"/> authors, so a grant
/// written the documented way is honoured (issue #1795). Platform-operator authority
/// is a different thing - authority over the gate itself - and stays on
/// <see cref="LatticeAuthReservedTrees.PolicyTreeId"/>. Both route through the gate's
/// control-plane isolation, which denies an unmatched request regardless of the
/// data-plane default effect, so neither can fail open under
/// <c>LatticeAuthOptions.DefaultEffect = Allow</c>: the gate treats a request on the
/// sentinel as a scopeless capability request rather than a data-plane one, because
/// a data-plane read or write always names a real tree.
/// </para>
/// <para>
/// <b>A filtered allow is refused.</b> Neither capability is attached to a key, so
/// a gate returning a per-key filter has not authorized the whole scope; that is
/// treated as a denial rather than narrowed.
/// </para>
/// <para>
/// <b>The two checks differ on a missing gate, deliberately.</b> With no
/// <see cref="ILatticeAccessGate"/> registered at all the capability check admits,
/// inheriting the repository-wide zero-cost default that leaves an authorization-off
/// cluster byte-for-byte unchanged; the operator check denies, because widening a
/// query beyond the caller's own tenant is honoured "only after server-side
/// validation" and validation is impossible without the seam. Failing closed there
/// costs nothing - the caller still gets its own tenant's series.
/// </para>
/// </remarks>
public sealed class TelemetryAccessAuthorizer
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, string> CapabilityIds =
        new(StringComparer.Ordinal);

    private readonly ILatticeAccessGate? _gate;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>
    /// Initializes the authorizer.
    /// </summary>
    /// <param name="gate">
    /// The registered core access gate, or <see langword="null"/> when none is
    /// registered. In a host with the authorization add-on absent this is the core
    /// no-op gate, so every check short-circuits to allow at zero cost.
    /// </param>
    /// <param name="membership">
    /// The membership context used to resolve the caller subject, or
    /// <see langword="null"/> when none is registered (every caller then resolves to
    /// <see cref="LatticeSubject.Anonymous"/>).
    /// </param>
    public TelemetryAccessAuthorizer(
        ILatticeAccessGate? gate = null,
        ILatticeMembershipContext? membership = null)
    {
        _gate = gate;
        _membership = membership;
    }

    /// <summary>
    /// Authorizes the caller for the cluster-wide
    /// <see cref="LatticeOperation.Telemetry"/> capability, throwing when it is not
    /// granted. No other operation - not even <see cref="LatticeOperation.Admin"/> -
    /// confers it. The check targets <see cref="LatticeScope.ClusterWideTreeId"/>, so
    /// the grant <see cref="LatticeScope.ClusterWide"/> authors is the one that
    /// authorizes it.
    /// </summary>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the caller may read cluster telemetry.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized for cluster telemetry.</exception>
    public async ValueTask AuthorizeClusterTelemetryAsync(CancellationToken cancellationToken = default)
    {
        if (LatticeSystemOrigin.IsActive || _gate is null)
        {
            return;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        var request = new LatticeAccessRequest(
            LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        if (!decision.Allowed)
        {
            throw new LatticeAuthorizationDeniedException(
                LatticeScope.ClusterWideTreeId,
                LatticeOperation.Telemetry,
                subject.SubjectId,
                decision.Reason ?? "Reading cluster telemetry requires the Telemetry capability.");
        }

        if (decision.KeyFilter is not null)
        {
            throw new LatticeAuthorizationDeniedException(
                LatticeScope.ClusterWideTreeId,
                LatticeOperation.Telemetry,
                subject.SubjectId,
                decision.Reason ?? "Cluster telemetry is not attached to a key, so a key-filtered "
                    + "allow does not authorize it and is refused.");
        }
    }

    /// <summary>
    /// Authorizes the caller for <see cref="LatticeOperation.Telemetry"/> confined to
    /// one tenant: the grant <see cref="LatticeScope.TenantCapability(TenantId)"/>
    /// authors, on the tenant's control-plane capability id
    /// (<see cref="LatticeTenantAdminScope.TenantScopePrefix"/> plus the tenant id).
    /// Throws when it is not granted. No other operation on that id - not even
    /// <see cref="LatticeOperation.Admin"/>, the delegated per-tenant admin capability
    /// it also carries - confers it.
    /// </summary>
    /// <remarks>
    /// <paramref name="tenant"/> must be the caller's <em>validated</em> active tenant,
    /// never a request field. The reserved default tenant and the uninitialised
    /// "no tenant" value are refused, because neither has a tenant capability scope;
    /// a key-filtered allow is refused as for the cluster-wide check.
    /// </remarks>
    /// <param name="tenant">The caller's validated active tenant.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the caller may read the tenant's telemetry.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized for the tenant's telemetry.</exception>
    public async ValueTask AuthorizeTenantTelemetryAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        if (tenant.Value is null || tenant.IsDefault)
        {
            var anonymous = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
            throw new LatticeAuthorizationDeniedException(
                LatticeScope.ClusterWideTreeId,
                LatticeOperation.Telemetry,
                anonymous.SubjectId,
                "Tenant-scoped telemetry needs a tenant other than the reserved default tenant; "
                    + "reading this telemetry requires the cluster-wide Telemetry capability.");
        }

        if (LatticeSystemOrigin.IsActive || _gate is null)
        {
            return;
        }

        var scopeId = CapabilityIdOf(tenant.Value);
        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        var request = new LatticeAccessRequest(scopeId, LatticeOperation.Telemetry, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        if (!decision.Allowed)
        {
            throw new LatticeAuthorizationDeniedException(
                scopeId,
                LatticeOperation.Telemetry,
                subject.SubjectId,
                decision.Reason ?? $"Reading tenant {tenant.Value}'s telemetry requires the Telemetry capability "
                    + "on that tenant's capability scope, or cluster-wide.");
        }

        if (decision.KeyFilter is not null)
        {
            throw new LatticeAuthorizationDeniedException(
                scopeId,
                LatticeOperation.Telemetry,
                subject.SubjectId,
                decision.Reason ?? "Tenant telemetry is not attached to a key, so a key-filtered "
                    + "allow does not authorize it and is refused.");
        }
    }

    /// <summary>
    /// <see langword="true"/> when the cluster-wide check
    /// (<see cref="AuthorizeClusterTelemetryAsync"/>) would admit the caller. Never
    /// throws for a denial, so the composed decision in
    /// <see cref="TelemetryTenantScopeResolver.AuthorizeAsync"/> pays for no exception
    /// on the steady-state path of a tenant-confined caller.
    /// </summary>
    internal async ValueTask<bool> IsClusterTelemetryGrantedAsync(CancellationToken cancellationToken)
    {
        if (LatticeSystemOrigin.IsActive || _gate is null)
        {
            return true;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        var request = new LatticeAccessRequest(
            LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
        return decision.Allowed && decision.KeyFilter is null;
    }

    /// <summary>
    /// <see langword="true"/> when <see cref="AuthorizeTenantTelemetryAsync"/> would
    /// admit the caller for <paramref name="tenant"/>. Never throws for a denial; the
    /// default and uninitialised tenants are never granted.
    /// </summary>
    internal async ValueTask<bool> IsTenantTelemetryGrantedAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        if (tenant.Value is null || tenant.IsDefault)
        {
            return false;
        }

        if (LatticeSystemOrigin.IsActive || _gate is null)
        {
            return true;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        var request = new LatticeAccessRequest(CapabilityIdOf(tenant.Value), LatticeOperation.Telemetry, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
        return decision.Allowed && decision.KeyFilter is null;
    }

    // The tenant capability id, composed once per tenant for the process lifetime so a
    // tenant-confined caller's every query does not allocate it. Bounded by the number
    // of distinct validated tenants that have read telemetry.
    private static string CapabilityIdOf(string tenantId) =>
        CapabilityIds.GetOrAdd(tenantId, static id => string.Concat(LatticeTenantAdminScope.TenantScopePrefix, id));

    /// <summary>
    /// <see langword="true"/> when the caller validates as a platform operator -
    /// granted <see cref="LatticeOperation.Admin"/> on the reserved auth policy
    /// tree. Never throws: an unvalidated caller degrades to its own tenant rather
    /// than being refused.
    /// </summary>
    /// <param name="cancellationToken">Cancels the check.</param>
    /// <returns><see langword="true"/> when the caller is a validated platform operator.</returns>
    public async ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default)
    {
        // Trusted co-hosted infrastructure runs system-origin and is not an external
        // caller at all, matching every sibling control-plane authorizer.
        if (LatticeSystemOrigin.IsActive)
        {
            return true;
        }

        if (_gate is null)
        {
            return false;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        var request = new LatticeAccessRequest(
            LatticeAuthReservedTrees.PolicyTreeId, LatticeOperation.Admin, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        return decision.Allowed && decision.KeyFilter is null;
    }

    private ValueTask<LatticeSubject> ResolveSubjectAsync(CancellationToken cancellationToken)
    {
        if (_membership is null)
        {
            return new ValueTask<LatticeSubject>(LatticeSubject.Anonymous);
        }

        if (_membership.TryResolveCurrent(out var subject))
        {
            return new ValueTask<LatticeSubject>(subject);
        }

        return ResolveUncachedAsync(cancellationToken);
    }

    private async ValueTask<LatticeSubject> ResolveUncachedAsync(CancellationToken cancellationToken)
    {
        // Resolution may read the dogfooded membership directory, which must not
        // re-enter the gate, so it runs system-origin exactly as the sibling
        // control-plane authorizers do.
        using (LatticeSystemOrigin.Enter())
        {
            return await _membership!.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
