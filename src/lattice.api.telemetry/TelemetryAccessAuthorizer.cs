using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The fail-closed authorization seam the telemetry facade consults: the coarse
/// <see cref="LatticeOperation.Telemetry"/> capability that gates the surface at
/// all, and the platform-operator validation that decides whether a caller's
/// request to see beyond its own tenant may be honoured.
/// </summary>
/// <remarks>
/// <para>
/// <b>Both checks authorize over the reserved auth policy tree.</b> Telemetry
/// addresses no single tree, and neither does platform-operator authority. Scoping
/// them to <see cref="LatticeAuthReservedTrees.PolicyTreeId"/> rather than the
/// data-plane all-trees sentinel routes them through the gate's control-plane
/// isolation, which denies an unmatched request regardless of the data-plane
/// default effect - so neither can fail open under
/// <c>LatticeAuthOptions.DefaultEffect = Allow</c>. Authorizing over <c>"*"</c>
/// would take the ordinary data-plane path and hand an elevated cluster-wide
/// capability to an anonymous caller on any permissively configured host.
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
    /// confers it.
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
            LatticeAuthReservedTrees.PolicyTreeId, LatticeOperation.Telemetry, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        if (!decision.Allowed)
        {
            throw new LatticeAuthorizationDeniedException(
                LatticeAuthReservedTrees.PolicyTreeId,
                LatticeOperation.Telemetry,
                subject.SubjectId,
                decision.Reason ?? "Reading cluster telemetry requires the Telemetry capability.");
        }

        if (decision.KeyFilter is not null)
        {
            throw new LatticeAuthorizationDeniedException(
                LatticeAuthReservedTrees.PolicyTreeId,
                LatticeOperation.Telemetry,
                subject.SubjectId,
                decision.Reason ?? "Cluster telemetry is not attached to a key, so a key-filtered "
                    + "allow does not authorize it and is refused.");
        }
    }

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
