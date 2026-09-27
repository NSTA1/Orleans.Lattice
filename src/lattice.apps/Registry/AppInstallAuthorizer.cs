using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The fail-closed authorization check every app-registry lifecycle transition runs
/// before it touches storage: the caller must hold the scopeless
/// <see cref="LatticeOperation.AppInstall"/> capability over
/// <see cref="LatticeScope.ClusterWideTreeId"/> - exactly the scope
/// <see cref="LatticeScope.ClusterWide"/> authors - evaluated through the shared
/// <see cref="ILatticeAccessGate"/>, as the telemetry capability is.
/// </summary>
/// <remarks>
/// <para>
/// The gate routes a request on the cluster-wide sentinel through its control-plane
/// branch, so an unmatched request is denied even under a data-plane default effect of
/// allow, and a whole-data-plane grant never confers <c>AppInstall</c> (it is excluded
/// from <see cref="LatticeAuthOperations.All"/>). A key-filtered allow is refused: the
/// capability is not attached to a key. Trusted co-hosted infrastructure running
/// system-origin skips the check, like every sibling control-plane authorizer.
/// </para>
/// <para>
/// The gate is a required collaborator, so there is no null-gate path that could fail
/// open. A host without the authorization add-on carries the core no-op gate, and so
/// admits, consistent with every other capability in such a host.
/// </para>
/// </remarks>
internal sealed class AppInstallAuthorizer
{
    private const string DeniedReason = "Changing an app's lifecycle requires the AppInstall capability.";
    private const string FilteredReason =
        "The AppInstall capability is not attached to a key, so a key-filtered allow does not authorize it and is refused.";

    private readonly ILatticeAccessGate _gate;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>Initializes a new <see cref="AppInstallAuthorizer"/>.</summary>
    /// <param name="gate">The shared access gate. Must not be <c>null</c>.</param>
    /// <param name="membership">
    /// The membership context used to resolve the caller, or <c>null</c> when none is
    /// registered (every caller then resolves to <see cref="LatticeSubject.Anonymous"/>).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="gate"/> is <c>null</c>.</exception>
    public AppInstallAuthorizer(ILatticeAccessGate gate, ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(gate);
        _gate = gate;
        _membership = membership;
    }

    /// <summary>
    /// Authorizes the ambient caller for <see cref="LatticeOperation.AppInstall"/>,
    /// throwing when it is not granted.
    /// </summary>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>
    /// The authorized caller's subject id, recorded as the consenting principal, or
    /// <c>null</c> when the caller is trusted system-origin infrastructure.
    /// </returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized.</exception>
    public async ValueTask<string?> AuthorizeAsync(CancellationToken cancellationToken)
    {
        if (LatticeSystemOrigin.IsActive)
        {
            return null;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        var request = new LatticeAccessRequest(LatticeScope.ClusterWideTreeId, LatticeOperation.AppInstall, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        if (!decision.Allowed)
        {
            throw new LatticeAuthorizationDeniedException(
                LatticeScope.ClusterWideTreeId,
                LatticeOperation.AppInstall,
                subject.SubjectId,
                decision.Reason ?? DeniedReason);
        }

        if (decision.KeyFilter is not null)
        {
            throw new LatticeAuthorizationDeniedException(
                LatticeScope.ClusterWideTreeId,
                LatticeOperation.AppInstall,
                subject.SubjectId,
                FilteredReason);
        }

        return subject.SubjectId;
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

        return ResolveUncachedAsync(_membership, cancellationToken);
    }

    private static async ValueTask<LatticeSubject> ResolveUncachedAsync(
        ILatticeMembershipContext membership,
        CancellationToken cancellationToken)
    {
        // Resolution may read the dogfooded membership directory, which must not
        // re-enter the gate, so it runs system-origin as the sibling control-plane
        // authorizers do.
        using (LatticeSystemOrigin.Enter())
        {
            return await membership.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
