namespace Orleans.Lattice;

/// <summary>
/// The single narrowest core seam that resolves the platform-operator versus
/// delegated-per-tenant-admin distinction against the registered
/// <see cref="ILatticeAccessGate"/>. It turns a <see cref="LatticeTenantAdminScope"/>
/// into an <see cref="LatticeOperation.Admin"/> request, evaluates it, and applies the
/// fail-closed rule that a whole-scope administrative capability may never be narrowed
/// to a subset of keys.
/// </summary>
/// <remarks>
/// <para>
/// Consolidating the check here means every caller that needs "is this subject a
/// platform operator / a delegated admin for tenant T?" enforces it identically at one
/// seam, rather than each re-deriving the request shape and the fail-closed handling.
/// The seam takes an already-resolved <see cref="LatticeSubject"/>; resolving the
/// caller from its credential, and any system-origin bypass, are the transport /
/// facade's concern and are deliberately not repeated here.
/// </para>
/// <para>
/// <b>Fail-closed.</b> A tenant-administration capability is a whole-scope grant, so a
/// gate decision that allows only a filtered subset of keys
/// (<see cref="LatticeAccessDecision.KeyFilter"/> is non-<c>null</c>) is treated as a
/// denial, not a partial grant. With no auth add-on registered the core gate allows
/// every request, so - exactly as for the rest of the access-gate surface - these
/// checks short-circuit to allow and core behaviour is unchanged until a real,
/// policy-evaluating gate is registered.
/// </para>
/// </remarks>
public sealed class LatticeTenantAdminAuthorizer
{
    private readonly ILatticeAccessGate _gate;

    /// <summary>
    /// Initialises a new <see cref="LatticeTenantAdminAuthorizer"/> over the supplied
    /// access gate.
    /// </summary>
    /// <param name="gate">The registered access gate the authorization decision is delegated to. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="gate"/> is <c>null</c>.</exception>
    public LatticeTenantAdminAuthorizer(ILatticeAccessGate gate)
    {
        ArgumentNullException.ThrowIfNull(gate);
        _gate = gate;
    }

    /// <summary>
    /// Decides whether <paramref name="subject"/> holds the administrative capability
    /// named by <paramref name="scope"/>.
    /// </summary>
    /// <param name="scope">The platform-wide or per-tenant admin scope to check.</param>
    /// <param name="subject">The resolved caller identity, or <see cref="LatticeSubject.Anonymous"/>.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns><c>true</c> when the gate allows the whole scope; <c>false</c> when it denies, or allows only a key-filtered subset (fail-closed).</returns>
    public async ValueTask<bool> IsAuthorizedAsync(
        LatticeTenantAdminScope scope,
        LatticeSubject subject,
        CancellationToken cancellationToken = default)
    {
        var request = scope.ToAdminRequest(subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
        return decision.Allowed && decision.KeyFilter is null;
    }

    /// <summary>
    /// Authorizes <paramref name="subject"/> for the administrative capability named by
    /// <paramref name="scope"/>, throwing when the capability is not held.
    /// </summary>
    /// <param name="scope">The platform-wide or per-tenant admin scope to enforce.</param>
    /// <param name="subject">The resolved caller identity, or <see cref="LatticeSubject.Anonymous"/>.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the capability is held.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The gate denies the request, or allows only a key-filtered subset (fail-closed).</exception>
    public async ValueTask AuthorizeAsync(
        LatticeTenantAdminScope scope,
        LatticeSubject subject,
        CancellationToken cancellationToken = default)
    {
        var request = scope.ToAdminRequest(subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
        if (decision.Allowed && decision.KeyFilter is null)
        {
            return;
        }

        throw new LatticeAuthorizationDeniedException(
            scope.TreeScope,
            LatticeOperation.Admin,
            subject.SubjectId,
            decision.Reason ?? "The caller does not hold the required tenant-administration capability.");
    }
}
