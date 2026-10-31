namespace Orleans.Lattice;

/// <summary>
/// The per-silo seam that resolves the ambient caller credential into a
/// <see cref="LatticeSubject"/>. The interface lives in core so the later
/// access-gate seam can resolve the current subject without depending on the
/// Membership package; the core library ships only the
/// <see cref="NullLatticeMembershipContext"/> anonymous fallback and the real
/// implementation is contributed by <c>AddLatticeMembership</c>.
/// </summary>
public interface ILatticeMembershipContext
{
    /// <summary>
    /// Resolves the current caller (from the ambient
    /// <see cref="LatticeCredentialContext"/>) into a <see cref="LatticeSubject"/>
    /// whose <see cref="LatticeSubject.GroupIds"/> carry the full transitive
    /// group closure. Returns <see cref="LatticeSubject.Anonymous"/> when no
    /// credential is present or the credential cannot be resolved.
    /// </summary>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The resolved subject, or <see cref="LatticeSubject.Anonymous"/>.</returns>
    ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to resolve the current caller <em>synchronously</em>, without any
    /// directory read - served from a warm resolution cache, or an immediate
    /// <see cref="LatticeSubject.Anonymous"/> when no credential is present. Lets
    /// the access-gate enforcement point skip the gate-bypass scope (an ambient
    /// request-context round-trip) on the warm path, which is only needed to
    /// protect the dogfooded directory reads a cache miss performs.
    /// </summary>
    /// <param name="subject">
    /// The resolved subject when this returns <see langword="true"/>; otherwise
    /// <c>default</c>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the subject was resolved synchronously;
    /// <see langword="false"/> when an asynchronous (directory-reading) resolution
    /// via <see cref="ResolveCurrentAsync"/> is required. The default
    /// implementation always returns <see langword="false"/>, so a context that
    /// cannot resolve synchronously safely falls back to the async path.
    /// </returns>
    bool TryResolveCurrent(out LatticeSubject subject)
    {
        subject = default;
        return false;
    }

    /// <summary>
    /// Reports what the cluster made of the credential on the ambient
    /// <see cref="LatticeCredentialContext"/>: absent, accepted, refused by the
    /// authenticator that recognised it, or unverified because nothing
    /// recognises it. Unlike <see cref="ResolveCurrentAsync"/>, which folds a
    /// refused credential into <see cref="LatticeSubject.Anonymous"/>, this lets
    /// a transport binding answer a refused credential as unauthenticated.
    /// </summary>
    /// <remarks>
    /// The default implementation cannot tell a refused credential from an
    /// unrecognised one, so it reports <see cref="LatticeCredentialVerdict.Absent"/>
    /// or <see cref="LatticeCredentialVerdict.Unverified"/> and never
    /// <see cref="LatticeCredentialVerdict.Refused"/>: a context that does not
    /// implement this leaves every caller exactly as it resolves today. Call it
    /// through <see cref="LatticeCredentialVerification.VerifyCurrentAsync"/>,
    /// which runs any directory read this needs under the gate-bypass scope.
    /// </remarks>
    /// <param name="cancellationToken">Cancels the verification.</param>
    /// <returns>The verdict on the ambient credential.</returns>
    ValueTask<LatticeCredentialVerdict> VerifyCurrentCredentialAsync(CancellationToken cancellationToken = default) =>
        new(LatticeCredentialContext.IsActive ? LatticeCredentialVerdict.Unverified : LatticeCredentialVerdict.Absent);
}
