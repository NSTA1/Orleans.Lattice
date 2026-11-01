namespace Orleans.Lattice;

/// <summary>
/// Verifies the credential on the ambient <see cref="LatticeCredentialContext"/>
/// through a registered <see cref="ILatticeMembershipContext"/>, so a transport
/// binding can refuse a credential the cluster does not accept before it serves
/// the call - rather than letting it fall through to
/// <see cref="LatticeSubject.Anonymous"/>, where a refused credential and a
/// caller who presented none look the same.
/// </summary>
/// <remarks>
/// Verification resolves the caller through the membership resolution cache, so a
/// warm credential costs a cache lookup. On a cache miss the resolution reads the
/// membership directory's own trees through the gated surface, so that read runs
/// under the same gate-bypass scope the access-gate enforcement point uses.
/// </remarks>
public static class LatticeCredentialVerification
{
    private static readonly ValueTask<LatticeCredentialVerdict> AbsentResult = new(LatticeCredentialVerdict.Absent);
    private static readonly ValueTask<LatticeCredentialVerdict> UnverifiedResult = new(LatticeCredentialVerdict.Unverified);

    /// <summary>
    /// Verifies the ambient credential through <paramref name="membership"/>.
    /// Reports <see cref="LatticeCredentialVerdict.Absent"/> when no credential is
    /// presented and <see cref="LatticeCredentialVerdict.Unverified"/> when
    /// <paramref name="membership"/> is <see langword="null"/>, so a host with no
    /// membership context registered keeps today's behaviour.
    /// </summary>
    /// <param name="membership">The registered membership context, or <see langword="null"/> when none is registered.</param>
    /// <param name="cancellationToken">Cancels the verification.</param>
    /// <returns>The verdict on the ambient credential.</returns>
    public static ValueTask<LatticeCredentialVerdict> VerifyCurrentAsync(
        ILatticeMembershipContext? membership,
        CancellationToken cancellationToken = default)
    {
        if (!LatticeCredentialContext.IsActive)
        {
            return AbsentResult;
        }

        if (membership is null)
        {
            return UnverifiedResult;
        }

        // Warm path: a cached subject verifies with no directory read, so the
        // gate-bypass scope (an ambient request-context round-trip) is skipped.
        if (membership.TryResolveCurrent(out _))
        {
            return membership.VerifyCurrentCredentialAsync(cancellationToken);
        }

        return VerifyUncachedAsync(membership, cancellationToken);
    }

    private static async ValueTask<LatticeCredentialVerdict> VerifyUncachedAsync(
        ILatticeMembershipContext membership,
        CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await membership.VerifyCurrentCredentialAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
