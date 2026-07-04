namespace Orleans.Lattice;

/// <summary>
/// The core no-op <see cref="ILatticeMembershipContext"/>: always resolves to
/// <see cref="LatticeSubject.Anonymous"/>. Registered by <c>AddLattice</c> as a
/// safe default so a consumer of the seam (for example the later access-gate)
/// always resolves an instance even when the Membership package is not
/// registered. <c>AddLatticeMembership</c> replaces it with the real,
/// credential-resolving implementation.
/// </summary>
internal sealed class NullLatticeMembershipContext : ILatticeMembershipContext
{
    private static readonly ValueTask<LatticeSubject> AnonymousResult =
        new(LatticeSubject.Anonymous);

    /// <inheritdoc />
    public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
        AnonymousResult;

    /// <inheritdoc />
    public bool TryResolveCurrent(out LatticeSubject subject)
    {
        subject = LatticeSubject.Anonymous;
        return true;
    }
}
