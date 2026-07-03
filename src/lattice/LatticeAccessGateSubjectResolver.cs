namespace Orleans.Lattice;

/// <summary>
/// Internal helper that resolves the caller <see cref="LatticeSubject"/> for
/// the access-gate enforcement point from the core
/// <see cref="ILatticeMembershipContext"/> seam. Defined here (rather than at
/// the future enforcement call site) so the resolution rule - "the membership
/// subject when a context is registered, else
/// <see cref="LatticeSubject.Anonymous"/>" - is small, single-sourced, and
/// unit-tested independently of any grain.
/// </summary>
/// <remarks>
/// <c>AddLattice</c> always registers <see cref="NullLatticeMembershipContext"/>
/// (which resolves <see cref="LatticeSubject.Anonymous"/>), so in a normally
/// configured host the context is never <c>null</c>; the null-tolerant overload
/// exists so a caller that resolves the context with
/// <c>IServiceProvider.GetService</c> (which returns <c>null</c> when
/// unregistered) still gets a well-defined anonymous subject without a
/// dependency on the Membership package.
/// </remarks>
internal static class LatticeAccessGateSubjectResolver
{
    /// <summary>
    /// Resolves the current caller subject from <paramref name="membership"/>,
    /// or <see cref="LatticeSubject.Anonymous"/> when it is <c>null</c>.
    /// </summary>
    /// <param name="membership">
    /// The registered membership context, or <c>null</c> when none is
    /// registered.
    /// </param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The resolved subject, or <see cref="LatticeSubject.Anonymous"/>.</returns>
    public static ValueTask<LatticeSubject> ResolveAsync(
        ILatticeMembershipContext? membership,
        CancellationToken cancellationToken = default) =>
        membership is null
            ? new ValueTask<LatticeSubject>(LatticeSubject.Anonymous)
            : membership.ResolveCurrentAsync(cancellationToken);
}
