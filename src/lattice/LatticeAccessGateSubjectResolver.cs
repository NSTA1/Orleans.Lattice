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
/// <para>
/// <c>AddLattice</c> always registers <see cref="NullLatticeMembershipContext"/>
/// (which resolves <see cref="LatticeSubject.Anonymous"/>), so in a normally
/// configured host the context is never <c>null</c>; the null-tolerant overload
/// exists so a caller that resolves the context with
/// <c>IServiceProvider.GetService</c> (which returns <c>null</c> when
/// unregistered) still gets a well-defined anonymous subject without a
/// dependency on the Membership package.
/// </para>
/// <para>
/// <b>Non-recursion, paid only on a cache miss.</b> Subject resolution reads the
/// membership directory's own dogfooded trees through the gated surface, so it
/// must run under a <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>
/// scope to bypass the gate and avoid re-entering it. That scope is an ambient
/// request-context round-trip, so this helper enters it <em>only</em> when
/// resolution actually needs a directory read: the warm path
/// (<see cref="ILatticeMembershipContext.TryResolveCurrent"/> - a cached or
/// anonymous subject) resolves synchronously with no tree read, cannot recurse,
/// and pays no scope cost. Owning the scope decision here keeps every
/// enforcement call site on the same fast path.
/// </para>
/// </remarks>
internal static class LatticeAccessGateSubjectResolver
{
    /// <summary>
    /// Resolves the current caller subject from <paramref name="membership"/>,
    /// or <see cref="LatticeSubject.Anonymous"/> when it is <c>null</c>. Enters a
    /// system-origin gate-bypass scope only when resolution must read the
    /// dogfooded directory (a cache miss); the warm cached/anonymous path
    /// resolves synchronously with no scope.
    /// </summary>
    /// <param name="membership">
    /// The registered membership context, or <c>null</c> when none is
    /// registered.
    /// </param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The resolved subject, or <see cref="LatticeSubject.Anonymous"/>.</returns>
    public static ValueTask<LatticeSubject> ResolveAsync(
        ILatticeMembershipContext? membership,
        CancellationToken cancellationToken = default)
    {
        if (membership is null)
        {
            return new ValueTask<LatticeSubject>(LatticeSubject.Anonymous);
        }

        // Warm fast path: a cached or anonymous subject resolves synchronously
        // with no directory read, so the gate-bypass scope is unnecessary.
        // Skipping it avoids an ambient request-context round-trip on every
        // gated operation.
        if (membership.TryResolveCurrent(out var subject))
        {
            return new ValueTask<LatticeSubject>(subject);
        }

        // Cache miss: resolution reads the membership directory's own dogfooded
        // trees through the gated surface, so it must run under a system-origin
        // scope to bypass the gate and avoid re-entering it.
        return ResolveUncachedAsync(membership, cancellationToken);
    }

    private static async ValueTask<LatticeSubject> ResolveUncachedAsync(
        ILatticeMembershipContext membership,
        CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await membership.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
