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
}
