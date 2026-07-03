namespace Orleans.Lattice.Membership;

/// <summary>
/// Merges an identity-provider-asserted <see cref="LatticePrincipal"/> with the
/// group ids derived from the local membership directory into the final
/// <see cref="LatticeSubject"/>. The seam lets a host customize claim-to-group
/// mapping and the token-vs-directory merge policy without replacing the
/// resolution pipeline.
/// </summary>
public interface ILatticeSubjectMapper
{
    /// <summary>
    /// Produces the final <see cref="LatticeSubject"/> from
    /// <paramref name="principal"/> and the already-transitively-expanded
    /// <paramref name="directoryGroups"/>. Implementations decide how the
    /// token-asserted and directory-derived groups combine.
    /// </summary>
    /// <param name="principal">The IDP-asserted principal. Must not be <c>null</c>.</param>
    /// <param name="directoryGroups">
    /// The transitively-expanded group ids the directory derived for the
    /// principal's subject (and any token-asserted seed groups). Must not be
    /// <c>null</c>; empty when the directory contributes no groups.
    /// </param>
    /// <returns>The resolved subject.</returns>
    LatticeSubject Map(LatticePrincipal principal, IReadOnlyCollection<string> directoryGroups);
}
