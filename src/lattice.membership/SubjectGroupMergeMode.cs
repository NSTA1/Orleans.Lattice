namespace Orleans.Lattice.Membership;

/// <summary>
/// Controls how <see cref="ILatticeSubjectMapper"/> combines the group ids a
/// token asserts with the group ids the local membership directory derives when
/// building a <see cref="LatticeSubject"/>.
/// </summary>
public enum SubjectGroupMergeMode
{
    /// <summary>
    /// The default: the resolved groups are the union of the token-asserted
    /// groups and the directory-derived (transitively-expanded) groups.
    /// </summary>
    Union = 0,

    /// <summary>Only the token-asserted groups are used; the directory is ignored for group membership.</summary>
    TokenOnly = 1,

    /// <summary>Only the directory-derived groups are used; token-asserted groups are ignored.</summary>
    DirectoryOnly = 2,
}
