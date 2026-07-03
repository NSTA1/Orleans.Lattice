namespace Orleans.Lattice.Auth;

/// <summary>
/// Discriminates the kind of principal a <see cref="LatticeSubjectSelector"/>
/// targets.
/// </summary>
public enum LatticeSubjectSelectorKind
{
    /// <summary>The selector targets a single user by id.</summary>
    User = 0,

    /// <summary>The selector targets a group by id (its transitive members).</summary>
    Group = 1,
}
