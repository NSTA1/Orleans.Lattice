namespace Orleans.Lattice.Auth;

/// <summary>
/// Discriminates the extent of a tree a <see cref="LatticeScope"/> covers.
/// </summary>
public enum LatticeScopeKind
{
    /// <summary>The scope covers an entire tree.</summary>
    Tree = 0,

    /// <summary>The scope covers a single key within a tree.</summary>
    Key = 1,

    /// <summary>The scope covers every key sharing a prefix within a tree.</summary>
    Prefix = 2,
}
