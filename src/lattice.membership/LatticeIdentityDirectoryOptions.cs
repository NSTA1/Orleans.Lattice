namespace Orleans.Lattice.Membership;

/// <summary>
/// Provider-neutral configuration for the <see cref="ILatticeIdentityDirectory"/>
/// seam: the default and maximum page sizes applied to
/// <see cref="DirectorySearchQuery"/>, and whether a supplied id must resolve to
/// an existing principal before an operator may grant it access.
/// </summary>
public sealed class LatticeIdentityDirectoryOptions
{
    /// <summary>
    /// The page size a provider applies when a <see cref="DirectorySearchQuery"/>
    /// requests none (<see cref="DirectorySearchQuery.PageSize"/> is <c>0</c>).
    /// Must be strictly positive and no greater than <see cref="MaxPageSize"/>.
    /// Defaults to 25.
    /// </summary>
    public int DefaultPageSize { get; set; } = 25;

    /// <summary>
    /// The upper bound a provider clamps a requested
    /// <see cref="DirectorySearchQuery.PageSize"/> to. Must be strictly positive.
    /// Defaults to 100.
    /// </summary>
    public int MaxPageSize { get; set; } = 100;

    /// <summary>
    /// Whether a supplied principal id must resolve to an existing
    /// <see cref="DirectoryPrincipal"/> (via
    /// <see cref="ILatticeIdentityDirectory.ResolveAsync(string, System.Threading.CancellationToken)"/>)
    /// before it may be granted access. <c>false</c> (the default) accepts ids
    /// without validation, matching the behaviour of
    /// <see cref="NullIdentityDirectory"/>.
    /// </summary>
    public bool ValidationRequired { get; set; }
}
