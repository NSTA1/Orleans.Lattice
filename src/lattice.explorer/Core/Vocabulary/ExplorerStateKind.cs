namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// Why a surface has nothing to show. An empty list is not one state, and the
/// difference is what the user needs told.
/// </summary>
/// <remarks>
/// Under deny-by-default the most common confusion is an empty list that is not
/// empty at all: the caller lacks a grant, or the active tenant scope is
/// filtering everything out. Naming the states separately forces every surface
/// to say which one it is in.
/// </remarks>
public enum ExplorerStateKind
{
    /// <summary>The read is still in flight.</summary>
    Loading = 0,

    /// <summary>There is genuinely nothing to list. Nothing is being withheld or filtered.</summary>
    Empty = 1,

    /// <summary>Items exist, but not inside the tenant scope currently in force.</summary>
    ScopedOut = 2,

    /// <summary>The caller's identity does not hold the grant the cluster requires.</summary>
    NotPermitted = 3,

    /// <summary>The cluster serves this only to a signed-in identity, and the caller is anonymous.</summary>
    SignInRequired = 4,

    /// <summary>The feature behind the surface is not enabled on this cluster.</summary>
    Unavailable = 5,

    /// <summary>The read was attempted and failed.</summary>
    Failed = 6,
}
