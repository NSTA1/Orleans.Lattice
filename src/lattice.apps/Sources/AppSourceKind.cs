namespace Orleans.Lattice.Apps.Sources;

/// <summary>Whether an app source's offering is fixed for the life of the process or may change at run time.</summary>
public enum AppSourceKind
{
    /// <summary>
    /// The offering is fixed when the silo starts, for example the apps compiled into the image
    /// (<see cref="InImageAppSource"/>). Nothing is acquired after start.
    /// </summary>
    Static = 0,

    /// <summary>
    /// The offering can change while the silo runs, for example a package feed, a blob container or a
    /// container registry. See <see cref="IAppCatalogSource"/> for what such a source must guarantee.
    /// </summary>
    Dynamic = 1,
}
