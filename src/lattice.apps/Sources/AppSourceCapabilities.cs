namespace Orleans.Lattice.Apps.Sources;

/// <summary>What an <see cref="IAppCatalogSource"/> can do, as flags on its <see cref="AppSourceDescriptor"/>.</summary>
[Flags]
public enum AppSourceCapabilities
{
    /// <summary>The source advertises no optional capability.</summary>
    None = 0,

    /// <summary>The source can list what it offers through <see cref="IAppCatalogSource.ListAsync"/>.</summary>
    Enumerate = 1,

    /// <summary>The source honours <see cref="AppSourceQuery.Text"/>; without this flag the text filter is ignored.</summary>
    Search = 2,

    /// <summary>The source can offer more than one version of the same slug.</summary>
    MultipleVersions = 4,

    /// <summary>
    /// The source must acquire and verify an artifact before it can be activated, so an install flow passes
    /// through acquiring and verifying stages that a static source never produces.
    /// </summary>
    RequiresAcquisition = 8,
}
