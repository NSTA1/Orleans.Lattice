namespace Orleans.Lattice.Api.Apps;

/// <summary>An API-layer mirror of what an app source supports, independent of the app engine's source types.</summary>
[Flags]
public enum AppSourceSummaryCapabilities
{
    /// <summary>The source supports none of the optional capabilities.</summary>
    None = 0,
    /// <summary>The source can list the apps it offers.</summary>
    Enumerate = 1,
    /// <summary>The source honours a text filter when listing.</summary>
    Search = 2,
    /// <summary>The source may offer more than one version of an app.</summary>
    MultipleVersions = 4,
    /// <summary>The source must acquire and verify an app before it can be described or installed.</summary>
    RequiresAcquisition = 8,
}
