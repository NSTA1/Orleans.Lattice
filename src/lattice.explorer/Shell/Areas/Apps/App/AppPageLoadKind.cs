namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.App;

/// <summary>What loading an app's page found.</summary>
internal enum AppPageLoadKind
{
    /// <summary>
    /// Nothing the caller may see: the app does not exist, is not installed, or the caller
    /// holds neither a role in it nor <c>AppInstall</c>. The three are indistinguishable.
    /// </summary>
    NotFound = 0,

    /// <summary>The app, as this caller may see it.</summary>
    Loaded = 1,

    /// <summary>The cluster could not answer, so nothing can be said about any app.</summary>
    Unavailable = 2,
}
