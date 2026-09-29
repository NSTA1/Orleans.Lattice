using Orleans.Lattice.Explorer.Shell.Design;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.App;

/// <summary>
/// The static assets the manifest-derived app pages (A2, issue #3819) ship, as paths
/// relative to the application's base URL.
/// </summary>
/// <remarks>
/// The page links its own stylesheet, as the app frame does, so it loads only where an
/// app page is shown. The cutover (K1) may instead link it from the head beside the
/// design system's stylesheets.
/// </remarks>
internal static class AppPagesAssets
{
    /// <summary>The folder the app pages' assets are served from.</summary>
    public const string BasePath = ShellDesignAssets.ContentBasePath + "apps/";

    /// <summary>The app pages' own layout rules: the page head, the icon and the section tabs.</summary>
    public const string Stylesheet = BasePath + "app.css";
}
