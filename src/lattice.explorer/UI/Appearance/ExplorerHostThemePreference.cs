namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// What a head's own host platform says about the palette it wants, for the
/// heads that have an opinion of their own.
/// </summary>
/// <remarks>
/// The web head has no opinion: the browser answers <c>prefers-color-scheme</c>
/// in the document itself, so the web head registers no
/// <see cref="IExplorerHostTheme"/> and the resolution is left to the document.
/// The desktop head does have one - a MAUI application carries its own requested
/// theme, which the user can change while the app is running - so it registers a
/// source and "follow system" tracks the application rather than the embedded
/// web view.
/// </remarks>
public enum ExplorerHostThemePreference
{
    /// <summary>
    /// The host has no opinion, so "follow system" is left for the document to
    /// resolve from <c>prefers-color-scheme</c>.
    /// </summary>
    Unspecified = 0,

    /// <summary>The host is in a light theme.</summary>
    Light = 1,

    /// <summary>The host is in a dark theme.</summary>
    Dark = 2,
}
