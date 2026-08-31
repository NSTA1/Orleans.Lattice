namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// A head's own theme, for a head whose host platform has one. Registering an
/// implementation is what makes "follow the system" mean the application rather
/// than the embedded web view.
/// </summary>
/// <remarks>
/// Optional by design: <see cref="ExplorerAppearance"/> takes it as a nullable
/// dependency, so the web head - where the browser answers
/// <c>prefers-color-scheme</c> in the document itself - registers nothing and
/// the resolution stays where it belongs.
/// </remarks>
public interface IExplorerHostTheme
{
    /// <summary>
    /// The palette the host is currently asking for, or
    /// <see cref="ExplorerHostThemePreference.Unspecified"/> when it has no
    /// opinion.
    /// </summary>
    ExplorerHostThemePreference Preference { get; }

    /// <summary>
    /// Raised when <see cref="Preference"/> has changed, so an operator who is
    /// following the system sees the Explorer follow a theme switch made while it
    /// is running.
    /// </summary>
    event Action? Changed;
}
