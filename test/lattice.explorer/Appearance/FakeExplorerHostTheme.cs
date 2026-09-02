using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// A directly driven <see cref="IExplorerHostTheme"/>, so a test moves the host
/// platform's theme explicitly rather than waiting on one.
/// </summary>
internal sealed class FakeExplorerHostTheme : IExplorerHostTheme
{
    /// <inheritdoc />
    public ExplorerHostThemePreference Preference { get; private set; }

    /// <inheritdoc />
    public event Action? Changed;

    /// <summary>Creates a source reporting <paramref name="preference"/>.</summary>
    /// <param name="preference">The theme the host starts out asking for.</param>
    public FakeExplorerHostTheme(ExplorerHostThemePreference preference = ExplorerHostThemePreference.Unspecified) =>
        Preference = preference;

    /// <summary>Switches the host's theme and announces it, as an operating-system theme switch does.</summary>
    /// <param name="preference">The theme the host now asks for.</param>
    public void MoveTo(ExplorerHostThemePreference preference)
    {
        Preference = preference;
        Changed?.Invoke();
    }
}
