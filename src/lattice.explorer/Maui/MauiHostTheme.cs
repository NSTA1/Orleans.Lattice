using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer;

/// <summary>
/// The desktop head's <see cref="IExplorerHostTheme"/>: reports the MAUI
/// application's own requested theme, so an operator who has left the Explorer
/// following the system follows <em>Windows</em> rather than whatever the
/// embedded web view happens to report.
/// </summary>
/// <remarks>
/// <para>
/// The distinction matters on the desktop. A <c>BlazorWebView</c> resolves
/// <c>prefers-color-scheme</c> from the web view's own configuration, which does
/// not necessarily track the app's theme - and MAUI lets the app's theme be set
/// explicitly, independently of the operating system. Deferring to
/// <see cref="Application.RequestedTheme"/> makes "follow the system" mean the
/// one thing a desktop operator would expect it to.
/// </para>
/// <para>
/// <see cref="Application.RequestedThemeChanged"/> fires when the operating
/// system's theme is switched while the app is running, so the Explorer repaints
/// with it instead of waiting for a restart.
/// </para>
/// </remarks>
internal sealed class MauiHostTheme : IExplorerHostTheme, IDisposable
{
    private readonly Application? _application;

    /// <summary>
    /// Subscribes to the current MAUI application's theme. A null application -
    /// which is what a unit-test host or a very early activation sees - simply
    /// reports no opinion.
    /// </summary>
    public MauiHostTheme()
    {
        _application = Application.Current;

        if (_application is not null)
        {
            _application.RequestedThemeChanged += OnRequestedThemeChanged;
        }
    }

    /// <inheritdoc />
    public ExplorerHostThemePreference Preference => _application?.RequestedTheme switch
    {
        AppTheme.Light => ExplorerHostThemePreference.Light,
        AppTheme.Dark => ExplorerHostThemePreference.Dark,

        // AppTheme.Unspecified, and the no-application case: leave the answer to
        // the document's own prefers-color-scheme query rather than guessing.
        _ => ExplorerHostThemePreference.Unspecified,
    };

    /// <inheritdoc />
    public event Action? Changed;

    /// <summary>Detaches from the application's theme notifications.</summary>
    public void Dispose()
    {
        if (_application is not null)
        {
            _application.RequestedThemeChanged -= OnRequestedThemeChanged;
        }
    }

    private void OnRequestedThemeChanged(object? sender, AppThemeChangedEventArgs args) => Changed?.Invoke();
}
