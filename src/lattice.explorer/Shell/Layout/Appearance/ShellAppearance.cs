using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Layout.Appearance;

/// <summary>
/// The operator's appearance - material, contrast and density - held for the
/// circuit, remembered through Core's preference contract, and put on the
/// document through an <see cref="IShellAppearanceApplier"/>.
/// </summary>
/// <remarks>
/// <para>
/// Remembering is optional: when the head registers no preference contract the
/// choices still apply for the session and are simply not remembered.
/// </para>
/// <para>
/// A choice is applied before it is written, so the screen never waits on
/// storage and is right even if storage fails. A remembered value this build
/// does not recognise is forgotten on restore rather than resurfacing.
/// </para>
/// </remarks>
internal sealed class ShellAppearance : IDisposable
{
    private static readonly Func<string, byte, bool> ThemeIsKnown =
        static (name, ignored) => ShellAppearanceNames.TryParseTheme(name, out _);

    private static readonly Func<string, byte, bool> ContrastIsKnown =
        static (name, ignored) => ShellAppearanceNames.TryParseContrast(name, out _);

    private static readonly Func<string, byte, bool> DensityIsKnown =
        static (name, ignored) => ShellAppearanceNames.TryParseDensity(name, out _);

    private readonly IShellAppearanceApplier _applier;
    private readonly IExplorerShellPreferences? _preferences;

    /// <summary>Creates the appearance state.</summary>
    /// <param name="applier">Puts the appearance on the document.</param>
    /// <param name="preferences">Core's preference contract, or <see langword="null"/> to remember nothing.</param>
    /// <param name="catalog">The contract's catalog, on which the appearance keys are registered.</param>
    public ShellAppearance(
        IShellAppearanceApplier applier,
        IExplorerShellPreferences? preferences = null,
        IExplorerPreferenceCatalog? catalog = null)
    {
        ArgumentNullException.ThrowIfNull(applier);

        _applier = applier;

        if (preferences is not null && catalog is not null)
        {
            foreach (var key in ShellAppearancePreferenceKeys.All)
            {
                catalog.Register(key);
            }

            _preferences = preferences;
            _preferences.Changed += OnPreferencesChanged;
        }
    }

    /// <summary>Raised after the appearance changes.</summary>
    public event Action? Changed;

    /// <summary>The chosen material.</summary>
    public ShellTheme Theme { get; private set; }

    /// <summary>The chosen contrast overlay.</summary>
    public ShellContrast Contrast { get; private set; }

    /// <summary>The chosen density.</summary>
    public LtDensity Density { get; private set; } = LtDensity.Comfortable;

    /// <summary>Whether the remembered choices have been read.</summary>
    public bool IsLoaded { get; private set; }

    /// <summary>
    /// Reads the remembered choices, once they can be read, and applies them.
    /// Safe to call repeatedly.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task EnsureLoadedAsync(CancellationToken cancellationToken = default)
    {
        if (IsLoaded)
        {
            return;
        }

        if (_preferences is not null)
        {
            await _preferences.EnsureLoadedAsync(cancellationToken).ConfigureAwait(false);
            if (!_preferences.IsLoaded)
            {
                // Browser storage is not reachable yet (a prerender). Try again later.
                return;
            }

            var theme = await _preferences.RestoreAsync(
                ShellAppearancePreferenceKeys.Theme, ShellAppearanceNames.FollowSystem, (byte)0, ThemeIsKnown, cancellationToken).ConfigureAwait(false);
            var contrast = await _preferences.RestoreAsync(
                ShellAppearancePreferenceKeys.Contrast, ShellAppearanceNames.FollowSystem, (byte)0, ContrastIsKnown, cancellationToken).ConfigureAwait(false);
            var density = await _preferences.RestoreAsync(
                ShellAppearancePreferenceKeys.Density, ShellAppearanceNames.Comfortable, (byte)0, DensityIsKnown, cancellationToken).ConfigureAwait(false);

            ShellAppearanceNames.TryParseTheme(theme.Value, out var parsedTheme);
            ShellAppearanceNames.TryParseContrast(contrast.Value, out var parsedContrast);
            ShellAppearanceNames.TryParseDensity(density.Value, out var parsedDensity);
            (Theme, Contrast, Density) = (parsedTheme, parsedContrast, parsedDensity);
        }

        IsLoaded = true;
        await _applier.ApplyAsync(Theme, Contrast, Density, cancellationToken).ConfigureAwait(false);
        Changed?.Invoke();
    }

    /// <summary>Chooses a material, applies it, and remembers it.</summary>
    /// <param name="theme">The material.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    public Task SetThemeAsync(ShellTheme theme, CancellationToken cancellationToken = default)
    {
        var name = ShellAppearanceNames.Name(theme);
        Theme = theme;
        return ApplyAndRememberAsync(ShellAppearancePreferenceKeys.Theme, name, cancellationToken);
    }

    /// <summary>Chooses a contrast overlay, applies it, and remembers it.</summary>
    /// <param name="contrast">The contrast.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    public Task SetContrastAsync(ShellContrast contrast, CancellationToken cancellationToken = default)
    {
        var name = ShellAppearanceNames.Name(contrast);
        Contrast = contrast;
        return ApplyAndRememberAsync(ShellAppearancePreferenceKeys.Contrast, name, cancellationToken);
    }

    /// <summary>Chooses a density, applies it, and remembers it.</summary>
    /// <param name="density">The density.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    public Task SetDensityAsync(LtDensity density, CancellationToken cancellationToken = default)
    {
        var name = ShellAppearanceNames.Name(density);
        Density = density;
        return ApplyAndRememberAsync(ShellAppearancePreferenceKeys.Density, name, cancellationToken);
    }

    /// <summary>Stops listening to the preference contract.</summary>
    public void Dispose()
    {
        if (_preferences is not null)
        {
            _preferences.Changed -= OnPreferencesChanged;
        }
    }

    private async Task ApplyAndRememberAsync(ExplorerPreferenceKey key, string name, CancellationToken cancellationToken)
    {
        await _applier.ApplyAsync(Theme, Contrast, Density, cancellationToken).ConfigureAwait(false);
        Changed?.Invoke();

        if (_preferences is not null)
        {
            await _preferences.SetAsync(key, name, cancellationToken).ConfigureAwait(false);
        }
    }

    // The contract reports a change of scope (another user) or a reset: re-read
    // what this identity remembers and re-apply it.
    private void OnPreferencesChanged()
    {
        ShellAppearanceNames.TryParseTheme(
            _preferences!.GetOrDefault(ShellAppearancePreferenceKeys.Theme, string.Empty), out var theme);
        ShellAppearanceNames.TryParseContrast(
            _preferences.GetOrDefault(ShellAppearancePreferenceKeys.Contrast, string.Empty), out var contrast);
        ShellAppearanceNames.TryParseDensity(
            _preferences.GetOrDefault(ShellAppearancePreferenceKeys.Density, string.Empty), out var density);
        (Theme, Contrast, Density) = (theme, contrast, density);

        _ = ApplyDetachedAsync();
        Changed?.Invoke();
    }

    private async Task ApplyDetachedAsync()
    {
        try
        {
            await _applier.ApplyAsync(Theme, Contrast, Density).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Cosmetic, and raised from an event handler: never fault the caller.
        }
    }
}
