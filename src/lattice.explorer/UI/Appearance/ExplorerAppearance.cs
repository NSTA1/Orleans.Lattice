using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// Default <see cref="IExplorerAppearance"/>: the three appearance choices held
/// over the shell's declared preference contract, folded together with any host
/// theme, and pushed onto the document through an
/// <see cref="IExplorerAppearanceApplier"/>.
/// </summary>
/// <remarks>
/// <para>
/// The choices are held as fields rather than re-read from the store on every
/// access, because a shell reads them on the render path. Every read is therefore
/// a field read of a value type and allocates nothing; the store is consulted
/// only on hydration, on a set, and when the contract says its scope changed.
/// </para>
/// <para>
/// Applying is best effort and never throws into a caller. The appearance is
/// cosmetic: a head that cannot reach a document - a prerender pass, a static
/// render, a component test - must render a fully usable shell rather than fail.
/// </para>
/// </remarks>
public sealed class ExplorerAppearance : IExplorerAppearance, IDisposable
{
    // Cached so the restore path passes a static delegate rather than allocating
    // a closure per call. Restore runs on the shell's start-up path.
    private static readonly Func<string, byte, bool> ThemeIsKnown =
        static (name, ignored) => ExplorerAppearanceNames.TryParseThemeName(name, out _);

    private static readonly Func<string, byte, bool> ContrastIsKnown =
        static (name, ignored) => ExplorerAppearanceNames.TryParseContrastName(name, out _);

    private static readonly Func<string, byte, bool> DensityIsKnown =
        static (name, ignored) => ExplorerAppearanceNames.TryParseDensityName(name, out _);

    private readonly IExplorerShellPreferences _preferences;
    private readonly IExplorerAppearanceApplier _applier;
    private readonly IExplorerHostTheme? _hostTheme;

    private ExplorerThemeChoice _theme;
    private ExplorerContrastChoice _contrast;
    private ExplorerDensityChoice _density;

    /// <summary>Creates the appearance state over its collaborators.</summary>
    /// <param name="preferences">The durable preference contract. Must not be <see langword="null"/>.</param>
    /// <param name="applier">The seam that puts the resolved state on the document. Must not be <see langword="null"/>.</param>
    /// <param name="hostTheme">
    /// The head's own theme, or <see langword="null"/> for a head whose host has
    /// no opinion and leaves "follow system" to the document.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="preferences"/> or <paramref name="applier"/> is <see langword="null"/>.</exception>
    public ExplorerAppearance(
        IExplorerShellPreferences preferences,
        IExplorerAppearanceApplier applier,
        IExplorerHostTheme? hostTheme = null)
    {
        ArgumentNullException.ThrowIfNull(preferences);
        ArgumentNullException.ThrowIfNull(applier);

        _preferences = preferences;
        _applier = applier;
        _hostTheme = hostTheme;

        _preferences.Changed += OnPreferencesChanged;

        if (_hostTheme is not null)
        {
            _hostTheme.Changed += OnHostThemeChanged;
        }
    }

    /// <inheritdoc />
    public bool IsLoaded => _preferences.IsLoaded;

    /// <inheritdoc />
    public ExplorerThemeChoice Theme => _theme;

    /// <inheritdoc />
    public ExplorerContrastChoice Contrast => _contrast;

    /// <inheritdoc />
    public ExplorerDensityChoice Density => _density;

    /// <inheritdoc />
    public ExplorerAppearanceState Effective => new(ResolveTheme(), _contrast, _density);

    /// <inheritdoc />
    public string? Notice { get; private set; }

    /// <inheritdoc />
    public event Action? Changed;

    /// <inheritdoc />
    public async Task EnsureLoadedAsync(CancellationToken cancellationToken = default)
    {
        await _preferences.EnsureLoadedAsync(cancellationToken).ConfigureAwait(false);

        // RestoreAsync additionally forgets a stored name this build does not
        // know, so a value written by a newer build - or a corrupted entry - is
        // explained once and then stops resurfacing on every later load.
        string? notice = null;

        var theme = await _preferences.RestoreAsync(
            ExplorerAppearancePreferenceKeys.Theme,
            ExplorerAppearanceNames.FollowSystemName,
            state: (byte)0,
            ThemeIsKnown,
            cancellationToken).ConfigureAwait(false);
        notice ??= theme.Explanation;

        var contrast = await _preferences.RestoreAsync(
            ExplorerAppearancePreferenceKeys.Contrast,
            ExplorerAppearanceNames.FollowSystemName,
            state: (byte)0,
            ContrastIsKnown,
            cancellationToken).ConfigureAwait(false);
        notice ??= contrast.Explanation;

        var density = await _preferences.RestoreAsync(
            ExplorerAppearancePreferenceKeys.Density,
            ExplorerAppearanceNames.FollowLayoutName,
            state: (byte)0,
            DensityIsKnown,
            cancellationToken).ConfigureAwait(false);
        notice ??= density.Explanation;

        ExplorerAppearanceNames.TryParseThemeName(theme.Value, out _theme);
        ExplorerAppearanceNames.TryParseContrastName(contrast.Value, out _contrast);
        ExplorerAppearanceNames.TryParseDensityName(density.Value, out _density);

        // A later hydration must not erase an earlier one's explanation. Restoring
        // forgets the value it could not use, so the second pass a prerendering
        // head necessarily performs finds nothing stored and has nothing to say -
        // and would otherwise wipe the sentence before anybody had read it. The
        // notice stands until the operator chooses something or the scope changes.
        Notice ??= notice;

        await _applier.ApplyAsync(Effective, cancellationToken).ConfigureAwait(false);
        Changed?.Invoke();
    }

    /// <inheritdoc />
    public Task SetThemeAsync(ExplorerThemeChoice theme, CancellationToken cancellationToken = default)
    {
        var name = ExplorerAppearanceNames.ThemeName(theme);
        _theme = theme;
        return PersistAsync(ExplorerAppearancePreferenceKeys.Theme, name, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetContrastAsync(ExplorerContrastChoice contrast, CancellationToken cancellationToken = default)
    {
        var name = ExplorerAppearanceNames.ContrastName(contrast);
        _contrast = contrast;
        return PersistAsync(ExplorerAppearancePreferenceKeys.Contrast, name, cancellationToken);
    }

    /// <inheritdoc />
    public Task SetDensityAsync(ExplorerDensityChoice density, CancellationToken cancellationToken = default)
    {
        var name = ExplorerAppearanceNames.DensityName(density);
        _density = density;
        return PersistAsync(ExplorerAppearancePreferenceKeys.Density, name, cancellationToken);
    }

    /// <summary>Detaches from the contract and the host theme.</summary>
    public void Dispose()
    {
        _preferences.Changed -= OnPreferencesChanged;

        if (_hostTheme is not null)
        {
            _hostTheme.Changed -= OnHostThemeChanged;
        }
    }

    private async Task PersistAsync(
        ExplorerPreferenceKey key,
        string name,
        CancellationToken cancellationToken)
    {
        // A choice the operator just made is applied before it is written, so the
        // screen never waits on storage - and is applied even if storage fails.
        Notice = null;
        await _applier.ApplyAsync(Effective, cancellationToken).ConfigureAwait(false);
        Changed?.Invoke();
        await _preferences.SetAsync(key, name, cancellationToken).ConfigureAwait(false);
    }

    private ExplorerThemeChoice ResolveTheme()
    {
        if (_theme != ExplorerThemeChoice.FollowSystem)
        {
            return _theme;
        }

        return _hostTheme?.Preference switch
        {
            ExplorerHostThemePreference.Light => ExplorerThemeChoice.Light,
            ExplorerHostThemePreference.Dark => ExplorerThemeChoice.Dark,

            // No host opinion: leave it unresolved so the document's own
            // prefers-color-scheme query answers it, which is what keeps the web
            // head correct before any circuit exists.
            _ => ExplorerThemeChoice.FollowSystem,
        };
    }

    // The contract reports a scope change (a different user or cluster) or a
    // reset. Both mean the choices held here may no longer be this identity's, so
    // they are re-read from the store and re-applied.
    private void OnPreferencesChanged()
    {
        ExplorerAppearanceNames.TryParseThemeName(
            _preferences.GetOrDefault(ExplorerAppearancePreferenceKeys.Theme, string.Empty),
            out _theme);
        ExplorerAppearanceNames.TryParseContrastName(
            _preferences.GetOrDefault(ExplorerAppearancePreferenceKeys.Contrast, string.Empty),
            out _contrast);
        ExplorerAppearanceNames.TryParseDensityName(
            _preferences.GetOrDefault(ExplorerAppearancePreferenceKeys.Density, string.Empty),
            out _density);
        Notice = null;

        ApplyDetached();
        Changed?.Invoke();
    }

    // The desktop head's application theme moved. Only an operator following the
    // system sees a change, but re-applying unconditionally is a single interop
    // call and keeps the branch out of an event handler.
    private void OnHostThemeChanged()
    {
        ApplyDetached();
        Changed?.Invoke();
    }

    // Applied from a synchronous event handler, so there is nothing to await into.
    // An applier that completes synchronously - which the real ones do, having
    // nothing to wait on but the interop call - costs no state machine here.
    private void ApplyDetached()
    {
        var pending = _applier.ApplyAsync(Effective);

        if (!pending.IsCompletedSuccessfully)
        {
            _ = ObserveAsync(pending);
        }
    }

    private static async Task ObserveAsync(ValueTask pending)
    {
        try
        {
            await pending.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The appearance is cosmetic. A head that cannot reach its document
            // keeps a usable shell rather than faulting an unobserved task.
        }
    }
}
