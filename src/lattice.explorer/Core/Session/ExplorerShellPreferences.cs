using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Default <see cref="IExplorerShellPreferences"/>: the declared contract layered
/// over the durable <see cref="IUiPreferenceStore"/>, adding scope, key
/// validation, reset, and the shared fall-back-and-explain path.
/// </summary>
/// <remarks>
/// The underlying store stays a flat key/value document; this type owns the
/// mapping from a declared key plus the current identity onto a stored name. That
/// mapping is cached and invalidated on a scope change, so a read on a render
/// path is a dictionary lookup rather than a string built per call.
/// </remarks>
public sealed class ExplorerShellPreferences : IExplorerShellPreferences, IDisposable
{
    private readonly IUiPreferenceStore _store;
    private readonly IExplorerPreferenceCatalog _catalog;
    private readonly IExplorerPreferenceScopeProvider _scope;

    // Keyed by the declared key instance (reference equality is the type's
    // identity), so the scoped name is composed once per key per scope rather
    // than concatenated on every read.
    private readonly Dictionary<ExplorerPreferenceKey, string> _scopedNames = [];

    /// <summary>Creates the contract over its collaborators.</summary>
    /// <param name="store">The durable preference store. Must not be <see langword="null"/>.</param>
    /// <param name="catalog">The registry of declared keys. Must not be <see langword="null"/>.</param>
    /// <param name="scope">The identity preferences are remembered against. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public ExplorerShellPreferences(
        IUiPreferenceStore store,
        IExplorerPreferenceCatalog catalog,
        IExplorerPreferenceScopeProvider scope)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(scope);

        _store = store;
        _catalog = catalog;
        _scope = scope;
        _scope.ScopeChanged += OnScopeChanged;
    }

    /// <inheritdoc />
    public bool IsLoaded => _store.IsLoaded;

    /// <inheritdoc />
    public IReadOnlyList<ExplorerPreferenceKey> Keys => _catalog.Keys;

    /// <inheritdoc />
    public event Action? Changed;

    /// <inheritdoc />
    public Task EnsureLoadedAsync(CancellationToken cancellationToken = default) =>
        _store.EnsureLoadedAsync(cancellationToken);

    /// <inheritdoc />
    public T GetOrDefault<T>(ExplorerPreferenceKey key, T fallback = default!) =>
        _store.GetOrDefault(ScopedName(key), fallback);

    /// <inheritdoc />
    public ExplorerPreferenceResolution<T> Resolve<T, TState>(
        ExplorerPreferenceKey key,
        T fallback,
        TState state,
        Func<T, TState, bool> isResolvable)
    {
        ArgumentNullException.ThrowIfNull(isResolvable);

        var name = ScopedName(key);

        if (!_store.IsLoaded)
        {
            // Reading an unhydrated mirror would report "nothing remembered",
            // which a caller could then persist over the user's real choice.
            return ExplorerPreferenceResolution<T>.FellBack(
                fallback,
                ExplorerPreferenceFallbackReason.NotLoaded);
        }

        if (!_store.TryGet<T>(name, out var remembered))
        {
            return ExplorerPreferenceResolution<T>.FellBack(
                fallback,
                ExplorerPreferenceFallbackReason.NotStored);
        }

        if (isResolvable(remembered, state))
        {
            return ExplorerPreferenceResolution<T>.Restored(remembered);
        }

        return ExplorerPreferenceResolution<T>.Abandoned(fallback, Explain(key));
    }

    /// <inheritdoc />
    public ExplorerPreferenceResolution<T> Resolve<T>(
        ExplorerPreferenceKey key,
        T fallback,
        Func<T, bool> isResolvable)
    {
        ArgumentNullException.ThrowIfNull(isResolvable);

        // The predicate itself is the state, so this shape adds no allocation of
        // its own beyond the closure the caller already created.
        return Resolve(key, fallback, isResolvable, static (value, predicate) => predicate(value));
    }

    /// <inheritdoc />
    public async Task<ExplorerPreferenceResolution<T>> RestoreAsync<T, TState>(
        ExplorerPreferenceKey key,
        T fallback,
        TState state,
        Func<T, TState, bool> isResolvable,
        CancellationToken cancellationToken = default)
    {
        var resolution = Resolve(key, fallback, state, isResolvable);

        if (resolution.WasAbandoned)
        {
            // Forget it here rather than leaving every caller to remember to:
            // a value that no longer resolves would otherwise be re-read, and
            // re-explained, on every restore for the rest of its retention.
            await _store.RemoveAsync(ScopedName(key), cancellationToken).ConfigureAwait(false);
        }

        return resolution;
    }

    /// <inheritdoc />
    public Task SetAsync<T>(ExplorerPreferenceKey key, T value, CancellationToken cancellationToken = default) =>
        _store.SetAsync(ScopedName(key), value, owner: null, cancellationToken);

    /// <inheritdoc />
    public Task ClearAsync(ExplorerPreferenceKey key, CancellationToken cancellationToken = default) =>
        _store.RemoveAsync(ScopedName(key), cancellationToken);

    /// <inheritdoc />
    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        await _store.EnsureLoadedAsync(cancellationToken).ConfigureAwait(false);

        var keys = _catalog.Keys;
        for (var i = 0; i < keys.Count; i++)
        {
            await _store.RemoveAsync(ScopedName(keys[i]), cancellationToken).ConfigureAwait(false);
        }

        Changed?.Invoke();
    }

    /// <inheritdoc />
    public ExplorerRoute GetRememberedRoute()
    {
        var area = GetOrDefault(ExplorerPreferenceKeys.ActiveArea, string.Empty);
        if (!ExplorerRouteSlug.IsCanonical(area))
        {
            // Nothing remembered, or something that could never have been written
            // through this contract. Either way there is no route to restore.
            return RouteWithTenantScope(ExplorerRoute.Root);
        }

        var route = ExplorerRoute.Root.WithArea(area);

        var kind = GetOrDefault(ExplorerPreferenceKeys.CatalogKind, string.Empty);
        var id = GetOrDefault(ExplorerPreferenceKeys.Selection, string.Empty);
        if (ExplorerRouteSlug.IsCanonical(kind) && id.Length != 0)
        {
            route = route.WithSelection(kind, id);

            var surface = GetOrDefault(ExplorerPreferenceKeys.DetailSurface, string.Empty);
            if (ExplorerRouteSlug.IsCanonical(surface))
            {
                route = route.WithSurface(surface);
            }
        }

        return RouteWithTenantScope(route);
    }

    /// <inheritdoc />
    public async Task RememberRouteAsync(ExplorerRoute route, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(route);

        if (route.IsBare)
        {
            return;
        }

        if (!_store.IsLoaded)
        {
            // Writing before the mirror hydrates would compare this route against
            // an empty one, so a route with no selection would issue a clear that
            // erased the very selection the shell is about to restore - and every
            // other segment would be written whether or not it changed. The
            // caller re-remembers once hydration completes.
            return;
        }

        await RememberIfChangedAsync(ExplorerPreferenceKeys.ActiveArea, route.Area, cancellationToken)
            .ConfigureAwait(false);
        await RememberIfChangedAsync(ExplorerPreferenceKeys.CatalogKind, route.Kind, cancellationToken)
            .ConfigureAwait(false);
        await RememberIfChangedAsync(ExplorerPreferenceKeys.Selection, route.Id, cancellationToken)
            .ConfigureAwait(false);
        await RememberIfChangedAsync(ExplorerPreferenceKeys.DetailSurface, route.Surface, cancellationToken)
            .ConfigureAwait(false);
        await RememberIfChangedAsync(ExplorerPreferenceKeys.ActiveTenant, route.Tenant, cancellationToken)
            .ConfigureAwait(false);

        if (GetOrDefault(ExplorerPreferenceKeys.AllTenantsVisible, false) != route.AllTenants)
        {
            await SetAsync(ExplorerPreferenceKeys.AllTenantsVisible, route.AllTenants, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Detaches from the scope provider.</summary>
    public void Dispose() => _scope.ScopeChanged -= OnScopeChanged;

    private ExplorerRoute RouteWithTenantScope(ExplorerRoute route)
    {
        var tenant = GetOrDefault(ExplorerPreferenceKeys.ActiveTenant, string.Empty);
        return route
            .WithTenant(tenant)
            .WithAllTenants(GetOrDefault(ExplorerPreferenceKeys.AllTenantsVisible, false));
    }

    private Task RememberIfChangedAsync(
        ExplorerPreferenceKey key,
        string value,
        CancellationToken cancellationToken)
    {
        // The durable store rewrites its whole document on every write, so a
        // navigation that changed one segment must not cost six persists.
        if (string.Equals(GetOrDefault(key, string.Empty), value, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        return value.Length == 0
            ? ClearAsync(key, cancellationToken)
            : SetAsync(key, value, cancellationToken);
    }

    private static string Explain(ExplorerPreferenceKey key) =>
        $"The Explorer could not restore {key.Description}, so it is showing a default instead.";

    private string ScopedName(ExplorerPreferenceKey key)
    {
        ArgumentNullException.ThrowIfNull(key);

        if (_scopedNames.TryGetValue(key, out var name))
        {
            return name;
        }

        if (!_catalog.Contains(key))
        {
            throw new ArgumentException(
                $"'{key.Name}' is not a registered Explorer preference key. Declare it once and register it with IExplorerPreferenceCatalog so it is enumerable, scoped and resettable.",
                nameof(key));
        }

        name = string.Concat(_scope.Current.ToScopeToken(key.Scope), ".", key.Name);
        _scopedNames[key] = name;
        return name;
    }

    private void OnScopeChanged()
    {
        // Every cached name embedded the previous identity's token.
        _scopedNames.Clear();
        Changed?.Invoke();
    }
}
