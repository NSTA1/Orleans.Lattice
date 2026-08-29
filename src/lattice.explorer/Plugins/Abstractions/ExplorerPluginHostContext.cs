namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The default <see cref="IExplorerPluginHostContext"/>: a thin, bound view
/// over the ambient host state, a plugin-scoped preference namespace, and the
/// plugin's own declared domain contract.
/// <para>
/// The ambient facts are read through to
/// <see cref="IExplorerPluginHostState"/> rather than copied, so a context
/// never serves a stale selection or connection status and holds no
/// per-transition state of its own.
/// </para>
/// </summary>
public sealed class ExplorerPluginHostContext : IExplorerPluginHostContext
{
    private readonly IExplorerPluginHostState _state;
    private readonly IExplorerPluginDomainResolver _domains;
    private readonly IExplorerPluginPreferences _preferences;

    /// <summary>
    /// Binds a context to <paramref name="pluginId"/>.
    /// </summary>
    /// <param name="pluginId">The plugin this context serves. Must not be <see langword="null"/> or whitespace.</param>
    /// <param name="state">The ambient host state to read through to. Must not be <see langword="null"/>.</param>
    /// <param name="preferences">
    /// The host's root preference store. It is wrapped in a namespace private to
    /// <paramref name="pluginId"/>, so the plugin can neither collide with nor
    /// read another plugin's preferences.
    /// </param>
    /// <param name="domains">The resolver for the plugin's declared domain contract. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="pluginId"/> is empty or whitespace.</exception>
    public ExplorerPluginHostContext(
        string pluginId,
        IExplorerPluginHostState state,
        IExplorerPluginPreferences preferences,
        IExplorerPluginDomainResolver domains)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pluginId);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(preferences);
        ArgumentNullException.ThrowIfNull(domains);

        PluginId = pluginId;
        _state = state;
        _domains = domains;
        _preferences = new ScopedPreferences(pluginId, preferences);
    }

    /// <inheritdoc />
    public string PluginId { get; }

    /// <inheritdoc />
    public ExplorerPluginSelection? Selection => _state.Selection;

    /// <inheritdoc />
    public ExplorerPluginConnectionStatus Connection => _state.Connection;

    /// <inheritdoc />
    public ExplorerPluginTenantScope Tenant => _state.Tenant;

    /// <inheritdoc />
    public IExplorerPluginPreferences Preferences => _preferences;

    /// <inheritdoc />
    public event Action<ExplorerPluginHostChange>? Changed
    {
        add => _state.Changed += value;
        remove => _state.Changed -= value;
    }

    /// <inheritdoc />
    public TDomain GetDomain<TDomain>() where TDomain : class => _domains.Resolve<TDomain>(PluginId);

    /// <inheritdoc />
    public bool TryGetDomain<TDomain>(out TDomain? domain) where TDomain : class =>
        _domains.TryResolve(PluginId, out domain);

    /// <summary>
    /// Namespaces every key with the owning plugin's id, so two plugins may use
    /// the same key without colliding and neither can name the other's entries.
    /// </summary>
    private sealed class ScopedPreferences(string pluginId, IExplorerPluginPreferences inner)
        : IExplorerPluginPreferences
    {
        // Composed once, so scoping a key costs a single concat rather than a
        // format call per read.
        private readonly string _prefix = pluginId + "/";

        public bool IsLoaded => inner.IsLoaded;

        public Task EnsureLoadedAsync(CancellationToken cancellationToken = default) =>
            inner.EnsureLoadedAsync(cancellationToken);

        public bool TryGet<T>(string key, out T value) => inner.TryGet(Scope(key), out value);

        public T GetOrDefault<T>(string key, T fallback = default!) =>
            inner.GetOrDefault(Scope(key), fallback);

        public Task SetAsync<T>(string key, T value, CancellationToken cancellationToken = default) =>
            inner.SetAsync(Scope(key), value, cancellationToken);

        public Task RemoveAsync(string key, CancellationToken cancellationToken = default) =>
            inner.RemoveAsync(Scope(key), cancellationToken);

        private string Scope(string key)
        {
            ArgumentNullException.ThrowIfNull(key);
            return string.Concat(_prefix, key);
        }
    }
}
