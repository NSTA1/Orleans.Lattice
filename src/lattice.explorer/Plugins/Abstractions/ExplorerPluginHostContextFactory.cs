using System.Collections.Concurrent;

namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The default <see cref="IExplorerPluginHostContextFactory"/>. It caches one
/// context per plugin id, so repeated calls - one per probe, one per render -
/// cost a dictionary lookup rather than an allocation, and a plugin's
/// <see cref="IExplorerPluginHostContext.Changed"/> subscription stays attached
/// to a stable instance.
/// </summary>
/// <param name="state">The ambient host state contexts read through to.</param>
/// <param name="preferences">The root preference store each context namespaces.</param>
/// <param name="domains">The resolver each context uses for its plugin's declared contract.</param>
public sealed class ExplorerPluginHostContextFactory(
    IExplorerPluginHostState state,
    IExplorerPluginPreferences preferences,
    IExplorerPluginDomainResolver domains) : IExplorerPluginHostContextFactory
{
    private readonly ConcurrentDictionary<string, IExplorerPluginHostContext> _contexts =
        new(StringComparer.Ordinal);

    private readonly IExplorerPluginHostState _state =
        state ?? throw new ArgumentNullException(nameof(state));

    private readonly IExplorerPluginPreferences _preferences =
        preferences ?? throw new ArgumentNullException(nameof(preferences));

    private readonly IExplorerPluginDomainResolver _domains =
        domains ?? throw new ArgumentNullException(nameof(domains));

    /// <inheritdoc />
    public IExplorerPluginHostContext Create(string pluginId)
    {
        ArgumentNullException.ThrowIfNull(pluginId);

        // The factory argument is a cached static lambda over `this`-free state
        // captured in a tuple, so the common (already-cached) path allocates
        // nothing at all.
        return _contexts.GetOrAdd(
            pluginId,
            static (id, host) => new ExplorerPluginHostContext(id, host.State, host.Preferences, host.Domains),
            (State: _state, Preferences: _preferences, Domains: _domains));
    }
}
