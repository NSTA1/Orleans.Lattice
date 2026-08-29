using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.UI.Plugins;

/// <summary>
/// The head's side of the plugin preference seam: it adapts the Explorer's own
/// durable <see cref="IUiPreferenceStore"/> onto the plugin contract's narrow
/// <see cref="IExplorerPluginPreferences"/> projection.
/// <para>
/// The contract's projection is deliberately thinner than the Explorer store:
/// it carries no owner discriminator and no garbage-collection sweep, so a
/// plugin can neither tag another surface's entries nor drop them. Per-plugin
/// key isolation is applied above this adapter, by the bound host context.
/// </para>
/// </summary>
/// <param name="store">The Explorer's durable preference store.</param>
public sealed class ExplorerPluginPreferences(IUiPreferenceStore store) : IExplorerPluginPreferences
{
    private readonly IUiPreferenceStore _store = store ?? throw new ArgumentNullException(nameof(store));

    /// <inheritdoc />
    public bool IsLoaded => _store.IsLoaded;

    /// <inheritdoc />
    public Task EnsureLoadedAsync(CancellationToken cancellationToken = default) =>
        _store.EnsureLoadedAsync(cancellationToken);

    /// <inheritdoc />
    public bool TryGet<T>(string key, out T value)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _store.TryGet(key, out value);
    }

    /// <inheritdoc />
    public T GetOrDefault<T>(string key, T fallback = default!)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _store.GetOrDefault(key, fallback);
    }

    /// <inheritdoc />
    public Task SetAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        // No owner discriminator: a plugin preference is not tied to a catalog
        // selection's lifetime, so it must not be swept when that selection goes
        // away.
        return _store.SetAsync(key, value, owner: null, cancellationToken);
    }

    /// <inheritdoc />
    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _store.RemoveAsync(key, cancellationToken);
    }
}
