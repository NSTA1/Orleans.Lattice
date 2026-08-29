using System.Collections.Concurrent;

namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The default in-memory <see cref="IExplorerPluginAccessStore"/>. Scoped to a
/// single session, so one operator's probed decisions never surface in another
/// circuit.
/// <para>
/// Backed by a <see cref="ConcurrentDictionary{TKey, TValue}"/> because
/// fault-isolated probes complete independently and write concurrently while
/// the render path reads. Reads take no lock and allocate nothing; a write is a
/// lock-free compare-and-swap that raises
/// <see cref="IExplorerPluginAccessStore.Changed"/> only when it actually
/// alters the stored value, so a re-probe that confirms the previous answer
/// does not re-render the shell.
/// </para>
/// </summary>
public sealed class ExplorerPluginAccessStore : IExplorerPluginAccessStore
{
    private readonly ConcurrentDictionary<ExplorerPluginAccessKey, ExplorerPluginAccess> _entries = new();

    /// <inheritdoc />
    public event Action<ExplorerPluginAccessChange>? Changed;

    /// <inheritdoc />
    public ExplorerPluginAccess Get(ExplorerPluginAccessKey key) =>
        _entries.TryGetValue(key, out var access) ? access : ExplorerPluginAccess.Denied;

    /// <inheritdoc />
    public ExplorerPluginAccess Get(string pluginId)
    {
        ArgumentNullException.ThrowIfNull(pluginId);
        return Get(new ExplorerPluginAccessKey(pluginId, Scope: null));
    }

    /// <inheritdoc />
    public ExplorerPluginAccess Get(string pluginId, string scope)
    {
        ArgumentNullException.ThrowIfNull(pluginId);
        ArgumentNullException.ThrowIfNull(scope);
        return Get(new ExplorerPluginAccessKey(pluginId, scope));
    }

    /// <inheritdoc />
    public void Set(ExplorerPluginAccessKey key, ExplorerPluginAccess access)
    {
        ArgumentNullException.ThrowIfNull(key.PluginId);

        while (true)
        {
            if (_entries.TryGetValue(key, out var existing))
            {
                if (existing == access)
                {
                    return;
                }

                if (_entries.TryUpdate(key, access, existing))
                {
                    break;
                }
            }
            else if (_entries.TryAdd(key, access))
            {
                break;
            }
        }

        Changed?.Invoke(new ExplorerPluginAccessChange(key, access));
    }

    /// <inheritdoc />
    public void Set(string pluginId, ExplorerPluginAccess access)
    {
        ArgumentNullException.ThrowIfNull(pluginId);
        Set(new ExplorerPluginAccessKey(pluginId, Scope: null), access);
    }

    /// <inheritdoc />
    public void Set(string pluginId, string scope, ExplorerPluginAccess access)
    {
        ArgumentNullException.ThrowIfNull(pluginId);
        ArgumentNullException.ThrowIfNull(scope);
        Set(new ExplorerPluginAccessKey(pluginId, scope), access);
    }

    /// <inheritdoc />
    public void Clear(string pluginId)
    {
        ArgumentNullException.ThrowIfNull(pluginId);

        foreach (var key in _entries.Keys)
        {
            if (string.Equals(key.PluginId, pluginId, StringComparison.Ordinal))
            {
                Remove(key);
            }
        }
    }

    /// <inheritdoc />
    public void Reset()
    {
        foreach (var key in _entries.Keys)
        {
            Remove(key);
        }
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<ExplorerPluginAccessKey, ExplorerPluginAccess> Snapshot() =>
        new Dictionary<ExplorerPluginAccessKey, ExplorerPluginAccess>(_entries);

    private void Remove(ExplorerPluginAccessKey key)
    {
        if (!_entries.TryRemove(key, out var removed))
        {
            return;
        }

        // A removed key reads as the fail-closed default, so dropping an entry
        // that already held exactly that default is not an observable change.
        if (removed != ExplorerPluginAccess.Denied)
        {
            Changed?.Invoke(new ExplorerPluginAccessChange(key, ExplorerPluginAccess.Denied));
        }
    }
}
