namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The default <see cref="IExplorerPluginAccessRefresher"/>: it probes each
/// registered plugin's own gate, contains any fault that probe produces, and
/// files the result under the plugin's key.
/// <para>
/// Isolation is structural rather than a convention. Each probe runs in its own
/// contained operation, so a gate that throws cannot escape into a sibling's
/// probe or into the caller, and every probe is started before any of them is
/// awaited, so a gate that never completes cannot stop a sibling's decision
/// from being filed. A faulted, cancelled, or misbehaving gate resolves to
/// <see cref="ExplorerPluginAccess.Denied"/> - the fail-closed answer - and is
/// never treated as an admission.
/// </para>
/// </summary>
/// <param name="catalog">The registered plugins to probe.</param>
/// <param name="store">The keyed store results are filed in.</param>
/// <param name="contexts">Supplies each plugin its own bound host context.</param>
public sealed class ExplorerPluginAccessRefresher(
    IExplorerPluginCatalog catalog,
    IExplorerPluginAccessStore store,
    IExplorerPluginHostContextFactory contexts) : IExplorerPluginAccessRefresher
{
    private readonly IExplorerPluginCatalog _catalog =
        catalog ?? throw new ArgumentNullException(nameof(catalog));

    private readonly IExplorerPluginAccessStore _store =
        store ?? throw new ArgumentNullException(nameof(store));

    private readonly IExplorerPluginHostContextFactory _contexts =
        contexts ?? throw new ArgumentNullException(nameof(contexts));

    /// <inheritdoc />
    public Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        var plugins = _catalog.All;
        if (plugins.Count == 0)
        {
            return Task.CompletedTask;
        }

        // Start every probe before awaiting any of them. ProbeOneAsync files a
        // synchronously-resolved decision before it yields, so one gate that
        // never completes cannot keep a sibling's result out of the store.
        var probes = new Task[plugins.Count];
        for (var i = 0; i < plugins.Count; i++)
        {
            probes[i] = ProbeOneAsync(plugins[i], cancellationToken);
        }

        return Task.WhenAll(probes);
    }

    /// <inheritdoc />
    public Task RefreshAsync(string pluginId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(pluginId);

        var plugin = _catalog.Find(pluginId);
        return plugin is null ? Task.CompletedTask : ProbeOneAsync(plugin, cancellationToken);
    }

    private async Task ProbeOneAsync(IExplorerPlugin plugin, CancellationToken cancellationToken)
    {
        ExplorerPluginAccess access;
        string? pluginId = null;

        try
        {
            pluginId = plugin.Descriptor.PluginId;

            var context = _contexts.Create(pluginId);
            var gate = plugin.AccessGate
                ?? throw new InvalidOperationException($"Plugin '{pluginId}' supplied no access gate.");

            access = await gate.ProbeAsync(context, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Fail closed, and contain the fault: a gate that throws denies its
            // own plugin and never reaches the caller or a sibling probe. The
            // message is advisory display text only.
            access = ExplorerPluginAccess.Deny(ex.Message);
        }

        // Reading the descriptor is inside the containment too, so a plugin
        // malformed enough to fault before it even yields an id is contained
        // rather than escaping through the shared refresh. There is then no key
        // to file under, and the plugin simply stays at the fail-closed default.
        if (pluginId is not null)
        {
            _store.Set(pluginId, access);
        }
    }
}
