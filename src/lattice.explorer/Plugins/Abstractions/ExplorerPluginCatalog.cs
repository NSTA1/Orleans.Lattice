namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The default <see cref="IExplorerPluginCatalog"/>: it validates the resolved
/// plugin set once, sorts it into a stable total order, and pre-computes the
/// per-surface projections.
/// <para>
/// All of the work happens in the constructor, so
/// <see cref="All"/>, <see cref="ForSurface"/> and <see cref="Find"/> are
/// allocation-free reads on the render path.
/// </para>
/// </summary>
public sealed class ExplorerPluginCatalog : IExplorerPluginCatalog
{
    private static readonly IExplorerPlugin[] Empty = [];

    private readonly IExplorerPlugin[] _all;
    private readonly IExplorerPlugin[] _areas;
    private readonly IExplorerPlugin[] _selections;
    private readonly Dictionary<string, IExplorerPlugin> _byId;

    /// <summary>
    /// Builds the catalog over the plugins resolved from the container.
    /// </summary>
    /// <param name="plugins">The registered plugins. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="plugins"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// A plugin exposes no descriptor, view type, or access gate, or two
    /// plugins claim the same id. Registration is a compile-time decision, so
    /// these are programming errors and fail fast rather than degrading.
    /// </exception>
    public ExplorerPluginCatalog(IEnumerable<IExplorerPlugin> plugins)
    {
        ArgumentNullException.ThrowIfNull(plugins);

        var ordered = new List<IExplorerPlugin>();
        _byId = new Dictionary<string, IExplorerPlugin>(StringComparer.Ordinal);

        foreach (var plugin in plugins)
        {
            if (plugin is null)
            {
                throw new InvalidOperationException("A null plugin was registered in the container.");
            }

            var descriptor = plugin.Descriptor
                ?? throw new InvalidOperationException(
                    $"Plugin '{plugin.GetType()}' exposes no descriptor.");

            if (plugin.ViewType is null)
            {
                throw new InvalidOperationException(
                    $"Plugin '{descriptor.PluginId}' exposes no view type.");
            }

            if (plugin.AccessGate is null)
            {
                throw new InvalidOperationException(
                    $"Plugin '{descriptor.PluginId}' exposes no access gate.");
            }

            if (!_byId.TryAdd(descriptor.PluginId, plugin))
            {
                throw new InvalidOperationException(
                    $"Two plugins claim the id '{descriptor.PluginId}': "
                    + $"'{_byId[descriptor.PluginId].GetType()}' and '{plugin.GetType()}'. "
                    + "Plugin ids must be unique.");
            }

            ordered.Add(plugin);
        }

        ordered.Sort(static (left, right) =>
        {
            var byOrder = left.Descriptor.Order.CompareTo(right.Descriptor.Order);
            if (byOrder != 0)
            {
                return byOrder;
            }

            var byLabel = string.CompareOrdinal(left.Descriptor.Label, right.Descriptor.Label);
            return byLabel != 0
                ? byLabel
                : string.CompareOrdinal(left.Descriptor.PluginId, right.Descriptor.PluginId);
        });

        _all = ordered.Count == 0 ? Empty : [.. ordered];
        _areas = Project(_all, ExplorerPluginSurface.Area);
        _selections = Project(_all, ExplorerPluginSurface.Selection);
    }

    /// <inheritdoc />
    public IReadOnlyList<IExplorerPlugin> All => _all;

    /// <inheritdoc />
    public IReadOnlyList<IExplorerPlugin> ForSurface(ExplorerPluginSurface surface) => surface switch
    {
        ExplorerPluginSurface.Area => _areas,
        ExplorerPluginSurface.Selection => _selections,
        _ => Empty,
    };

    /// <inheritdoc />
    public IExplorerPlugin? Find(string pluginId)
    {
        ArgumentNullException.ThrowIfNull(pluginId);
        return _byId.GetValueOrDefault(pluginId);
    }

    private static IExplorerPlugin[] Project(IExplorerPlugin[] all, ExplorerPluginSurface surface)
    {
        var count = 0;
        foreach (var plugin in all)
        {
            if (plugin.Descriptor.Surface == surface)
            {
                count++;
            }
        }

        if (count == 0)
        {
            return Empty;
        }

        var projected = new IExplorerPlugin[count];
        var next = 0;
        foreach (var plugin in all)
        {
            if (plugin.Descriptor.Surface == surface)
            {
                projected[next++] = plugin;
            }
        }

        return projected;
    }
}
