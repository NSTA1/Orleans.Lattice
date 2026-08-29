namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The registered plugin set, resolved once and presented in a stable display
/// order.
/// <para>
/// Ordering is a total order and does not depend on container registration
/// order: plugins sort by
/// <see cref="ExplorerPluginDescriptor.Order"/>, then by
/// <see cref="ExplorerPluginDescriptor.Label"/>, then by
/// <see cref="ExplorerPluginDescriptor.PluginId"/>, all ordinal. Two plugins
/// that share an ordering hint therefore still render in the same sequence on
/// every head and every run.
/// </para>
/// </summary>
public interface IExplorerPluginCatalog
{
    /// <summary>Every registered plugin, in display order.</summary>
    IReadOnlyList<IExplorerPlugin> All { get; }

    /// <summary>
    /// The registered plugins occupying <paramref name="surface"/>, in display
    /// order. Returns an empty list for a surface no plugin occupies.
    /// </summary>
    /// <param name="surface">The navigation tier to filter to.</param>
    IReadOnlyList<IExplorerPlugin> ForSurface(ExplorerPluginSurface surface);

    /// <summary>
    /// The <see cref="ExplorerPluginSurface.Selection"/> plugins that apply to a
    /// selection of <paramref name="kind"/>, in display order. Returns an empty
    /// list when no plugin declares that kind.
    /// <para>
    /// This is the whole of per-selection resolution: a selection kind with its
    /// own dedicated surface simply resolves to a different plugin set, so the
    /// host never special-cases a selection and never bypasses the tier.
    /// Projections are pre-computed, so this is an allocation-free read on the
    /// render path.
    /// </para>
    /// </summary>
    /// <param name="kind">The kind of the current selection.</param>
    IReadOnlyList<IExplorerPlugin> ForSelection(ExplorerPluginSelectionKind kind);

    /// <summary>
    /// The plugin registered under <paramref name="pluginId"/>, or
    /// <see langword="null"/> when none is. Ids compare ordinally.
    /// </summary>
    /// <param name="pluginId">The plugin id to find. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    IExplorerPlugin? Find(string pluginId);
}
