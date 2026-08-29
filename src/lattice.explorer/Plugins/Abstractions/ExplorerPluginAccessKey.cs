namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The key one access decision is filed under: a plugin id, plus an optional
/// scope for a decision that is narrower than the plugin as a whole.
/// <para>
/// The scope is what lets the keyed store express everything the Explorer's
/// former fat capability record did without a shared type. A plugin-level
/// decision uses a <see langword="null"/> scope; a per-resource decision - one
/// backup scope per tree, say - uses the resource id, and an independent
/// sub-capability - "the cluster has a searchable identity directory" - uses a
/// plugin-chosen constant. A plugin owns its own scope vocabulary; nothing
/// outside it needs to know the strings.
/// </para>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/> <see langword="struct"/>
/// so store lookups on the render path allocate nothing. Both components
/// compare and hash ordinally, as <see cref="string"/> equality does by
/// default.
/// </para>
/// </summary>
/// <param name="PluginId">The plugin the decision belongs to.</param>
/// <param name="Scope">
/// The narrower resource or sub-capability the decision applies to, or
/// <see langword="null"/> for the plugin-level decision.
/// </param>
public readonly record struct ExplorerPluginAccessKey(string PluginId, string? Scope)
{
    /// <summary>
    /// Creates a plugin-level key (no scope).
    /// </summary>
    /// <param name="pluginId">The plugin id. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="pluginId"/> is <see langword="null"/>.</exception>
    public ExplorerPluginAccessKey(string pluginId)
        : this(Validated(pluginId), Scope: null)
    {
    }

    /// <summary>
    /// A short, stable rendering of the key for diagnostics:
    /// <c>pluginId</c> or <c>pluginId/scope</c>.
    /// </summary>
    public override string ToString() => Scope is null ? PluginId : $"{PluginId}/{Scope}";

    private static string Validated(string pluginId)
    {
        ArgumentNullException.ThrowIfNull(pluginId);
        return pluginId;
    }
}
