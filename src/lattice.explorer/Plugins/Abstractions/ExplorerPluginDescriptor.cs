namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The identity and placement of one plugin: the stable id it is keyed by, the
/// label the navigation strip renders, the ordering hint that positions it
/// among its peers, and the <see cref="ExplorerPluginSurface"/> it occupies.
/// <para>
/// The id is a <see cref="string"/> and never an enum. An enum is a closed set
/// in a shared assembly, so every new plugin would edit it; a string id lets a
/// plugin ship in its own package - and lets a third party add one - with no
/// change to any shared type. Ids are compared with
/// <see cref="StringComparer.Ordinal"/> throughout, so casing is significant.
/// Prefer a dotted, package-owned id such as <c>orleans.lattice.backups</c> so
/// two independently authored plugins do not collide.
/// </para>
/// </summary>
public sealed record ExplorerPluginDescriptor
{
    private readonly string _pluginId = string.Empty;
    private readonly string _label = string.Empty;

    /// <summary>
    /// The stable, opaque plugin id. Must be non-empty and non-whitespace; it
    /// keys the access store, the domain-model seam, and the plugin's
    /// preference namespace, so changing it is a breaking change for anything
    /// persisted under it.
    /// </summary>
    /// <exception cref="ArgumentException">The value is empty or whitespace.</exception>
    /// <exception cref="ArgumentNullException">The value is <see langword="null"/>.</exception>
    public required string PluginId
    {
        get => _pluginId;
        init
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            _pluginId = value;
        }
    }

    /// <summary>
    /// The human-readable label the navigation strip renders for this plugin.
    /// Must be non-empty and non-whitespace.
    /// </summary>
    /// <exception cref="ArgumentException">The value is empty or whitespace.</exception>
    /// <exception cref="ArgumentNullException">The value is <see langword="null"/>.</exception>
    public required string Label
    {
        get => _label;
        init
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            _label = value;
        }
    }

    /// <summary>The navigation tier this plugin occupies.</summary>
    public required ExplorerPluginSurface Surface { get; init; }

    /// <summary>
    /// The ordering hint within <see cref="Surface"/>: lower sorts first.
    /// Ties break on <see cref="Label"/> then <see cref="PluginId"/>, both
    /// ordinal, so the displayed order is a stable total order that does not
    /// depend on DI registration order. Defaults to <c>0</c>.
    /// </summary>
    public int Order { get; init; }

    /// <summary>
    /// The selection kinds this plugin applies to, for a plugin on the
    /// <see cref="ExplorerPluginSurface.Selection"/> surface. Defaults to
    /// <see cref="ExplorerPluginSelectionKinds.All"/>, so an unstated
    /// applicability means "wherever a selection exists".
    /// <para>
    /// This is how a surface that is only meaningful for one kind of selection
    /// - a tag-index browser, say - resolves to a different plugin set instead
    /// of the host special-casing that selection. Ignored for an
    /// <see cref="ExplorerPluginSurface.Area"/> plugin, which is not selection
    /// scoped.
    /// </para>
    /// </summary>
    public ExplorerPluginSelectionKinds SelectionKinds { get; init; } = ExplorerPluginSelectionKinds.All;

    /// <summary>
    /// Whether this plugin applies to a selection of <paramref name="kind"/>,
    /// per <see cref="SelectionKinds"/>. A bitwise test, so the host may call it
    /// on the render path.
    /// </summary>
    /// <param name="kind">The kind of the current selection.</param>
    public bool AppliesTo(ExplorerPluginSelectionKind kind) => SelectionKinds.Includes(kind);
}
