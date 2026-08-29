namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The currently selected catalog entry, as much of it as a plugin is entitled
/// to see: the id it queries by, the label it renders, and the kind it is.
/// <para>
/// This is a projection the host maintains, not the Explorer's own catalog
/// record. The plugin contract deliberately declares its own narrow shape so a
/// plugin cannot reach the catalog reader, the cluster connection, or anything
/// else behind the selection. Anything richer a plugin needs about the
/// selection comes through its declared domain contract, where the dependency
/// is explicit and reviewable.
/// </para>
/// </summary>
public sealed record ExplorerPluginSelection
{
    private readonly string _id = string.Empty;
    private readonly string _label = string.Empty;

    /// <summary>
    /// The opaque tree or view id the plugin queries by. Must be non-empty and
    /// non-whitespace.
    /// </summary>
    /// <exception cref="ArgumentException">The value is empty or whitespace.</exception>
    /// <exception cref="ArgumentNullException">The value is <see langword="null"/>.</exception>
    public required string Id
    {
        get => _id;
        init
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            _id = value;
        }
    }

    /// <summary>
    /// The human-readable label for the selection, which may differ from
    /// <see cref="Id"/> (a view renders its bare name while its id carries the
    /// physical prefix). Must be non-empty and non-whitespace.
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

    /// <summary>What kind of catalog entry the selection is.</summary>
    public required ExplorerPluginSelectionKind Kind { get; init; }
}
