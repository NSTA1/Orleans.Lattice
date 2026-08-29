namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Conversions between the single <see cref="ExplorerPluginSelectionKind"/> a
/// selection actually is and the <see cref="ExplorerPluginSelectionKinds"/> set
/// a plugin declares it applies to.
/// <para>
/// Both operations are branch-and-mask only, so the host resolves applicability
/// on the render path without allocating or enumerating.
/// </para>
/// </summary>
public static class ExplorerPluginSelectionKindsExtensions
{
    /// <summary>
    /// The single-flag set corresponding to <paramref name="kind"/>, or
    /// <see cref="ExplorerPluginSelectionKinds.None"/> for a value outside the
    /// enum. An unrecognised kind resolves to no flag rather than throwing, so a
    /// host reading a selection projected by a newer package degrades to
    /// rendering no plugin instead of faulting the shell.
    /// </summary>
    /// <param name="kind">The selection kind to convert.</param>
    public static ExplorerPluginSelectionKinds ToFlag(this ExplorerPluginSelectionKind kind) => kind switch
    {
        ExplorerPluginSelectionKind.Tree => ExplorerPluginSelectionKinds.Tree,
        ExplorerPluginSelectionKind.View => ExplorerPluginSelectionKinds.View,
        ExplorerPluginSelectionKind.TagIndex => ExplorerPluginSelectionKinds.TagIndex,
        _ => ExplorerPluginSelectionKinds.None,
    };

    /// <summary>
    /// Whether <paramref name="kinds"/> includes <paramref name="kind"/>. An
    /// unrecognised kind is never included, so applicability fails closed.
    /// </summary>
    /// <param name="kinds">The declared applicability set.</param>
    /// <param name="kind">The selection kind to test.</param>
    public static bool Includes(this ExplorerPluginSelectionKinds kinds, ExplorerPluginSelectionKind kind)
    {
        var flag = kind.ToFlag();
        return flag != ExplorerPluginSelectionKinds.None && (kinds & flag) == flag;
    }
}
