namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The set of selection kinds a plugin on the
/// <see cref="ExplorerPluginSurface.Selection"/> surface applies to.
/// <para>
/// This is what makes per-selection navigation one model rather than a general
/// case plus hard-coded exceptions: a surface that is only meaningful for a
/// tag-index membership tree declares
/// <see cref="ExplorerPluginSelectionKinds.TagIndex"/> and simply resolves to a
/// different plugin set, instead of the host special-casing that selection and
/// bypassing the tier.
/// </para>
/// <para>
/// A flag set rather than a single <see cref="ExplorerPluginSelectionKind"/>, so
/// applicability is a bitwise test on the render path and costs no allocation
/// and no enumeration. Ignored entirely for an
/// <see cref="ExplorerPluginSurface.Area"/> plugin, which is not selection
/// scoped.
/// </para>
/// </summary>
[Flags]
public enum ExplorerPluginSelectionKinds
{
    /// <summary>
    /// No selection kind at all, so the plugin never appears in the per-selection
    /// tier. This is the <c>default</c>, which is why a descriptor defaults its
    /// own property to <see cref="All"/> rather than leaving it unset: an
    /// unstated applicability should mean "wherever a selection exists", not
    /// "nowhere".
    /// </summary>
    None = 0,

    /// <summary>Applies to a registered tree. Pairs with <see cref="ExplorerPluginSelectionKind.Tree"/>.</summary>
    Tree = 1,

    /// <summary>Applies to a materialised view. Pairs with <see cref="ExplorerPluginSelectionKind.View"/>.</summary>
    View = 2,

    /// <summary>Applies to a tag-index membership tree. Pairs with <see cref="ExplorerPluginSelectionKind.TagIndex"/>.</summary>
    TagIndex = 4,

    /// <summary>Applies to every selection kind.</summary>
    All = Tree | View | TagIndex,
}
