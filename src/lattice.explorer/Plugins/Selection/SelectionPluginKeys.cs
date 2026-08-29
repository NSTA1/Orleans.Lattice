namespace Orleans.Lattice.Explorer.Plugins.Selection;

/// <summary>
/// The stable ids the Explorer's own per-selection plugins are registered and
/// keyed under, and the durable preference key the active one is retained in.
/// <para>
/// These are the same dotted, package-owned ids every other plugin uses, so the
/// per-selection tier holds no privileged position: a head can withhold one by
/// not registering it, and a third party can add its own beside them. The
/// constants live in the shared kernel because more than one place names an id -
/// the plugin's own descriptor, the retained-preference seeding a view performs
/// when it navigates, and the hand-off a surface makes to a sibling surface -
/// and a literal in any of them would drift.
/// </para>
/// </summary>
public static class SelectionPluginKeys
{
    /// <summary>The durable preference key the active per-selection plugin id is retained under.</summary>
    /// <remarks>
    /// Global rather than per selection, which is what makes the panel reopen on
    /// the same surface after the caller moves between trees.
    /// </remarks>
    public const string ActivePluginPreferenceKey = "detail-plugin";

    /// <summary>The stable plugin id of the live-metrics surface. The tier's leftmost surface.</summary>
    public const string Metrics = "orleans.lattice.metrics";

    /// <summary>The stable plugin id of the tree-topology surface.</summary>
    public const string Topology = "orleans.lattice.topology";

    /// <summary>The stable plugin id of the key and value drill-down surface.</summary>
    public const string Data = "orleans.lattice.data";

    /// <summary>The stable plugin id of the strict-mode dead-letter surface.</summary>
    public const string DeadLetter = "orleans.lattice.deadletter";

    /// <summary>
    /// The stable plugin id of the tag-index browsing surface. It declares
    /// <see cref="ExplorerPluginSelectionKinds.TagIndex"/> alone, so a tag-index
    /// selection resolves to it through ordinary applicability rather than
    /// through a special case in the panel.
    /// </summary>
    public const string TagIndex = "orleans.lattice.tagindex";
}
