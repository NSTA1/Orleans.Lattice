namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The Access plugin's own key vocabulary: the stable id its access decisions
/// are filed under, plus the scope name for the independent sub-capability the
/// access-model probe reports.
/// <para>
/// The plugin owns these strings; nothing outside the Access feature needs to
/// know them, which is what lets a decision be keyed without a shared record.
/// </para>
/// </summary>
public static class AccessPluginKeys
{
    /// <summary>The stable plugin id the Access area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.access";

    /// <summary>
    /// The scope the cluster's searchable-identity-directory decision is filed
    /// under. A scoped key does not inherit the plugin-level decision, so this
    /// reads denied until the access-model probe reports a directory - which is
    /// the fail-closed answer the create forms need.
    /// </summary>
    public const string DirectoryScope = "directory";
}
