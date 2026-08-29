namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The Schema plugin's own key vocabulary: the stable id its access decisions
/// are filed under.
/// <para>
/// The plugin owns this string; nothing outside the Schema feature needs to
/// know it, which is what lets a decision be keyed without a shared record.
/// </para>
/// </summary>
public static class SchemaPluginKeys
{
    /// <summary>The stable plugin id the Schema area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.schema";
}
