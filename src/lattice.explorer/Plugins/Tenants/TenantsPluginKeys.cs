namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// The Tenants plugin's own key vocabulary: the stable id its access decision is
/// filed under in the Explorer's keyed plugin access store.
/// <para>
/// The plugin owns this string; nothing outside the Tenants feature needs to
/// know it, which is what lets the decision be keyed without a shared record.
/// </para>
/// </summary>
public static class TenantsPluginKeys
{
    /// <summary>The stable plugin id the Tenants area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.tenants";
}
