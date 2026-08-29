namespace Orleans.Lattice.Explorer.MyTenant;

/// <summary>
/// The My Tenant plugin's own key vocabulary: the stable id its access decision
/// is filed under, and the scopes the plugin publishes its own advisory
/// sub-decisions against.
/// <para>
/// The plugin owns these strings; nothing outside the My Tenant feature needs to
/// know them, which is what lets a decision be keyed without a shared record.
/// </para>
/// </summary>
public static class MyTenantPluginKeys
{
    /// <summary>The stable plugin id the My Tenant area is registered and keyed under.</summary>
    public const string PluginId = "orleans.lattice.mytenant";

    /// <summary>
    /// The scope the plugin files its platform-operator-gate diagnostic under.
    /// <para>
    /// It is not an authorization decision: it records whether the head supplied
    /// a real <c>IExplorerTenantOperatorGate</c> or is still running on the
    /// navigation core's fail-closed placeholder, so a head that called
    /// <c>AddExplorerTenantView()</c> before <c>AddExplorerAccess()</c> is told
    /// so instead of silently losing every tenant switch.
    /// </para>
    /// </summary>
    public const string OperatorGateScope = "operator-gate";
}
