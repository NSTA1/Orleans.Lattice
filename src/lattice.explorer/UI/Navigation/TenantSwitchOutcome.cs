namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>What choosing a tenant in the top-bar switcher did.</summary>
internal enum TenantSwitchOutcome
{
    /// <summary>The tenant chosen was already active; nothing moved.</summary>
    Unchanged,

    /// <summary>
    /// The browser went to the current address re-rooted at the tenant; the
    /// layout resolves the switch, and announces a refusal, as it does for any
    /// address.
    /// </summary>
    Navigated,

    /// <summary>At a cluster-wide address the tenant was switched in place, and the switch announced.</summary>
    Switched,

    /// <summary>At a cluster-wide address the switch was refused, and the refusal announced.</summary>
    Refused,
}
