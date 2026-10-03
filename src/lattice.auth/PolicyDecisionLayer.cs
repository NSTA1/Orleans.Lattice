namespace Orleans.Lattice.Auth;

/// <summary>
/// The authorization layer a decision came from. The <b>operator layer</b> is every
/// rule whose id is outside the <see cref="LatticeTenantRuleIds.Prefix"/> namespace
/// (operator rules, the all-trees <c>Tree:*</c> tier and app-compiled <c>app:</c>
/// rules); its matched verdict is final. The <b>tenant layer</b> is the tenant-tier
/// rules a tenant's administrators author over their own trees, consulted only when
/// no operator rule matches. Carried on <see cref="PolicyMatch"/> so explain and
/// effective-permissions surfaces can report which layer decided.
/// </summary>
/// <remarks>In-process value only; carries no Orleans serialization attributes.</remarks>
internal enum PolicyDecisionLayer
{
    /// <summary>No rule matched; the default effect (or a control-plane fail-closed rule) decided.</summary>
    None = 0,

    /// <summary>An operator-layer rule decided (including the <c>Tree:*</c> tier and <c>app:</c> rules).</summary>
    Operator = 1,

    /// <summary>A tenant-tier rule decided (a tree-scoped tenant rule or a tenant-wide rule).</summary>
    Tenant = 2,
}
