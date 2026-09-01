using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The world the end-to-end journey head presents: which identities exist, which
/// tenants each may reach, and which trees the catalog holds.
/// <para>
/// <b>Why this exists.</b> The default UI-test head is deliberately disconnected -
/// no silo, no gRPC backend - which is exactly right for a per-surface accessibility
/// sweep but leaves several of this suite's journeys with no reachable precondition:
/// there is no tenant to switch to, no tree to open, and no second area to move
/// between. Rather than weaken those journeys into assertions that cannot fail, the
/// journey head composes the same shipped shell over the <i>injectable seams the
/// product already publishes</i> - <see cref="Core.Catalog.ICatalogReader"/>,
/// <see cref="IExplorerTenantOperatorGate"/>,
/// <see cref="IExplorerAccessibleTenantSource"/> - so the code under test is the real
/// code and only the cluster behind it is a double.
/// </para>
/// <para>
/// <b>What is still real.</b> The shell, the rail, the visibility policy, the tenant
/// switcher, the identity resolver, the preference store, the router and every
/// rendering decision are the shipped implementations. This type supplies facts, not
/// behaviour: it never decides what the UI does with them.
/// </para>
/// <para>
/// Registered as a singleton so a test can move the world between page loads - which
/// is the only honest way to reach the fail-closed restore journey, where a tenant
/// the user legitimately chose stops being reachable before they come back.
/// </para>
/// </summary>
internal sealed class JourneyWorld
{
    /// <summary>The identity that validates as a platform operator and reaches every demo tenant.</summary>
    internal const string PlatformAdmin = "platform-admin";

    /// <summary>An identity that holds no operator rights and reaches exactly one tenant.</summary>
    internal const string DataReader = "data-reader";

    /// <summary>The tenant the demo cluster establishes first, and the safe fallback.</summary>
    internal const string AcmeTenant = "acme";

    /// <summary>The second reachable tenant, the one the tenant-scope journey switches to.</summary>
    internal const string GlobexTenant = "globex";

    private static readonly ExplorerTenantId[] BothTenants =
        [new ExplorerTenantId(AcmeTenant), new ExplorerTenantId(GlobexTenant)];

    private static readonly ExplorerTenantId[] AcmeOnly = [new ExplorerTenantId(AcmeTenant)];

    private volatile bool _globexWithdrawn;

    /// <summary>
    /// <see langword="true"/> once <see cref="WithdrawGlobex"/> has been called, after
    /// which <see cref="AccessibleTenants"/> no longer reports
    /// <see cref="GlobexTenant"/> for anybody. Models the entitlement a user
    /// legitimately held being revoked between two visits.
    /// </summary>
    internal bool IsGlobexWithdrawn => _globexWithdrawn;

    /// <summary>Withdraws <see cref="GlobexTenant"/> from every identity's reachable set.</summary>
    internal void WithdrawGlobex() => _globexWithdrawn = true;

    /// <summary>Restores the starting world, so no journey inherits another's mutation.</summary>
    internal void Reset() => _globexWithdrawn = false;

    /// <summary>
    /// Whether <paramref name="username"/> validates as a platform operator. Only
    /// <see cref="PlatformAdmin"/> does, so the adaptive tenant affordance has a
    /// genuine operator and a genuine non-operator to distinguish between.
    /// </summary>
    /// <param name="username">The signed-in username, or <see langword="null"/> when anonymous.</param>
    internal static bool IsOperator(string? username) =>
        string.Equals(username, PlatformAdmin, StringComparison.Ordinal);

    /// <summary>
    /// The tenants <paramref name="username"/> may read as, best-first. An operator
    /// reaches both demo tenants (so a picker is offered); anybody else reaches one
    /// (so it is not). An anonymous caller reaches none.
    /// </summary>
    /// <param name="username">The signed-in username, or <see langword="null"/> when anonymous.</param>
    internal IReadOnlyList<ExplorerTenantId> AccessibleTenants(string? username)
    {
        if (username is null)
        {
            return Array.Empty<ExplorerTenantId>();
        }

        return IsOperator(username) && !_globexWithdrawn ? BothTenants : AcmeOnly;
    }
}
