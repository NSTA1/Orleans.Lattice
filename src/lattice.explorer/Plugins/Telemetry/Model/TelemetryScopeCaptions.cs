namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Turns the tenant scope the facade pinned into the caption a panel renders.
/// A pure function of the server's own answer: it never consults the head's
/// tenant identity, never substitutes a tenant, and never softens a degrade.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every caption is built from the effective scope.</b> The requested
/// visibility appears only to explain a degrade - "you asked for all tenants and
/// were served one" - and never as the thing the figures are labelled with. A
/// panel that captioned the request would label a fail-closed answer with the
/// question, which is exactly the misinformation the scope type exists to
/// prevent.
/// </para>
/// <para>
/// <b>The tenancy-absent path renders the same panels.</b> With no tenancy
/// add-on there is one tenant and nothing to choose between, so the caption
/// drops the tenant wording entirely rather than saying "active tenant" about a
/// deployment that has no such concept. The panels either side of the caption
/// are byte-for-byte the same (epic decision D3 of the telemetry strand).
/// </para>
/// </remarks>
public static class TelemetryScopeCaptions
{
    private const string Unscoped =
        "This cluster serves one tenant, so these figures cover all of it.";

    private const string ActiveTenantWithoutId =
        "Scoped to your active tenant.";

    private const string CrossTenant =
        "Scoped across every tenant.";

    private const string DegradedWithoutId =
        "You asked for a wider view than the cluster granted. "
        + "These figures cover only the scope named above, not everything you asked for.";

    /// <summary>
    /// The caption for <paramref name="scope"/> on a head with tenant scoping
    /// enabled.
    /// </summary>
    /// <param name="scope">The scope the facade reported, never the one requested.</param>
    /// <returns>The caption and its severity.</returns>
    public static TelemetryScopeCaption For(ExplorerTelemetryScope scope) => For(scope, tenancyEnabled: true);

    /// <summary>
    /// The caption for <paramref name="scope"/>, dropping tenant wording
    /// entirely when the deployment has no tenancy add-on.
    /// </summary>
    /// <param name="scope">The scope the facade reported, never the one requested.</param>
    /// <param name="tenancyEnabled">
    /// Whether the head has tenant scoping at all. On a deployment without it
    /// there is one tenant, so naming one would be noise - but a degrade is
    /// still reported, because a facade that narrowed a request has said
    /// something a caller needs to know however the head is configured.
    /// </param>
    /// <returns>The caption and its severity.</returns>
    public static TelemetryScopeCaption For(ExplorerTelemetryScope scope, bool tenancyEnabled)
    {
        if (scope.WasDowngraded)
        {
            return new TelemetryScopeCaption(TelemetryScopeSeverity.Degraded, DegradeText(scope, tenancyEnabled));
        }

        if (!tenancyEnabled)
        {
            return new TelemetryScopeCaption(TelemetryScopeSeverity.Informational, Unscoped);
        }

        if (scope.IsCrossTenant)
        {
            return new TelemetryScopeCaption(TelemetryScopeSeverity.Informational, CrossTenant);
        }

        return new TelemetryScopeCaption(
            TelemetryScopeSeverity.Informational,
            scope.TenantId is { Length: > 0 } tenant
                ? $"Scoped to tenant '{tenant}'."
                : ActiveTenantWithoutId);
    }

    /// <summary>
    /// The short badge a panel renders beside the chart title: the effective
    /// scope in as few words as it can be said.
    /// </summary>
    /// <param name="scope">The scope the facade reported.</param>
    /// <param name="tenancyEnabled">Whether the head has tenant scoping at all.</param>
    /// <returns>The badge text.</returns>
    public static string BadgeFor(ExplorerTelemetryScope scope, bool tenancyEnabled)
    {
        if (!tenancyEnabled)
        {
            return "all data";
        }

        if (scope.IsCrossTenant)
        {
            return "all tenants";
        }

        return scope.TenantId is { Length: > 0 } tenant ? tenant : "active tenant";
    }

    private static string DegradeText(ExplorerTelemetryScope scope, bool tenancyEnabled)
    {
        if (!tenancyEnabled || scope.TenantId is not { Length: > 0 } tenant)
        {
            return DegradedWithoutId;
        }

        // The two degrades are different refusals and must not be collapsed. A
        // refused AllTenants means the caller is not a validated platform
        // operator; a refused SingleTenant means they are not entitled to the
        // tenant they named. Saying "wider view" about the second would tell an
        // operator their cross-tenant request was too broad when what actually
        // happened is that they were pinned back to their own tenant.
        return scope.RequestedVisibility switch
        {
            ExplorerTelemetryVisibility.AllTenants =>
                $"You asked for every tenant and were served tenant '{tenant}' only. "
                + "These figures are one tenant's, not the cluster's.",
            ExplorerTelemetryVisibility.SingleTenant =>
                $"You asked for another tenant and were served tenant '{tenant}' instead. "
                + "These figures are not the tenant you asked for.",
            _ => DegradedWithoutId,
        };
    }
}
