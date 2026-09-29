using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Shell.Areas.Telemetry;

/// <summary>
/// Says which tenants a telemetry answer covers, from the scope the facade reports
/// it actually applied - never from the scope that was asked for - so a fail-closed
/// narrowing is visible rather than silent.
/// </summary>
internal static class TelemetryScopeCaption
{
    /// <summary>Describes <paramref name="scope"/>.</summary>
    /// <param name="scope">The scope the facade applied.</param>
    /// <returns>The caption, and whether the facade narrowed what was asked for.</returns>
    public static (string Text, bool Narrowed) Describe(TelemetryTenantScope scope)
    {
        if (scope.WasDowngraded)
        {
            var tenant = scope.TenantId is { Length: > 0 } id ? "tenant " + id : "your own tenant";
            return (scope.RequestedVisibility switch
            {
                TelemetryTenantVisibility.AllTenants =>
                    $"You asked for every tenant; the cluster answered for {tenant} only.",
                TelemetryTenantVisibility.SingleTenant =>
                    $"You asked for another tenant; the cluster answered for {tenant} instead.",
                _ => $"The cluster answered for {tenant} only.",
            }, true);
        }

        if (scope.IsCrossTenant)
        {
            return ("Every tenant.", false);
        }

        return (scope.TenantId is { Length: > 0 } active ? $"Tenant {active}." : "Your tenant.", false);
    }
}
