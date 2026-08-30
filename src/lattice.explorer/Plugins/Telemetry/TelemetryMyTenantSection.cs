using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Views;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Fills the seam the My Tenant area declared for its tenant-metrics section:
/// the telemetry panels, pinned to the caller's own tenant.
/// </summary>
/// <remarks>
/// <para>
/// <b>One adapter, no second implementation.</b> The section renders
/// <see cref="TelemetryTenantSection"/>, which renders the same board the
/// Telemetry area does. The tenant pin lives in the workspace that component
/// constructs, not in a parallel view, so there is exactly one place a telemetry
/// panel is composed and one place a scope caption is rendered.
/// </para>
/// <para>
/// <b>The reference points this way deliberately.</b> This package already owns
/// the component, so the adapter belongs beside it; making My Tenant reference
/// this package instead would put a telemetry gRPC binding into a plugin whose
/// whole shape is that it reaches the cluster through the tenancy seam and
/// names no wire contract at all.
/// </para>
/// </remarks>
public sealed class TelemetryMyTenantSection : IMyTenantMetricsSection
{
    /// <inheritdoc />
    public Type ViewType => typeof(TelemetryTenantSection);

    /// <inheritdoc />
    public string Label => "Your tenant's metrics";
}
