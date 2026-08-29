using Azure.Core;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure;

/// <summary>
/// Options for the Azure managed-identity backend-token provider that feeds the
/// telemetry proxy's <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/>
/// mode. The provider mints and rotates an Entra (Azure AD) access token from
/// <see cref="Credential"/> for <see cref="Scope"/> and hands it to the proxy for
/// every managed-Prometheus query. Bound by
/// <see cref="LatticeAzureTelemetryTokenServiceCollectionExtensions.AddAzureTelemetryBackendToken"/>
/// and resolvable via <c>IOptions&lt;AzureTelemetryBackendTokenOptions&gt;</c>.
/// </summary>
public sealed class AzureTelemetryBackendTokenOptions
{
    /// <summary>
    /// The default token scope for an Azure Monitor workspace (managed Prometheus)
    /// query endpoint. This is the resource the acquired access token is audienced
    /// for; it is the same scope KEDA and Grafana use to read the workspace.
    /// </summary>
    public const string ManagedPrometheusScope = "https://prometheus.monitor.azure.com/.default";

    /// <summary>
    /// The Azure credential the access token is acquired from. Supply a concrete
    /// <c>Azure.Identity</c> credential such as <c>new DefaultAzureCredential()</c>
    /// or a <c>ManagedIdentityCredential</c> bound to the workload's user-assigned
    /// identity. There is no default; a host opting into the provider must set
    /// this. The core telemetry package never sees this credential - only the
    /// bearer token it produces reaches the backend.
    /// </summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>
    /// The scope the access token is requested for. Defaults to
    /// <see cref="ManagedPrometheusScope"/>. Must be a non-empty, non-whitespace
    /// value.
    /// </summary>
    public string Scope { get; set; } = ManagedPrometheusScope;

    /// <summary>
    /// How long before a cached token's expiry the provider proactively acquires a
    /// fresh one, so an in-flight query never presents an about-to-expire token.
    /// Defaults to 5 minutes. Must be greater than or equal to
    /// <see cref="TimeSpan.Zero"/>.
    /// </summary>
    public TimeSpan RefreshSkew { get; set; } = TimeSpan.FromMinutes(5);
}
