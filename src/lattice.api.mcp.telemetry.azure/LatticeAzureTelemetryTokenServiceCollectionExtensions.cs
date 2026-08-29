using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure;

/// <summary>
/// Registers the Azure managed-identity backend-token provider that satisfies the
/// telemetry proxy's <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/>
/// mode, letting the MCP cluster-telemetry tools authenticate to an Azure Monitor
/// managed-Prometheus query endpoint with a rotating Entra access token.
/// </summary>
public static class LatticeAzureTelemetryTokenServiceCollectionExtensions
{
    /// <summary>
    /// Binds and validates <see cref="AzureTelemetryBackendTokenOptions"/> from
    /// <paramref name="configure"/> and registers the
    /// <see cref="ITelemetryBackendTokenProvider"/> the telemetry proxy consults in
    /// <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/> mode. Call this
    /// alongside <c>AddTelemetryTools</c> (which must set
    /// <see cref="LatticeTelemetryOptions.AuthMode"/> to
    /// <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/>). Idempotent: a
    /// second call rebinds the options but registers exactly one provider, and a
    /// host that registered its own <see cref="ITelemetryBackendTokenProvider"/>
    /// first keeps it.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Delegate that populates <see cref="AzureTelemetryBackendTokenOptions"/> - at
    /// minimum the <see cref="AzureTelemetryBackendTokenOptions.Credential"/>.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAzureTelemetryBackendToken(
        this IServiceCollection services,
        Action<AzureTelemetryBackendTokenOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IValidateOptions<AzureTelemetryBackendTokenOptions>,
            AzureTelemetryBackendTokenOptionsValidator>());
        services.TryAddSingleton<ITelemetryBackendTokenProvider, AzureTelemetryBackendTokenProvider>();

        return services;
    }
}
